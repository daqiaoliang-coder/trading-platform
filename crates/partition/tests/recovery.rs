use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use trading_partition::{Engine, EngineConfig, Partition, PartitionConfig};
use trading_protocol::{Command, Status};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn temp_dir() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "trading-partition-test-{}-{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn a_data_directory_admits_only_one_partition_writer() {
    let config = PartitionConfig::new(temp_dir());
    let first = Partition::open(config.clone()).unwrap();
    let second = Partition::open(config.clone()).err().unwrap();
    assert_eq!(second.kind(), std::io::ErrorKind::WouldBlock);
    assert!(second.to_string().contains("already locked"));
    drop(first);
    assert!(Partition::open(config).is_ok());
}

#[test]
fn wal_replay_restores_balances_and_idempotency() {
    let config = PartitionConfig::new(temp_dir());
    let mut partition = Partition::open(config.clone()).unwrap();
    partition
        .execute_batch(&[
            Command::open_account(1, 10),
            Command::open_account(2, 20),
            Command::deposit(3, 10, 100),
            Command::transfer(4, 10, 20, 30),
        ])
        .unwrap();
    drop(partition);

    let mut restored = Partition::open(config).unwrap();
    assert_eq!(restored.last_sequence(), 4);
    assert_eq!(restored.balance(10), Some(70));
    assert_eq!(restored.balance(20), Some(30));
    let duplicate = restored.execute(Command::transfer(4, 10, 20, 30)).unwrap();
    assert_eq!(duplicate.sequence, 4);
    assert_eq!(restored.balance(10), Some(70));
    assert_eq!(restored.balance(20), Some(30));
}

#[test]
fn durable_checkpoint_replaces_covered_wal_and_recovers() {
    let config = PartitionConfig::new(temp_dir());
    let mut partition = Partition::open(config.clone()).unwrap();
    partition
        .execute_batch(&[
            Command::open_account(1, 10),
            Command::open_account(2, 20),
            Command::deposit(3, 10, 100),
            Command::transfer(4, 10, 20, 25),
        ])
        .unwrap();
    partition.checkpoint().unwrap();
    assert_eq!(
        std::fs::metadata(config.data_dir.join("partition.wal"))
            .unwrap()
            .len(),
        0
    );
    drop(partition);

    let mut restored = Partition::open(config.clone()).unwrap();
    assert_eq!(restored.last_sequence(), 4);
    assert_eq!(restored.balance(10), Some(75));
    assert_eq!(restored.balance(20), Some(25));
    assert_eq!(
        restored
            .execute(Command::deposit(3, 10, 100))
            .unwrap()
            .sequence,
        3
    );
    assert_eq!(restored.balance(10), Some(75));
    assert_eq!(
        restored
            .execute(Command::withdraw(5, 20, 5))
            .unwrap()
            .status,
        Status::Success
    );
    assert_eq!(restored.last_sequence(), 6);
    drop(restored);

    let restored = Partition::open(config).unwrap();
    assert_eq!(restored.last_sequence(), 6);
    assert_eq!(restored.balance(10), Some(75));
    assert_eq!(restored.balance(20), Some(20));
}

#[test]
fn torn_wal_tail_is_discarded_without_losing_prior_commit() {
    let config = PartitionConfig::new(temp_dir());
    let mut partition = Partition::open(config.clone()).unwrap();
    partition
        .execute_batch(&[Command::open_account(1, 10), Command::deposit(2, 10, 50)])
        .unwrap();
    drop(partition);
    let mut wal = OpenOptions::new()
        .append(true)
        .open(config.data_dir.join("partition.wal"))
        .unwrap();
    wal.write_all(b"TPW1\x01partial").unwrap();
    drop(wal);

    let restored = Partition::open(config).unwrap();
    assert_eq!(restored.last_sequence(), 2);
    assert_eq!(restored.balance(10), Some(50));
}

#[test]
fn executor_applies_and_responds_only_after_the_durable_batch() {
    let config = PartitionConfig::new(temp_dir());
    let partition = Partition::open(config.clone()).unwrap();
    let handle = Engine::new(partition, EngineConfig::default()).spawn();
    assert!(handle
        .submit(Command::open_account(1, 10))
        .unwrap()
        .is_success());
    assert!(handle
        .submit(Command::deposit(2, 10, 100))
        .unwrap()
        .is_success());
    assert!(handle
        .submit(Command::withdraw(3, 10, 40))
        .unwrap()
        .is_success());
    let partition = handle.shutdown().unwrap();
    assert_eq!(partition.balance(10), Some(60));
    drop(partition);

    let restored = Partition::open(config).unwrap();
    assert_eq!(restored.balance(10), Some(60));
    assert_eq!(restored.last_sequence(), 3);
}
