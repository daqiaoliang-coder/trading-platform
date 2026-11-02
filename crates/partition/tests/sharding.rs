use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use trading_partition::{ClusterConfig, RouteTable, ShardedCluster, SyncPolicy};
use trading_protocol::{Command, Route, RouteError, SubmitError};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn temp_dir() -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "trading-sharding-test-{}-{}",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn shards_have_independent_sequences_storage_and_recovery() {
    let config = ClusterConfig::new(temp_dir());
    let routes = RouteTable::uniform(1, 3).unwrap();
    let cluster = ShardedCluster::open(config.clone(), routes.clone()).unwrap();
    let mut transfers = Vec::new();

    for shard in 0..3_u64 {
        let primary = shard + 3;
        let secondary = shard + 6;
        let request = shard as u128 * 10;
        assert_eq!(cluster.route_account(primary).shard_id, shard as u16);
        assert_eq!(cluster.route_account(secondary).shard_id, shard as u16);
        cluster
            .submit(Command::open_account(request + 1, primary))
            .unwrap();
        cluster
            .submit(Command::open_account(request + 2, secondary))
            .unwrap();
        cluster
            .submit(Command::deposit(request + 3, primary, 100))
            .unwrap();
        let transfer = Command::transfer(request + 4, primary, secondary, 25);
        let result = cluster.submit(transfer).unwrap();
        assert_eq!(result.sequence, 4);
        transfers.push((shard as u16, transfer, result, primary, secondary));
    }

    let extra = cluster.submit(Command::withdraw(100, 4, 5)).unwrap();
    assert_eq!(extra.sequence, 5);
    assert!(matches!(
        cluster.submit(Command::transfer(101, 3, 4, 1)),
        Err(SubmitError::Route(RouteError::CrossShard { .. }))
    ));

    let metrics = cluster.metrics();
    assert_eq!(metrics[&0].executed, 4);
    assert_eq!(metrics[&1].executed, 5);
    assert_eq!(metrics[&2].executed, 4);
    assert_eq!(metrics[&1].last_sequence, 5);
    assert!(metrics.values().all(|metric| metric.queue_capacity > 0));
    assert!(metrics.values().all(|metric| metric.batches > 0));
    assert!(metrics
        .values()
        .all(|metric| metric.max_batch_latency_ns > 0));

    let mut partitions = cluster.shutdown().unwrap();
    for (shard, _, _, primary, secondary) in &transfers {
        let partition = &partitions[shard];
        let expected_primary = if *shard == 1 { 70 } else { 75 };
        assert_eq!(partition.balance(*primary), Some(expected_primary));
        assert_eq!(partition.balance(*secondary), Some(25));
        assert_eq!(partition.account_count(), 2);
        for other in 0..3_u16 {
            if other != *shard {
                assert_eq!(partitions[&other].balance(*primary), None);
            }
        }
    }
    assert_eq!(partitions[&1].balance(4), Some(70));

    partitions.get_mut(&0).unwrap().checkpoint().unwrap();
    assert_eq!(
        std::fs::metadata(config.data_dir.join("shard-0/partition.wal"))
            .unwrap()
            .len(),
        0
    );
    assert!(
        std::fs::metadata(config.data_dir.join("shard-1/partition.wal"))
            .unwrap()
            .len()
            > 0
    );
    drop(partitions);

    let cluster = ShardedCluster::open(config, routes).unwrap();
    for (shard, transfer, original, _, _) in &transfers {
        let duplicate = cluster.submit(*transfer).unwrap();
        assert_eq!(&duplicate, original);
        let metrics = cluster.metrics();
        assert_eq!(
            metrics[shard].last_sequence,
            if *shard == 1 { 6 } else { 5 }
        );
        let route = cluster.route(transfer).unwrap();
        assert_eq!(route.shard_id, *shard);
        assert_eq!(duplicate.sequence, 4);
    }
    let partitions = cluster.shutdown().unwrap();
    for (shard, _, _, primary, secondary) in transfers {
        let expected_primary = if shard == 1 { 70 } else { 75 };
        assert_eq!(partitions[&shard].balance(primary), Some(expected_primary));
        assert_eq!(partitions[&shard].balance(secondary), Some(25));
    }
}

#[test]
fn committed_events_preserve_per_shard_sequence_and_route_version() {
    let config = ClusterConfig {
        sync_policy: SyncPolicy::UnsafeBuffered,
        ..ClusterConfig::new(temp_dir())
    };
    let (sender, receiver) = mpsc::channel();
    let cluster =
        ShardedCluster::open_with_event_sender(config, RouteTable::uniform(1, 2).unwrap(), sender)
            .unwrap();

    for shard in 0..2_u64 {
        let account = shard + 2;
        cluster
            .submit(Command::open_account(1_000 + shard as u128, account))
            .unwrap();
        cluster
            .submit(Command::deposit(2_000 + shard as u128, account, 100))
            .unwrap();
        let duplicate = cluster
            .submit(Command::deposit(2_000 + shard as u128, account, 100))
            .unwrap();
        assert_eq!(duplicate.sequence, 2);
    }
    assert!(cluster.submit(Command::transfer(3_000, 2, 3, 1)).is_err());
    cluster
        .update_route_table(RouteTable::uniform(2, 2).unwrap())
        .unwrap();
    for shard in 0..2_u64 {
        cluster
            .submit(Command::withdraw(4_000 + shard as u128, shard + 2, 1))
            .unwrap();
    }
    drop(cluster.shutdown().unwrap());

    let events: Vec<_> = receiver.into_iter().collect();
    assert_eq!(events.len(), 8);
    for shard in 0..2_u16 {
        let shard_events: Vec<_> = events
            .iter()
            .filter(|event| event.shard_id == shard)
            .collect();
        assert_eq!(
            shard_events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            shard_events
                .iter()
                .map(|event| event.route_version)
                .collect::<Vec<_>>(),
            vec![1, 1, 1, 2]
        );
        assert_eq!(shard_events[2].sequence, 3);
        assert_eq!(shard_events[2].result.sequence, 2);
    }
}

#[test]
fn stale_route_redirect_preserves_request_idempotency() {
    let config = ClusterConfig {
        sync_policy: SyncPolicy::UnsafeBuffered,
        ..ClusterConfig::new(temp_dir())
    };
    let cluster = ShardedCluster::open(config, RouteTable::uniform(1, 2).unwrap()).unwrap();
    let old_route = cluster.route_account(2);
    let open = Command::open_account(42, 2);
    let deposit = Command::deposit(43, 2, 100);
    assert_eq!(
        cluster.submit_with_route(old_route, open).unwrap().sequence,
        1
    );
    let committed = cluster.submit_with_route(old_route, deposit).unwrap();
    assert_eq!(committed.sequence, 2);

    cluster
        .update_route_table(RouteTable::uniform(2, 2).unwrap())
        .unwrap();
    let current = match cluster.submit_with_route(old_route, deposit) {
        Err(SubmitError::Route(RouteError::StaleRoute { current })) => current,
        other => panic!("expected stale route redirect, got {other:?}"),
    };
    assert_eq!(current.version, 2);
    assert_eq!(current.shard_id, old_route.shard_id);
    assert_eq!(
        cluster.submit_with_route(current, deposit).unwrap(),
        committed
    );
    assert!(matches!(
        cluster.submit_with_route(
            Route {
                version: 2,
                shard_id: 1,
            },
            open,
        ),
        Err(SubmitError::Route(RouteError::StaleRoute { .. }))
    ));
    assert_eq!(
        cluster.submit_with_route(current, open).unwrap().sequence,
        1
    );
    assert_eq!(
        cluster.update_route_table(RouteTable::uniform(2, 2).unwrap()),
        Err(RouteError::StaleVersion { current_version: 2 })
    );
    assert_eq!(
        cluster.update_route_table(RouteTable::new(3, vec![0, 9]).unwrap()),
        Err(RouteError::UnknownShard(9))
    );
    assert_eq!(
        cluster.update_route_table(RouteTable::new(3, vec![1, 0]).unwrap()),
        Err(RouteError::MappingChanged)
    );
    assert_eq!(cluster.route_table().version(), 2);

    let partitions = cluster.shutdown().unwrap();
    assert_eq!(partitions[&0].last_sequence(), 4);
    assert_eq!(partitions[&0].balance(2), Some(100));
    assert_eq!(partitions[&0].result_count(), 2);
}

#[test]
fn concurrent_shards_assign_sequences_without_a_global_counter() {
    let config = ClusterConfig {
        sync_policy: SyncPolicy::UnsafeBuffered,
        ..ClusterConfig::new(temp_dir())
    };
    let cluster =
        Arc::new(ShardedCluster::open(config, RouteTable::uniform(1, 3).unwrap()).unwrap());
    for shard in 0..3_u64 {
        let account = shard + 3;
        cluster
            .submit(Command::open_account(shard as u128 + 1, account))
            .unwrap();
        cluster
            .submit(Command::deposit(shard as u128 + 4, account, 1))
            .unwrap();
    }

    let mut workers = Vec::new();
    for shard in 0..3_u64 {
        let cluster = Arc::clone(&cluster);
        workers.push(thread::spawn(move || {
            let account = shard + 3;
            let mut sequences = Vec::new();
            for index in 0..32_u64 {
                let request_id = 100 + shard as u128 * 100 + index as u128;
                let result = cluster
                    .submit(Command::deposit(request_id, account, 1))
                    .unwrap();
                sequences.push(result.sequence);
            }
            sequences
        }));
    }
    for worker in workers {
        let sequences = worker.join().unwrap();
        assert_eq!(sequences, (3..=34).collect::<Vec<_>>());
    }
    let metrics = cluster.metrics();
    for shard in 0..3_u16 {
        assert_eq!(metrics[&shard].last_sequence, 34);
        assert_eq!(metrics[&shard].executed, 34);
    }

    let cluster = match Arc::try_unwrap(cluster) {
        Ok(cluster) => cluster,
        Err(_) => panic!("cluster still has outstanding owners"),
    };
    let partitions = cluster.shutdown().unwrap();
    for shard in 0..3_u16 {
        assert_eq!(partitions[&shard].last_sequence(), 34);
        assert_eq!(partitions[&shard].balance(shard as u64 + 3), Some(33));
    }
}
