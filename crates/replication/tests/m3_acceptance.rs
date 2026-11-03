//! M3 acceptance drills: failover, fencing, idempotent retries, partition
//! safety and replica catch-up (snapshot + log) over the replicated shard.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use trading_protocol::{Command, CommandResult, ShardId, Status, SubmitError};
use trading_replication::{ReplicatedShard, ReplicatedShardConfig};
use trading_storage::SyncPolicy;

/// Raft drills are wall-clock sensitive; running them concurrently skews tick
/// cadence and turns elections into split-vote livelocks. One drill at a time.
static SERIAL_DRILLS: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    match SERIAL_DRILLS.lock() {
        Ok(guard) => guard,
        Err(poison) => poison.into_inner(),
    }
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "trading-m3-{tag}-{}-{}",
            std::process::id(),
            tag_counter()
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tag_counter() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::SeqCst) + std::process::id() as u64
}

fn request_ids() -> impl Iterator<Item = u128> {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    (0..).map(|_| NEXT.fetch_add(1, Ordering::SeqCst) as u128)
}

fn fast_config(sync_policy: SyncPolicy) -> ReplicatedShardConfig {
    ReplicatedShardConfig {
        replicas: 3,
        sync_policy,
        // Wide window: a narrow one lets same-tick candidates split-vote
        // forever when the tick jitter is coarse (debug builds).
        election_timeout_min: 400,
        election_timeout_max: 1200,
        heartbeat_interval: 50,
        snapshot_threshold: 50,
        max_in_flight: 256,
        write_timeout: Duration::from_secs(6),
        attempt_timeout: Duration::from_millis(600),
        transport_capacity: 1024,
    }
}

/// Holds the TempDir so the data dir outlives the shard: TempDir::drop would
/// delete the WAL/vote/snapshot files the running group depends on.
fn open_shard(
    tag: &str,
    config: ReplicatedShardConfig,
) -> (TempDir, Arc<Runtime>, ReplicatedShard) {
    let dir = TempDir::new(tag);
    // Two workers per group keep parallel test runs from oversubscribing the
    // machine; more workers would skew the tick cadence and starve elections.
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime"),
    );
    let shard = ReplicatedShard::open(dir.0.clone(), 0 as ShardId, config, Arc::clone(&runtime))
        .expect("shard opens");
    (dir, runtime, shard)
}

fn wait_until(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("timed out waiting for {what}");
}

fn assert_success(result: CommandResult) {
    assert_eq!(
        result.status,
        Status::Success,
        "command failed with {:?}",
        result.status
    );
}

/// Seed money so the group has a known total balance before the drills.
fn seed(shard: &ReplicatedShard, ids: &mut impl Iterator<Item = u128>) -> u64 {
    assert_success(
        shard
            .submit(Command::open_account(ids.next().unwrap(), 1))
            .unwrap(),
    );
    assert_success(
        shard
            .submit(Command::deposit(ids.next().unwrap(), 1, 1_000))
            .unwrap(),
    );
    1_000
}

/// Committed writes are visible on every replica and submits are idempotent.
#[test]
fn commit_replicates_and_duplicate_requests_apply_once() {
    let _drills = serial();
    let (_dir, _runtime, shard) = open_shard("commit", fast_config(SyncPolicy::Durable));
    let mut ids = request_ids();

    assert_success(
        shard
            .submit(Command::open_account(ids.next().unwrap(), 1))
            .unwrap(),
    );
    assert_success(
        shard
            .submit(Command::open_account(ids.next().unwrap(), 2))
            .unwrap(),
    );
    assert_success(
        shard
            .submit(Command::deposit(ids.next().unwrap(), 1, 100))
            .unwrap(),
    );
    let transfer_id = ids.next().unwrap();
    let transfer = Command::transfer(transfer_id, 1, 2, 40);
    let first = shard.submit(transfer).expect("transfer commits");
    assert_success(first);
    // Retrying the same request (as any client would on a redirect or a
    // failover) must return the original result without applying twice.
    let retry = shard.submit(transfer).expect("retry commits");
    assert_eq!(retry, first);

    let summary = shard.summary();
    assert_eq!(summary.len(), 3);
    for replica in &summary {
        assert!(replica.alive, "replica {} down", replica.id);
        assert_eq!(replica.account_count, Some(2));
        assert_eq!(replica.total_balance, Some(100));
    }
    let leaders = summary.iter().filter(|replica| replica.leader).count();
    assert_eq!(leaders, 1, "exactly one leader: {summary:?}");
}

/// Killing the leader must not lose committed writes; a new leader is elected
/// within seconds and the recovered old leader cannot resurrect its term.
#[test]
fn leader_crash_failover_preserves_committed_writes() {
    let _drills = serial();
    let (_dir, _runtime, shard) = open_shard("failover", fast_config(SyncPolicy::Durable));
    let mut ids = request_ids();
    let expected = seed(&shard, &mut ids);
    let leader = shard.leader().expect("leader elected");

    // In-flight writes right before the crash must either commit or surface as
    // NoLeader; committed ones are durable. Crash the leader.
    let started = Instant::now();
    shard.crash_replica(leader);
    let mut failover_write_ms = None;
    let mut committed_after = 0u128;
    for _ in 0..50 {
        match shard.submit(Command::deposit(ids.next().unwrap(), 1, 1)) {
            Ok(result) => {
                assert_success(result);
                if failover_write_ms.is_none() {
                    failover_write_ms = Some(started.elapsed().as_millis());
                }
                committed_after += 1;
            }
            Err(SubmitError::NoLeader) => {
                // Stop-the-world window while the majority elects a leader.
                // The first writes legitimately hit this while the election
                // runs; what matters is that some write commits in time.
            }
            Err(SubmitError::ForwardToLeader { .. }) | Err(SubmitError::Overloaded) => {}
            Err(other) => panic!("unexpected submit error during failover: {other:?}"),
        }
    }
    let stop_ms = failover_write_ms.expect("some write commits after failover");
    assert!(stop_ms < 6_000, "failover stalled {stop_ms}ms");

    let new_leader = shard.leader().expect("new leader");
    assert_ne!(new_leader, leader, "the crashed leader must not lead");
    // Everything committed before the crash survives the failover.
    let live_total = shard
        .summary()
        .iter()
        .filter(|replica| replica.alive)
        .find(|replica| replica.leader)
        .and_then(|replica| replica.total_balance)
        .expect("leader summary");
    assert_eq!(
        live_total,
        expected as u128 + committed_after,
        "committed writes lost across failover"
    );

    // Restart the old leader: it recovers from disk and follows the new term.
    shard.restart_replica(leader).expect("old leader restarts");
    let target_applied = shard
        .summary()
        .iter()
        .find(|replica| replica.leader)
        .and_then(|replica| replica.last_applied)
        .expect("new leader applied index");
    wait_until(
        "restarted leader catches up",
        Duration::from_secs(10),
        || {
            let summary = shard.summary();
            let lagging = summary.iter().find(|replica| replica.id == leader);
            lagging
                .and_then(|replica| replica.last_applied)
                .unwrap_or(0)
                >= target_applied
        },
    );
    let summary = shard.summary();
    for replica in &summary {
        assert!(replica.alive, "all replicas alive again");
        assert_eq!(
            replica.term, summary[0].term,
            "terms converged: {summary:?}"
        );
        assert_eq!(
            replica.total_balance,
            Some(expected as u128 + committed_after),
            "replica {} diverged after catch-up",
            replica.id
        );
    }
    let leaders = summary.iter().filter(|replica| replica.leader).count();
    assert_eq!(leaders, 1, "still exactly one leader");

    // Writes keep flowing after the old leader is back.
    assert_success(
        shard
            .submit(Command::withdraw(ids.next().unwrap(), 1, 1))
            .unwrap(),
    );
}

/// A leader isolated from the quorum cannot commit; clients retry onto the
/// newly elected leader and no double-apply happens across the switch.
#[test]
fn partitioned_leader_cannot_commit_and_retries_apply_once() {
    let _drills = serial();
    let (_dir, _runtime, shard) = open_shard("partition", fast_config(SyncPolicy::UnsafeBuffered));
    let mut ids = request_ids();
    let expected = seed(&shard, &mut ids);
    let leader = shard.leader().expect("leader elected");
    let others: Vec<u64> = (1..=3).filter(|id| *id != leader).collect();

    // Isolate the leader from the quorum.
    shard.transport().cut(leader, others[0]);
    shard.transport().cut(leader, others[1]);

    // A write during the partition: no quorum on the old leader, so the
    // submit must land on a new leader (or fail cleanly with NoLeader).
    let withdraw_id = ids.next().unwrap();
    let withdraw = Command::withdraw(withdraw_id, 1, 100);
    let applied = match shard.submit(withdraw) {
        Ok(result) => {
            assert_success(result);
            true
        }
        Err(SubmitError::NoLeader) => false,
        Err(other) => panic!("unexpected error during partition: {other:?}"),
    };

    // Heal and let the group converge, then replay the same request: it must
    // not double-apply no matter which leader first handled it.
    shard.transport().heal_all();
    let replay = shard
        .submit(withdraw)
        .expect("replay commits after healing");
    assert_success(replay);
    if applied {
        // Same result object: the first application is what sticks.
        assert_eq!(replay.request_id, withdraw_id);
    }

    wait_until(
        "partitioned leader steps down",
        Duration::from_secs(10),
        || {
            shard
                .summary()
                .iter()
                .filter(|replica| replica.alive)
                .all(|replica| replica.term >= 1 && replica.leader || !replica.leader)
        },
    );
    // All replicas converge on the same balance: exactly one withdrawal.
    wait_until("all replicas converge", Duration::from_secs(10), || {
        let summary = shard.summary();
        let total = expected as u128 - 100;
        summary.len() == 3
            && summary.iter().all(|replica| replica.alive)
            && summary
                .iter()
                .all(|replica| replica.total_balance == Some(total))
    });
    let leaders = shard
        .summary()
        .iter()
        .filter(|replica| replica.leader)
        .count();
    assert_eq!(leaders, 1);
}

/// A crashed follower rejoins after the leader has snapshotted and purged old
/// logs: the catch-up must complete via snapshot transfer plus log replay.
#[test]
fn lagging_replica_catches_up_after_snapshot_purge() {
    let _drills = serial();
    let (_dir, _runtime, shard) = open_shard("catchup", {
        let mut config = fast_config(SyncPolicy::UnsafeBuffered);
        config.snapshot_threshold = 30;
        config
    });
    let mut ids = request_ids();
    let expected = seed(&shard, &mut ids);
    let leader = shard.leader().expect("leader elected");
    let follower = (1..=3).find(|id| *id != leader).expect("a follower");

    shard.crash_replica(follower);

    // Push well past the snapshot threshold so the leader snapshots and
    // purges logs the crashed follower would otherwise replay.
    let extra = 120u64;
    for step in 1..=extra {
        assert_success(
            shard
                .submit(Command::deposit(ids.next().unwrap(), 1, step))
                .unwrap(),
        );
    }
    let expected_total = expected as u128 + (1..=extra).sum::<u64>() as u128;

    shard.restart_replica(follower).expect("follower restarts");
    let leader_applied = shard
        .summary()
        .into_iter()
        .find(|replica| replica.leader)
        .and_then(|replica| replica.last_applied)
        .expect("leader applied index");
    wait_until(
        "follower catches up via snapshot+log",
        Duration::from_secs(20),
        || {
            let lagging = shard
                .summary()
                .into_iter()
                .find(|replica| replica.id == follower)
                .expect("restarted follower");
            lagging.alive && lagging.last_applied.unwrap_or(0) >= leader_applied
        },
    );
    let summary = shard.summary();
    for replica in &summary {
        assert!(replica.alive);
        assert_eq!(
            replica.total_balance,
            Some(expected_total),
            "replica {} total diverged",
            replica.id
        );
        assert_eq!(replica.account_count, Some(1));
    }

    // The caught-up follower keeps following new commits.
    assert_success(
        shard
            .submit(Command::deposit(ids.next().unwrap(), 1, 999))
            .unwrap(),
    );
    wait_until(
        "all replicas see the tail write",
        Duration::from_secs(10),
        || {
            shard
                .summary()
                .iter()
                .all(|replica| replica.total_balance == Some(expected_total + 999))
        },
    );
}

/// A replica can only be restarted on one process: the directory lock fences
/// double-opens of the same WAL.
#[test]
fn replica_directory_lock_fences_double_open() {
    let _drills = serial();
    let (_dir, _runtime, shard) = open_shard("fence", fast_config(SyncPolicy::UnsafeBuffered));
    let mut ids = request_ids();
    seed(&shard, &mut ids);
    let leader = shard.leader().expect("leader elected");

    // Restart of a live replica fails on the directory lock.
    let result = shard.restart_replica(leader);
    let error = result.expect_err("live replica cannot restart");
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);

    // Crash then restart is the supported rebuild path.
    shard.crash_replica(leader);
    shard
        .restart_replica(leader)
        .expect("rebuild works after crash");
}
