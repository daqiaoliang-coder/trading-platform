//! M4 acceptance drills: online shard migration, single-shard pause, route
//! epoch convergence, old-primary fencing, scale out/in, rollback and restart
//! recovery.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};
use trading_partition::RouteTable;
use trading_protocol::{Command, Route, RouteError, ShardId, Status, SubmitError};
use trading_replication::{
    MigrationFault, MigrationStatus, ReplicatedCluster, ReplicatedClusterConfig,
    ReplicatedShardConfig, ShardMigrationPlan,
};
use trading_storage::SyncPolicy;

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
            "trading-m4-{tag}-{}-{}",
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

fn request_id() -> u128 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::SeqCst) as u128
}

fn shard_config() -> ReplicatedShardConfig {
    ReplicatedShardConfig {
        replicas: 3,
        sync_policy: SyncPolicy::UnsafeBuffered,
        election_timeout_min: 400,
        election_timeout_max: 1200,
        heartbeat_interval: 50,
        // Force snapshots during the migration catch-up so the new learner
        // exercises snapshot transfer plus log replay, not just log tailing.
        snapshot_threshold: 30,
        max_in_flight: 512,
        write_timeout: Duration::from_secs(8),
        attempt_timeout: Duration::from_millis(600),
        transport_capacity: 1024,
    }
}

fn open_cluster(dir: &Path, shard_count: u16) -> ReplicatedCluster {
    let config = ReplicatedClusterConfig {
        data_dir: dir.to_path_buf(),
        shard: shard_config(),
    };
    ReplicatedCluster::open(
        config,
        RouteTable::uniform(1, shard_count).expect("route table"),
    )
    .expect("cluster opens")
}

fn assert_success(result: trading_protocol::CommandResult) {
    assert_eq!(result.status, Status::Success, "status {:?}", result.status);
}

fn submit_ok(cluster: &ReplicatedCluster, command: Command) -> trading_protocol::CommandResult {
    let result = cluster.submit(command).expect("submit succeeds");
    assert_success(result);
    result
}

fn wait_until(what: &str, timeout: Duration, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if check() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for {what}");
}

fn accounts_for(shard: ShardId, shards: u16, accounts: u64) -> Vec<u64> {
    (1..=accounts)
        .filter(|account| (account % shards as u64) == shard as u64)
        .collect()
}

fn seed_accounts(cluster: &ReplicatedCluster, accounts: u64, deposit: u64) {
    for account in 1..=accounts {
        submit_ok(cluster, Command::open_account(request_id(), account));
        submit_ok(cluster, Command::deposit(request_id(), account, deposit));
    }
}

fn expected_totals(
    shards: u16,
    accounts: u64,
    deposit: u64,
    extra: &BTreeMap<ShardId, u128>,
) -> BTreeMap<ShardId, u128> {
    (0..shards)
        .map(|shard| {
            let per_shard = accounts_for(shard, shards, accounts).len() as u128;
            let extra = extra.get(&shard).copied().unwrap_or(0);
            (shard, per_shard * deposit as u128 + extra)
        })
        .collect()
}

fn assert_replicas_match(
    cluster: &ReplicatedCluster,
    shards: u16,
    accounts: u64,
    totals: &BTreeMap<ShardId, u128>,
) -> Vec<u64> {
    let mut live = Vec::new();
    for (shard, replicas) in cluster.summary() {
        assert!(!replicas.is_empty(), "shard {shard} has no replicas");
        for replica in &replicas {
            assert!(
                replica.alive,
                "shard {shard} replica {} is down",
                replica.id
            );
            assert_eq!(
                replica.account_count,
                Some(accounts_for(shard, shards, accounts).len()),
                "shard {shard} replica {} account count",
                replica.id
            );
            assert_eq!(
                replica.total_balance,
                Some(totals[&shard]),
                "shard {shard} replica {} balance",
                replica.id
            );
            live.push(replica.id);
        }
        assert_eq!(
            replicas.iter().filter(|replica| replica.leader).count(),
            1,
            "shard {shard} must have exactly one leader: {replicas:?}"
        );
    }
    live.sort_unstable();
    live
}

#[derive(Debug)]
struct WorkerStats {
    shard: ShardId,
    latencies_us: Vec<u64>,
    committed: u64,
    errors: u64,
    last_error: Option<String>,
}

fn spawn_worker(
    cluster: Arc<ReplicatedCluster>,
    shard: ShardId,
    shards: u16,
    accounts: u64,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<WorkerStats> {
    thread::spawn(move || {
        let account_list = accounts_for(shard, shards, accounts);
        let mut latencies = Vec::new();
        let mut committed = 0u64;
        let mut errors = 0u64;
        let mut last_error = None;
        let mut cursor = 0usize;
        while !stop.load(Ordering::Relaxed) {
            let from = account_list[cursor % account_list.len()];
            let to = account_list[(cursor + 1) % account_list.len()];
            cursor += 1;
            let command = Command::transfer(request_id(), from, to, 1);
            let started = Instant::now();
            match cluster.try_submit(command) {
                Ok(result) => {
                    assert_success(result);
                    committed += 1;
                    latencies.push(started.elapsed().as_micros() as u64);
                }
                Err(SubmitError::Overloaded) => thread::yield_now(),
                Err(error) => {
                    errors += 1;
                    last_error = Some(format!("{error:?}"));
                }
            }
        }
        WorkerStats {
            shard,
            latencies_us: latencies,
            committed,
            errors,
            last_error,
        }
    })
}

#[derive(Debug)]
struct ProbeStats {
    commits: u64,
    errors: u64,
    max_latency_us: u64,
}

fn spawn_other_shard_probe(
    cluster: Arc<ReplicatedCluster>,
    probe_shard: ShardId,
    shards: u16,
    accounts: u64,
    migrating: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
) -> thread::JoinHandle<ProbeStats> {
    thread::spawn(move || {
        let account_list = accounts_for(probe_shard, shards, accounts);
        let mut stats = ProbeStats {
            commits: 0,
            errors: 0,
            max_latency_us: 0,
        };
        let mut cursor = 0usize;
        while !done.load(Ordering::Relaxed) {
            if !migrating.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(1));
                continue;
            }
            let from = account_list[cursor % account_list.len()];
            let to = account_list[(cursor + 1) % account_list.len()];
            cursor += 1;
            let command = Command::transfer(request_id(), from, to, 1);
            let started = Instant::now();
            match cluster.try_submit(command) {
                Ok(result) => {
                    assert_success(result);
                    stats.commits += 1;
                    stats.max_latency_us = stats
                        .max_latency_us
                        .max(started.elapsed().as_micros() as u64);
                }
                Err(_) => stats.errors += 1,
            }
        }
        stats
    })
}

fn spawn_leader_monitor(
    cluster: Arc<ReplicatedCluster>,
    shards: u16,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<u64> {
    thread::spawn(move || {
        let mut max_leaders = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let summary = cluster.summary();
            for shard in 0..shards {
                let leaders = summary
                    .get(&shard)
                    .map(|replicas| replicas.iter().filter(|replica| replica.leader).count())
                    .unwrap_or(0);
                max_leaders = max_leaders.max(leaders as u64);
            }
            thread::sleep(Duration::from_millis(15));
        }
        max_leaders
    })
}

fn pct(latencies: &[u64], pct: usize) -> u64 {
    if latencies.is_empty() {
        return 0;
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() - 1) * pct / 100).min(sorted.len() - 1);
    sorted[index]
}

fn print_latency(label: &str, stats: &WorkerStats) {
    println!(
        "m4: {label} shard={} committed={} errors={} p50={}us p99={}us p999={}us",
        stats.shard,
        stats.committed,
        stats.errors,
        pct(&stats.latencies_us, 50),
        pct(&stats.latencies_us, 99),
        pct(&stats.latencies_us, 999),
    );
    if let Some(error) = &stats.last_error {
        println!("m4: {label} last_error={error}");
    }
}

/// Online relocation of a hot shard under load:
/// - the logical route mapping never changes, only the epoch;
/// - only the migrated shard gets a bounded frozen window;
/// - the other shard keeps committing throughout;
/// - exactly one leader per shard at every sample;
/// - no committed write is lost or applied twice;
/// - the retired primary is fenced and cannot lead or apply new writes.
#[test]
fn online_migration_freezes_only_target_shard_and_fences_old_primary() {
    let _serial = serial();
    let dir = TempDir::new("online");
    let shards = 2u16;
    let accounts = 8u64;
    let initial_deposit = 1_000u64;
    let cluster = Arc::new(open_cluster(&dir.0, shards));
    seed_accounts(&cluster, accounts, initial_deposit);

    let stop = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let migrating = Arc::new(AtomicBool::new(false));

    let mut workers = Vec::new();
    for shard in 0..shards {
        for _ in 0..2 {
            let cluster = Arc::clone(&cluster);
            let stop = Arc::clone(&stop);
            workers.push(spawn_worker(
                Arc::clone(&cluster),
                shard,
                shards,
                accounts,
                Arc::clone(&stop),
            ));
        }
    }
    let monitor = spawn_leader_monitor(Arc::clone(&cluster), shards, Arc::clone(&stop));
    let probe = spawn_other_shard_probe(
        Arc::clone(&cluster),
        1,
        shards,
        accounts,
        Arc::clone(&migrating),
        Arc::clone(&done),
    );

    wait_until("load warms up", Duration::from_secs(10), || {
        // The worker threads increment their own counters; use summary instead.
        cluster
            .summary()
            .values()
            .all(|replicas| replicas.iter().all(|replica| replica.alive))
            && cluster
                .summary()
                .values()
                .all(|replicas| replicas.iter().filter(|replica| replica.leader).count() == 1)
    });

    let epoch_before = cluster.route_table().version();
    let old_route: Route = cluster
        .route(&Command::deposit(request_id(), 2, 1))
        .unwrap();
    let shard = cluster.shard(0).expect("shard 0").clone();
    let leader_before = shard.leader().expect("leader before migration");
    let voters_before = shard.membership().expect("membership before").0;
    let mut plan = ShardMigrationPlan::relocate_leader(0, 4);
    plan.catchup_timeout = Duration::from_secs(10);
    plan.switch_timeout = Duration::from_secs(6);

    migrating.store(true, Ordering::SeqCst);
    let migration = cluster.migrate_shard(&plan);
    migrating.store(false, Ordering::SeqCst);
    println!(
        "m4: migration status={} epoch={}->{} voters={:?}->{:?} leader={:?}->{:?} timings={:?}",
        migration.status,
        migration.epoch_before,
        migration.epoch_after,
        migration.voters_before,
        migration.voters_after,
        migration.leader_before,
        migration.leader_after,
        migration.timings
    );
    for event in &migration.events {
        println!(
            "m4: audit +{}ms {:?} {}",
            event.elapsed_ms, event.kind, event.detail
        );
    }

    thread::sleep(Duration::from_millis(300));
    done.store(true, Ordering::SeqCst);
    stop.store(true, Ordering::SeqCst);
    let probe = probe.join().expect("probe");
    let max_leaders = monitor.join().expect("monitor");
    let mut worker_stats = Vec::new();
    for worker in workers {
        worker_stats.push(worker.join().expect("worker"));
    }

    assert_eq!(
        migration.status,
        MigrationStatus::Completed,
        "migration failed: {:?}",
        migration.error
    );
    assert_eq!(migration.epoch_before, epoch_before);
    assert_eq!(
        migration.epoch_after,
        epoch_before + 1,
        "epoch must advance"
    );
    assert_eq!(migration.leader_before, Some(leader_before));
    assert_eq!(migration.voters_before, voters_before);
    assert!(
        migration.leader_after.is_some(),
        "no leader elected after migration"
    );
    assert_ne!(migration.leader_after, Some(leader_before));
    assert!(!migration.voters_after.contains(&leader_before));
    assert!(migration.voters_after.contains(&4));
    assert_eq!(migration.retired_nodes, vec![leader_before]);
    assert_eq!(max_leaders, 1, "dual leaders observed: {max_leaders}");
    assert_eq!(probe.errors, 0, "other shard failed during migration");
    assert!(
        probe.commits >= 3,
        "other shard did not keep serving during migration: {probe:?}"
    );
    assert!(
        probe.max_latency_us < 3_000_000,
        "other shard stalled during migration: {probe:?}"
    );
    println!(
        "m4: other-shard probe during migration commits={} max={}us",
        probe.commits, probe.max_latency_us
    );
    for stats in &worker_stats {
        print_latency("load", stats);
        assert_eq!(stats.errors, 0, "load errors: {stats:?}");
        assert!(stats.committed > 0);
    }
    for stats in worker_stats.iter().filter(|stats| stats.shard == 0) {
        assert!(
            pct(&stats.latencies_us, 99) < 6_000_000,
            "migrated shard P99 exceeded 6s: {stats:?}"
        );
    }
    assert!(
        migration.timings.freeze_ms < 2_000,
        "frozen window too long: {:?}",
        migration.timings
    );
    let catchup = migration.catchup.as_ref().expect("catch-up report");
    let target_position = migration
        .positions_before_promote
        .iter()
        .find(|position| position.id == 4)
        .expect("target position");
    assert_eq!(catchup.target, 4);
    assert_eq!(catchup.lag, Some(0), "target not exact at promotion");
    assert!(
        target_position.last_applied.unwrap_or(0) >= catchup.leader_applied.unwrap_or(0),
        "position check failed: {target_position:?} vs {catchup:?}"
    );

    // Idempotent write after the migration, plus the stale-epoch redirect.
    let deposit = Command::deposit(request_id(), 2, 7);
    let stale = cluster
        .submit_with_route(old_route, deposit)
        .expect_err("old epoch must be redirected");
    match stale {
        SubmitError::Route(RouteError::StaleRoute { current }) => {
            assert_eq!(current.version, migration.epoch_after);
            assert_eq!(current.shard_id, 0);
        }
        other => panic!("unexpected stale-route error: {other:?}"),
    }
    let first = submit_ok(&cluster, deposit);
    let retry = cluster.submit(deposit).expect("retry");
    assert_eq!(first, retry, "retry applied twice");

    let mut extra = BTreeMap::new();
    extra.insert(0u16, 7u128);
    let totals = expected_totals(shards, accounts, initial_deposit, &extra);
    wait_until(
        "replicas converge after migration",
        Duration::from_secs(15),
        || {
            let summary = cluster.summary();
            summary.get(&0).map(|replicas| {
                replicas
                    .iter()
                    .all(|replica| replica.alive && replica.total_balance == Some(totals[&0]))
            }) == Some(true)
                && summary.get(&1).map(|replicas| {
                    replicas
                        .iter()
                        .all(|replica| replica.alive && replica.total_balance == Some(totals[&1]))
                }) == Some(true)
        },
    );
    assert_replicas_match(&cluster, shards, accounts, &totals);

    // Route epochs are persisted for the next control-plane publication.
    let persisted = std::fs::read_to_string(dir.0.join("route.version")).unwrap();
    assert_eq!(persisted.trim(), migration.epoch_after.to_string());
    assert_eq!(cluster.route_table().version(), migration.epoch_after);

    // The retired primary is fenced: it cannot vote, lead or apply new writes.
    shard
        .restart_replica(leader_before)
        .expect("retired replica reopens from disk");
    thread::sleep(Duration::from_millis(150));
    let retired = shard
        .summary()
        .into_iter()
        .find(|replica| replica.id == leader_before)
        .expect("retired replica visible");
    assert!(retired.alive);
    assert!(!retired.voter, "retired replica must not be a voter");
    assert!(!retired.leader, "retired replica must not lead");
    let applied_before = retired.last_applied;
    assert_success(
        cluster
            .submit(Command::deposit(request_id(), 2, 3))
            .expect("write after restarting old primary"),
    );
    thread::sleep(Duration::from_millis(250));
    let retired = shard
        .summary()
        .into_iter()
        .find(|replica| replica.id == leader_before)
        .expect("retired replica visible");
    assert_eq!(
        retired.last_applied, applied_before,
        "retired replica applied new writes after being fenced"
    );
    assert_eq!(shard.leader(), migration.leader_after);
    shard
        .stop_replica(leader_before)
        .expect("retired replica stops");
    drop(shard);
    let cluster = Arc::try_unwrap(cluster).unwrap_or_else(|_| panic!("sole cluster owner"));
    cluster.shutdown().expect("cluster shutdown");
}

/// A failure before promotion must roll back cleanly: the old voters keep
/// serving, the temporary learner is discarded and no epoch is published.
#[test]
fn failed_migration_rolls_back_and_can_be_retried() {
    let _serial = serial();
    let dir = TempDir::new("rollback");
    let cluster = open_cluster(&dir.0, 1);
    seed_accounts(&cluster, 4, 1_000);

    let epoch_before = cluster.route_table().version();
    let leader_before = cluster.shard(0).unwrap().leader().expect("leader");
    let voters_before = cluster.shard(0).unwrap().membership().unwrap().0;
    let mut plan = ShardMigrationPlan::relocate_leader(0, 4);
    plan.catchup_timeout = Duration::from_secs(2);
    plan.switch_timeout = Duration::from_secs(2);
    plan.fault = Some(MigrationFault::CrashTargetBeforeLearner);

    let failed = cluster.migrate_shard(&plan);
    assert_eq!(failed.status, MigrationStatus::RolledBack);
    assert_eq!(
        failed.epoch_after, epoch_before,
        "rollback must not bump epoch"
    );
    assert_eq!(
        failed.voters_after, voters_before,
        "old voters must survive"
    );
    assert!(
        failed.error.is_some(),
        "rollback report must carry the failure reason"
    );
    assert!(
        failed
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("target"),
        "unexpected failure: {:?}",
        failed.error
    );
    assert_eq!(failed.retired_nodes, Vec::<u64>::new());
    assert!(failed
        .events
        .iter()
        .any(|event| event.kind == trading_replication::MigrationEventKind::RolledBack));
    let shard = cluster.shard(0).unwrap();
    assert_eq!(shard.leader(), Some(leader_before));
    assert_eq!(shard.live_replicas(), voters_before);
    assert_eq!(cluster.route_table().version(), epoch_before);
    assert_success(
        cluster
            .submit(Command::transfer(request_id(), 1, 2, 10))
            .expect("writes continue after rollback"),
    );

    // The same plan succeeds once the injected fault is removed.
    let mut retry = ShardMigrationPlan::relocate_leader(0, 4);
    retry.switch_timeout = Duration::from_secs(6);
    let report = cluster.migrate_shard(&retry);
    assert_eq!(
        report.status,
        MigrationStatus::Completed,
        "{:?}",
        report.error
    );
    assert_eq!(report.epoch_after, epoch_before + 1);
    assert!(report.voters_after.contains(&4));
    assert!(!report.voters_after.contains(&leader_before));
    let totals = expected_totals(1, 4, 1_000, &BTreeMap::new());
    assert_replicas_match(&cluster, 1, 4, &totals);
    cluster.shutdown().expect("cluster shutdown");
}

/// Scale out to four voters, then scale in by removing the current leader.
/// Both changes publish a new epoch and preserve every committed write.
#[test]
fn scale_out_and_scale_in_keep_quorum_and_data() {
    let _serial = serial();
    let dir = TempDir::new("scale");
    let cluster = open_cluster(&dir.0, 1);
    seed_accounts(&cluster, 4, 500);
    // Push past the snapshot threshold before the new voter joins.
    for _ in 0..45 {
        let id = request_id();
        submit_ok(&cluster, Command::deposit(id, 1, 1));
    }
    let epoch_before = cluster.route_table().version();

    let grown = cluster.migrate_shard(&ShardMigrationPlan::scale_out(0, 4));
    assert_eq!(
        grown.status,
        MigrationStatus::Completed,
        "{:?}",
        grown.error
    );
    assert_eq!(
        grown.voters_after,
        vec![1, 2, 3, 4],
        "scale-out must keep old voters"
    );
    assert_eq!(grown.epoch_after, epoch_before + 1);
    assert_eq!(grown.retired_nodes, Vec::<u64>::new());
    assert!(grown.catchup.as_ref().expect("catchup").lag == Some(0));
    let mut totals = expected_totals(1, 4, 500, &BTreeMap::new());
    totals.insert(0, totals[&0] + 45);
    wait_until("four voters converge", Duration::from_secs(15), || {
        cluster.summary()[&0]
            .iter()
            .all(|replica| replica.alive && replica.total_balance == Some(totals[&0]))
            && cluster.summary()[&0].len() == 4
    });
    assert_replicas_match(&cluster, 1, 4, &totals);

    let leader = cluster.shard(0).unwrap().leader().expect("leader");
    let shrunk = cluster.migrate_shard(&ShardMigrationPlan::scale_in(0, leader));
    assert_eq!(
        shrunk.status,
        MigrationStatus::Completed,
        "{:?}",
        shrunk.error
    );
    assert_eq!(shrunk.voters_before.len(), 4);
    assert_eq!(shrunk.voters_after.len(), 3);
    assert!(!shrunk.voters_after.contains(&leader));
    assert_eq!(shrunk.retired_nodes, vec![leader]);
    assert_eq!(shrunk.epoch_after, epoch_before + 2);
    assert_ne!(shrunk.leader_after, Some(leader));
    wait_until("three voters converge", Duration::from_secs(15), || {
        let summary = cluster.summary();
        summary[&0].len() == 3
            && summary[&0]
                .iter()
                .all(|replica| replica.alive && replica.total_balance == Some(totals[&0]))
    });
    assert_replicas_match(&cluster, 1, 4, &totals);
    submit_ok(&cluster, Command::transfer(request_id(), 1, 2, 25));
    cluster.shutdown().expect("cluster shutdown");
}

/// A migrated group reopens with its *new* voter set (retired replicas are
/// skipped) and the route epoch survives the restart.
#[test]
fn migrated_group_reopens_with_new_membership_and_epoch() {
    let _serial = serial();
    let dir = TempDir::new("reopen");
    let cluster = open_cluster(&dir.0, 1);
    seed_accounts(&cluster, 4, 200);
    for _ in 0..40 {
        submit_ok(&cluster, Command::deposit(request_id(), 1, 2));
    }
    let report = cluster.migrate_shard(&ShardMigrationPlan::relocate_leader(0, 4));
    assert_eq!(
        report.status,
        MigrationStatus::Completed,
        "{:?}",
        report.error
    );
    let expected_total = 4 * 200 + 80;
    let expected_epoch = report.epoch_after;
    let expected_voters = report.voters_after.clone();
    cluster.shutdown().expect("first shutdown");

    let reopened = open_cluster(&dir.0, 1);
    assert_eq!(
        reopened.route_table().version(),
        expected_epoch,
        "route epoch must be monotonic across restarts"
    );
    let placement = reopened.placement(0).expect("placement");
    assert_eq!(placement.voters, expected_voters);
    assert!(
        !placement.voters.contains(&report.retired_nodes[0]),
        "retired primary rejoined the voter set"
    );
    assert!(placement.leader.is_some());
    assert_eq!(
        reopened.summary()[&0].len(),
        expected_voters.len(),
        "retired replicas must be skipped on open"
    );
    let totals = BTreeMap::from([(0u16, expected_total as u128)]);
    wait_until("reopened group converges", Duration::from_secs(15), || {
        reopened.summary()[&0]
            .iter()
            .all(|replica| replica.alive && replica.total_balance == Some(expected_total as u128))
    });
    assert_replicas_match(&reopened, 1, 4, &totals);
    submit_ok(&reopened, Command::deposit(request_id(), 1, 11));
    wait_until("reopened group commits", Duration::from_secs(10), || {
        reopened.summary()[&0]
            .iter()
            .all(|replica| replica.total_balance == Some(expected_total as u128 + 11))
    });
    reopened.shutdown().expect("second shutdown");
}
