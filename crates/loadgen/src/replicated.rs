//! `--replicas` mode: run the load against Raft-replicated shards and, with
//! `--failover`/`--migrate`, drill leader failover and online shard migration
//! mid-run (election, stop-the-world window, catch-up, route convergence).

use crate::{percentile, require_success, Config, XorShift, INITIAL_BALANCE, ROUTE_VERSION};
use std::error::Error;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use trading_partition::RouteTable;
use trading_protocol::{Command, CommandResult, Route, RouteError, SubmitError};
use trading_replication::{
    MigrationReport, ReplicaSummary, ReplicatedCluster, ReplicatedClusterConfig,
    ReplicatedShardConfig, ShardMigrationPlan,
};

/// How often a worker retries the same request on NoLeader/redirect/stale
/// route before counting it as failed. Retries reuse the request id: the
/// state machine deduplicates, so a failover or migration can never double-apply.
const WORKER_RETRIES: usize = 20;

struct ReplicatedWorkerResult {
    /// `(start offset in us since run start, latency in ns)` per shard, so the
    /// migration window can be sliced out of the full-run latency histogram.
    shard_samples: Vec<Vec<(u64, u64)>>,
    executed_by_shard: Vec<u64>,
    retried: u64,
    redirects: u64,
    errors: u64,
    last_error: Option<String>,
}

#[derive(Debug)]
struct FailoverReport {
    shard: u16,
    old_leader: u64,
    new_leader: u64,
    election_ms: u128,
    catchup_ms: u128,
}

struct MigrationDrill {
    report: MigrationReport,
    started_offset_us: u64,
    ended_offset_us: u64,
}

pub(crate) fn run(config: Config) -> Result<(), Box<dyn Error>> {
    println!(
        "mode=replicated replicas={} failover={} migrate={} sync={:?}",
        config.replicas, config.failover, config.migrate, config.sync_policy
    );
    let routes = RouteTable::uniform(ROUTE_VERSION, config.shards)?;
    let shard_count = config.shards as usize;
    // Same-shard account lists keep every transfer atomic inside one shard;
    // the worker picks a shard first so load stays balanced across shards.
    let accounts_by_shard: Vec<Vec<u64>> = (0..config.shards)
        .map(|shard| {
            (1..=config.accounts)
                .filter(|account| routes.route_account(*account).shard_id == shard)
                .collect()
        })
        .collect();
    let cluster_config = ReplicatedClusterConfig {
        data_dir: config.data_dir.clone(),
        shard: ReplicatedShardConfig {
            replicas: config.replicas as u64,
            sync_policy: config.sync_policy,
            ..ReplicatedShardConfig::default()
        },
    };
    let cluster = Arc::new(ReplicatedCluster::open(cluster_config, routes.clone())?);

    let mut setup_request = 1_u128;
    let mut setup_ops_by_shard = vec![0_u64; shard_count];
    for account in 1..=config.accounts {
        let shard = routes.route_account(account).shard_id as usize;
        require_success(cluster.submit(Command::open_account(setup_request, account))?)?;
        setup_request += 1;
        require_success(cluster.submit(Command::deposit(
            setup_request,
            account,
            INITIAL_BALANCE,
        ))?)?;
        setup_request += 1;
        setup_ops_by_shard[shard] += 2;
    }
    let setup_ops: u64 = setup_ops_by_shard.iter().sum();

    let expected_balances = Arc::new(
        (0..=config.accounts)
            .map(|_| AtomicI64::new(INITIAL_BALANCE as i64))
            .collect::<Vec<_>>(),
    );
    let committed_progress = Arc::new(AtomicU64::new(0));
    let accounts_by_shard = Arc::new(accounts_by_shard);
    let run_started = Instant::now();

    let mut workers = Vec::with_capacity(config.concurrency);
    for worker_id in 0..config.concurrency {
        let cluster = Arc::clone(&cluster);
        let expected_balances = Arc::clone(&expected_balances);
        let committed_progress = Arc::clone(&committed_progress);
        let accounts_by_shard = Arc::clone(&accounts_by_shard);
        let count = config.ops / config.concurrency as u64
            + u64::from((worker_id as u64) < config.ops % config.concurrency as u64);
        let worker_routes = routes.clone();
        workers.push(thread::spawn(move || {
            run_replicated_worker(
                worker_id,
                count,
                Arc::clone(&accounts_by_shard),
                worker_routes,
                cluster,
                expected_balances,
                committed_progress,
                run_started,
            )
        }));
    }
    let failover_handle = if config.failover {
        let cluster = Arc::clone(&cluster);
        let progress = Arc::clone(&committed_progress);
        let shards = config.shards;
        let total = config.ops;
        Some(thread::spawn(move || {
            run_failover_drill(cluster, progress, total, shards)
        }))
    } else {
        None
    };
    let migration_handle = if config.migrate {
        let cluster = Arc::clone(&cluster);
        let progress = Arc::clone(&committed_progress);
        let total = config.ops;
        // Relocating the leader of shard 0 adds a fresh voter (scale-out), moves
        // the primary to it and retires the old primary (scale-in) in one plan.
        let target = config.replicas as u64 + 1;
        Some(thread::spawn(move || {
            run_migration_drill(cluster, progress, total, target, run_started)
        }))
    } else {
        None
    };

    let mut shard_samples: Vec<Vec<(u64, u64)>> = vec![Vec::new(); shard_count];
    let mut executed_by_shard = vec![0_u64; shard_count];
    let mut retried = 0_u64;
    let mut redirects = 0_u64;
    let mut errors = 0_u64;
    let mut last_error = None;
    for worker in workers {
        let result = worker.join().map_err(|_| "replicated worker panicked")?;
        retried += result.retried;
        redirects += result.redirects;
        errors += result.errors;
        if result.last_error.is_some() {
            last_error = result.last_error;
        }
        for shard in 0..shard_count {
            shard_samples[shard].extend(result.shard_samples[shard].iter().copied());
            executed_by_shard[shard] += result.executed_by_shard[shard];
        }
    }
    let failover_reports = match failover_handle {
        Some(handle) => Some(
            handle
                .join()
                .map_err(|_| "failover drill thread panicked")?,
        ),
        None => None,
    };
    let migration = match migration_handle {
        Some(handle) => {
            let drill = handle
                .join()
                .map_err(|_| "migration drill thread panicked")?;
            Some(drill?)
        }
        None => None,
    };
    let elapsed = run_started.elapsed();
    if let Some(drill) = &migration {
        println!(
            "migration status={} shard={} epoch={}->{} voters={:?}->{:?} leader={:?}->{:?} target={:?} retired={:?}",
            drill.report.status,
            drill.report.shard,
            drill.report.epoch_before,
            drill.report.epoch_after,
            drill.report.voters_before,
            drill.report.voters_after,
            drill.report.leader_before,
            drill.report.leader_after,
            drill.report.target_node,
            drill.report.retired_nodes,
        );
        println!(
            "migration_timings catchup_ms={} freeze_ms={} switch_ms={} election_ms={} total_ms={}",
            drill.report.timings.catchup_ms,
            drill.report.timings.freeze_ms,
            drill.report.timings.switch_ms,
            drill.report.timings.election_ms,
            drill.report.timings.total_ms,
        );
        for event in &drill.report.events {
            println!(
                "m4: audit +{}ms {:?} {}",
                event.elapsed_ms, event.kind, event.detail
            );
        }
    }

    // Wait for the slowest replica to reach the leader's applied index, then
    // reconcile every replica against the client-side expectation.
    wait_for_replica_convergence(&cluster, Duration::from_secs(30))?;
    for (shard_id, replicas) in cluster.summary() {
        let expected_balance: u128 = accounts_by_shard[shard_id as usize]
            .iter()
            .map(|account| {
                expected_balances[*account as usize]
                    .load(AtomicOrdering::Relaxed)
                    .max(0) as u128
            })
            .sum();
        let line: Vec<String> = replicas
            .iter()
            .map(|replica: &ReplicaSummary| {
                format!(
                    "#{} {}{} voter={} applied={} total={}",
                    replica.id,
                    replica.state,
                    if replica.leader { "(leader)" } else { "" },
                    replica.voter,
                    replica.last_applied.map_or(-1, |index| index as i64),
                    replica.total_balance.map_or(-1, |total| total as i64)
                )
            })
            .collect();
        println!(
            "shard={shard_id} accounts_expected={} total_expected={expected_balance} replicas: {}",
            accounts_by_shard[shard_id as usize].len(),
            line.join(" | ")
        );
    }
    verify_cluster(
        &cluster,
        &accounts_by_shard,
        &expected_balances,
        &setup_ops_by_shard,
        &executed_by_shard,
    )?;

    let mut latencies_ns: Vec<u64> = shard_samples
        .iter()
        .flat_map(|samples| samples.iter().map(|(_, latency)| *latency))
        .collect();
    latencies_ns.sort_unstable();
    let committed: u64 = executed_by_shard.iter().sum();
    let throughput = config.ops as f64 / elapsed.as_secs_f64();
    println!(
        "elapsed={:.3}s submitted={} executed={committed} retries={retried} redirects={redirects} errors={errors} throughput={:.0} ops/s",
        elapsed.as_secs_f64(),
        config.ops,
        throughput
    );
    if let Some(error) = &last_error {
        println!("last_error={error}");
    }
    println!(
        "p50={:.1}us p99={:.1}us p999={:.1}us",
        percentile(&latencies_ns, 500),
        percentile(&latencies_ns, 990),
        percentile(&latencies_ns, 999),
    );
    for (shard, samples) in shard_samples.iter().enumerate() {
        let mut shard_latencies: Vec<u64> = samples.iter().map(|(_, latency)| *latency).collect();
        shard_latencies.sort_unstable();
        println!(
            "shard={shard} accounts={} executed={} p50={:.1}us p99={:.1}us p999={:.1}us",
            accounts_by_shard[shard].len(),
            executed_by_shard[shard],
            percentile(&shard_latencies, 500),
            percentile(&shard_latencies, 990),
            percentile(&shard_latencies, 999),
        );
    }

    if let Some(drill) = &migration {
        let report = &drill.report;
        println!(
            "migration_pause freeze_ms={} observed_by_shard0_p99={:.1}us observed_by_shard0_max={:.1}us",
            report.timings.freeze_ms,
            percentile(&window_latencies(&shard_samples[0], drill), 990),
            window_max_latency(&shard_samples[0], drill),
        );
        for (shard, samples) in shard_samples.iter().enumerate() {
            let window = window_latencies(samples, drill);
            if window.is_empty() {
                continue;
            }
            let mut sorted = window.clone();
            sorted.sort_unstable();
            println!(
                "migration_window shard={shard} ops={} p99={:.1}us p999={:.1}us max={:.1}us",
                window.len(),
                percentile(&sorted, 990),
                percentile(&sorted, 999),
                window_max_latency(samples, drill),
            );
        }
        if cluster.route_table().version() != report.epoch_after {
            return Err(format!(
                "route epoch did not converge: report={} live={}",
                report.epoch_after,
                cluster.route_table().version()
            )
            .into());
        }
        println!(
            "route_epoch_before={} route_epoch_after={} stale_route_redirects={redirects}",
            report.epoch_before, report.epoch_after
        );
    }
    if let Some(reports) = failover_reports {
        for report in &reports {
            println!(
                "failover shard={} old_leader={} new_leader={} election_ms={} catchup_ms={}",
                report.shard,
                report.old_leader,
                report.new_leader,
                report.election_ms,
                report.catchup_ms
            );
        }
        let worst_election = reports.iter().map(|r| r.election_ms).max().unwrap_or(0);
        let worst_catchup = reports.iter().map(|r| r.catchup_ms).max().unwrap_or(0);
        println!(
            "rpo=0-committed failover_worst_election_ms={worst_election} failover_worst_catchup_ms={worst_catchup}"
        );
    }
    println!(
        "replication_mode=raft replicas={} quorum={} setup_ops={setup_ops}",
        config.replicas,
        config.replicas / 2 + 1
    );
    if errors != 0 {
        return Err(format!("replicated run observed {errors} failed operations").into());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_replicated_worker(
    worker_id: usize,
    count: u64,
    accounts_by_shard: Arc<Vec<Vec<u64>>>,
    routes: RouteTable,
    cluster: Arc<ReplicatedCluster>,
    expected_balances: Arc<Vec<AtomicI64>>,
    committed_progress: Arc<AtomicU64>,
    run_started: Instant,
) -> ReplicatedWorkerResult {
    let shard_count = accounts_by_shard.len();
    let mut shard_samples: Vec<Vec<(u64, u64)>> = vec![Vec::new(); shard_count];
    let mut executed_by_shard = vec![0_u64; shard_count];
    let mut retried = 0;
    let mut redirects = 0;
    let mut errors = 0;
    let mut last_error = None;
    // Each worker caches one route per shard. A migration bumps the epoch,
    // which the next submit rejects as `StaleRoute`; the worker refreshes the
    // cache and retries the same idempotent request.
    let mut cached_routes: Vec<Route> = accounts_by_shard
        .iter()
        .map(|accounts| routes.route_account(accounts[0]))
        .collect();
    let mut random = XorShift::new(0x9E3779B97F4A7C15 ^ (worker_id as u64 + 1));
    for index in 0..count {
        let shard = (random.next() % shard_count as u64) as usize;
        let accounts = &accounts_by_shard[shard];
        let position = random.next() as usize % accounts.len();
        let from = accounts[position];
        let mut to = accounts[(position + 1) % accounts.len()];
        if to == from {
            to = accounts[(position + 2) % accounts.len()];
        }
        let request_id = ((worker_id as u128 + 1) << 64) | index as u128;
        let command = Command::transfer(request_id, from, to, 1);
        let started_offset_us = run_started.elapsed().as_micros() as u64;
        let started = Instant::now();
        let mut outcome: Result<CommandResult, SubmitError> =
            cluster.try_submit_with_route(cached_routes[shard], command);
        let mut attempts = 0;
        loop {
            match outcome {
                Ok(result) => {
                    executed_by_shard[shard] += 1;
                    if result.is_success() {
                        expected_balances[from as usize].fetch_sub(1, AtomicOrdering::Relaxed);
                        expected_balances[to as usize].fetch_add(1, AtomicOrdering::Relaxed);
                        committed_progress.fetch_add(1, AtomicOrdering::Relaxed);
                    } else {
                        errors += 1;
                    }
                    break;
                }
                // The route epoch advanced under us: refresh and retry.
                Err(SubmitError::Route(RouteError::StaleRoute { current }))
                    if attempts < WORKER_RETRIES =>
                {
                    attempts += 1;
                    redirects += 1;
                    cached_routes[shard] = current;
                    outcome = cluster.try_submit_with_route(current, command);
                }
                // A failover or migration in progress: retry the same request
                // id, the replicas deduplicate it, so no double transfer can
                // happen.
                Err(SubmitError::NoLeader) | Err(SubmitError::ForwardToLeader { .. })
                    if attempts < WORKER_RETRIES =>
                {
                    attempts += 1;
                    retried += 1;
                    outcome = cluster.try_submit_with_route(cached_routes[shard], command);
                }
                Err(SubmitError::Overloaded) if attempts < WORKER_RETRIES => {
                    attempts += 1;
                    thread::sleep(Duration::from_micros(200));
                    outcome = cluster.try_submit_with_route(cached_routes[shard], command);
                }
                Err(error) => {
                    errors += 1;
                    last_error = Some(format!("{error:?}"));
                    break;
                }
            }
        }
        shard_samples[shard].push((started_offset_us, started.elapsed().as_nanos() as u64));
    }
    ReplicatedWorkerResult {
        shard_samples,
        executed_by_shard,
        retried,
        redirects,
        errors,
        last_error,
    }
}

/// Relocates shard 0 mid-run: creates a fourth replica, catches it up, fences
/// the shard only for the membership switch, retires the old primary and
/// publishes a new route epoch while the other shards keep serving.
fn run_migration_drill(
    cluster: Arc<ReplicatedCluster>,
    progress: Arc<AtomicU64>,
    total: u64,
    target: u64,
    run_started: Instant,
) -> Result<MigrationDrill, String> {
    // Start early enough that the load continues after the migration finishes.
    let trigger = (total / 10).max(1);
    while progress.load(AtomicOrdering::Relaxed) < trigger {
        thread::sleep(Duration::from_millis(5));
    }
    let started_offset_us = run_started.elapsed().as_micros() as u64;
    let mut plan = ShardMigrationPlan::relocate_leader(0, target);
    plan.catchup_timeout = Duration::from_secs(20);
    plan.switch_timeout = Duration::from_secs(10);
    let report = cluster.migrate_shard(&plan);
    let ended_offset_us = run_started.elapsed().as_micros() as u64;
    if !report.is_success() {
        return Err(format!(
            "migration drill failed: status={} error={:?}",
            report.status, report.error
        ));
    }
    Ok(MigrationDrill {
        report,
        started_offset_us,
        ended_offset_us,
    })
}

/// Crashes each shard's leader once the load is rolling, times the election,
/// then restarts the old leader and times its catch-up.
fn run_failover_drill(
    cluster: Arc<ReplicatedCluster>,
    progress: Arc<AtomicU64>,
    total: u64,
    shards: u16,
) -> Vec<FailoverReport> {
    // Fire the first crash once the run is warmed up.
    let trigger = total / 3;
    while progress.load(AtomicOrdering::Relaxed) < trigger {
        thread::sleep(Duration::from_millis(5));
    }
    let mut reports = Vec::new();
    for shard_id in 0..shards {
        let Ok(shard) = cluster.shard(shard_id) else {
            continue;
        };
        let Some(old_leader) = shard.leader() else {
            continue;
        };
        let crashed_at = Instant::now();
        shard.crash_replica(old_leader);
        // Detection: poll until a new leader reports in.
        let mut new_leader = None;
        while crashed_at.elapsed() < Duration::from_secs(30) {
            if let Some(leader) = shard.leader() {
                if leader != old_leader {
                    new_leader = Some(leader);
                    break;
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
        let Some(new_leader) = new_leader else {
            eprintln!("failover drill: shard {shard_id} elected no new leader");
            continue;
        };
        let election_ms = crashed_at.elapsed().as_millis();
        // Recovery: bring the crashed replica back and wait for catch-up.
        let restart_started = Instant::now();
        if let Err(error) = shard.restart_replica(old_leader) {
            eprintln!("failover drill: restart of replica {old_leader} failed: {error}");
            continue;
        }
        let target_applied = shard
            .summary()
            .iter()
            .find(|replica| replica.leader)
            .and_then(|replica| replica.last_applied)
            .unwrap_or(0);
        let mut caught_up = false;
        while restart_started.elapsed() < Duration::from_secs(30) {
            let applied = shard
                .summary()
                .iter()
                .find(|replica| replica.id == old_leader)
                .and_then(|replica| replica.last_applied)
                .unwrap_or(0);
            if applied >= target_applied {
                caught_up = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        let catchup_ms = restart_started.elapsed().as_millis();
        if !caught_up {
            eprintln!("failover drill: replica {old_leader} did not catch up in time");
        }
        reports.push(FailoverReport {
            shard: shard_id,
            old_leader,
            new_leader,
            election_ms,
            catchup_ms,
        });
    }
    reports
}

fn window_latencies(samples: &[(u64, u64)], drill: &MigrationDrill) -> Vec<u64> {
    samples
        .iter()
        .filter(|(started, _)| {
            *started >= drill.started_offset_us && *started <= drill.ended_offset_us
        })
        .map(|(_, latency)| *latency)
        .collect()
}

fn window_max_latency(samples: &[(u64, u64)], drill: &MigrationDrill) -> f64 {
    window_latencies(samples, drill)
        .into_iter()
        .max()
        .unwrap_or(0) as f64
        / 1000.0
}

fn wait_for_replica_convergence(
    cluster: &ReplicatedCluster,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let summaries = cluster.summary();
        let mut converged = true;
        for replicas in summaries.values() {
            let Some(leader) = replicas.iter().find(|replica| replica.leader) else {
                converged = false;
                continue;
            };
            let leader_applied = leader.last_applied.unwrap_or(0);
            if replicas
                .iter()
                .any(|replica| !replica.alive || replica.last_applied.unwrap_or(0) < leader_applied)
            {
                converged = false;
            }
        }
        if converged {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err("replicas did not converge within the reconciliation timeout".into())
}

fn verify_cluster(
    cluster: &ReplicatedCluster,
    accounts_by_shard: &[Vec<u64>],
    expected_balances: &Arc<Vec<AtomicI64>>,
    setup_ops_by_shard: &[u64],
    executed_by_shard: &[u64],
) -> Result<(), Box<dyn Error>> {
    let expected_total: u128 = (1..expected_balances.len())
        .map(|account| {
            expected_balances[account]
                .load(AtomicOrdering::Relaxed)
                .max(0) as u128
        })
        .sum();
    let mut observed_total = 0_u128;
    for (shard_id, replicas) in cluster.summary() {
        let shard = shard_id as usize;
        let leaders: Vec<&ReplicaSummary> =
            replicas.iter().filter(|replica| replica.leader).collect();
        if leaders.len() != 1 {
            return Err(format!("shard {shard_id} has {} leaders", leaders.len()).into());
        }
        let leader = leaders[0];
        let Some(leader_results) = leader.result_count else {
            return Err(format!("shard {shard_id} leader reports no state").into());
        };
        let submitted = setup_ops_by_shard[shard] + executed_by_shard[shard];
        if leader_results as u64 > submitted {
            return Err(format!(
                "shard {shard_id} idempotency results {leader_results} exceed submitted {submitted}"
            )
            .into());
        }
        let expected_accounts = accounts_by_shard[shard].len();
        let expected_balance: u128 = accounts_by_shard[shard]
            .iter()
            .map(|account| {
                expected_balances[*account as usize]
                    .load(AtomicOrdering::Relaxed)
                    .max(0) as u128
            })
            .sum();
        for replica in &replicas {
            if !replica.alive {
                return Err(format!("shard {shard_id} replica {} is down", replica.id).into());
            }
            let Some(total) = replica.total_balance else {
                return Err(format!("shard {shard_id} replica {} has no state", replica.id).into());
            };
            if total != expected_balance {
                return Err(format!(
                    "shard {shard_id} replica {} balance conservation failed: expected {expected_balance}, got {total}",
                    replica.id
                )
                .into());
            }
            let Some(accounts) = replica.account_count else {
                return Err(
                    format!("shard {shard_id} replica {} has no accounts", replica.id).into(),
                );
            };
            if accounts != expected_accounts {
                return Err(format!(
                    "shard {shard_id} replica {} account count mismatch: expected {expected_accounts}, got {accounts}",
                    replica.id
                )
                .into());
            }
        }
        observed_total += leaders[0].total_balance.unwrap_or(0);
    }
    // The client-side ledger must also close: transfers conserve the total.
    if observed_total != expected_total {
        return Err(format!(
            "client ledger diverged: expected {expected_total}, got {observed_total}"
        )
        .into());
    }
    Ok(())
}
