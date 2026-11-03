//! `--replicas` mode: run the load against Raft-replicated shards and, with
//! `--failover`, kill every shard leader mid-run and measure how the group
//! recovers (detection, stop-the-world window, catch-up).

use crate::{percentile, require_success, Config, XorShift, INITIAL_BALANCE, ROUTE_VERSION};
use std::error::Error;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use trading_partition::RouteTable;
use trading_protocol::{Command, CommandResult, SubmitError};
use trading_replication::{
    ReplicaSummary, ReplicatedCluster, ReplicatedClusterConfig, ReplicatedShardConfig,
};

/// How often a worker retries the same request on NoLeader/redirect before
/// counting it as failed. Retries reuse the request id: the state machine
/// deduplicates, so a failover can never double-apply.
const WORKER_RETRIES: usize = 5;

struct ReplicatedWorkerResult {
    latencies_ns: Vec<u64>,
    committed: u64,
    retried: u64,
    errors: u64,
}

#[derive(Debug)]
struct FailoverReport {
    shard: u16,
    old_leader: u64,
    new_leader: u64,
    election_ms: u128,
    catchup_ms: u128,
}

pub(crate) fn run(config: Config) -> Result<(), Box<dyn Error>> {
    println!(
        "mode=replicated replicas={} failover={} sync={:?}",
        config.replicas, config.failover, config.sync_policy
    );
    let routes = RouteTable::uniform(ROUTE_VERSION, config.shards)?;
    let cluster_config = ReplicatedClusterConfig {
        data_dir: config.data_dir.clone(),
        shard: ReplicatedShardConfig {
            replicas: config.replicas as u64,
            sync_policy: config.sync_policy,
            ..ReplicatedShardConfig::default()
        },
    };
    let cluster = Arc::new(ReplicatedCluster::open(cluster_config, routes)?);

    let mut setup_request = 1_u128;
    for account in 1..=config.accounts {
        require_success(cluster.submit(Command::open_account(setup_request, account))?)?;
        setup_request += 1;
        require_success(cluster.submit(Command::deposit(
            setup_request,
            account,
            INITIAL_BALANCE,
        ))?)?;
        setup_request += 1;
    }
    let setup_ops = setup_request - 1;

    let expected_balances = Arc::new(
        (0..=config.accounts)
            .map(|_| AtomicI64::new(INITIAL_BALANCE as i64))
            .collect::<Vec<_>>(),
    );
    let committed_progress = Arc::new(AtomicU64::new(0));

    let started = Instant::now();
    let mut workers = Vec::with_capacity(config.concurrency);
    for worker_id in 0..config.concurrency {
        let cluster = Arc::clone(&cluster);
        let expected_balances = Arc::clone(&expected_balances);
        let committed_progress = Arc::clone(&committed_progress);
        let count = config.ops / config.concurrency as u64
            + u64::from((worker_id as u64) < config.ops % config.concurrency as u64);
        let accounts = config.accounts;
        workers.push(thread::spawn(move || {
            run_replicated_worker(
                worker_id,
                count,
                accounts,
                cluster,
                expected_balances,
                committed_progress,
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

    let mut latencies_ns = Vec::new();
    let mut committed = 0_u64;
    let mut retried = 0_u64;
    let mut errors = 0_u64;
    for worker in workers {
        let result = worker.join().map_err(|_| "replicated worker panicked")?;
        committed += result.committed;
        retried += result.retried;
        errors += result.errors;
        latencies_ns.extend(result.latencies_ns);
    }
    let failover_reports = match failover_handle {
        Some(handle) => Some(
            handle
                .join()
                .map_err(|_| "failover drill thread panicked")?,
        ),
        None => None,
    };
    let elapsed = started.elapsed();
    latencies_ns.sort_unstable();

    // Wait for the slowest replica to reach the leader's applied index, then
    // reconcile every replica against the client-side expectation.
    wait_for_replica_convergence(&cluster, Duration::from_secs(30))?;
    verify_cluster(&cluster, &config, &expected_balances, setup_ops, committed)?;
    let expected_total = config.accounts as u128 * INITIAL_BALANCE as u128;
    for (shard_id, replicas) in cluster.summary() {
        let line: Vec<String> = replicas
            .iter()
            .map(|replica: &ReplicaSummary| {
                format!(
                    "#{} {}{} applied={} total={}",
                    replica.id,
                    replica.state,
                    if replica.leader { "(leader)" } else { "" },
                    replica.last_applied.map_or(-1, |index| index as i64),
                    replica.total_balance.map_or(-1, |total| total as i64)
                )
            })
            .collect();
        println!("shard={shard_id} replicas: {}", line.join(" | "));
    }

    let throughput = committed as f64 / elapsed.as_secs_f64();
    println!(
        "elapsed={:.3}s committed={committed} retries={retried} errors={errors} throughput={:.0} ops/s",
        elapsed.as_secs_f64(),
        throughput
    );
    println!(
        "p50={:.1}us p99={:.1}us p999={:.1}us",
        percentile(&latencies_ns, 500),
        percentile(&latencies_ns, 990),
        percentile(&latencies_ns, 999),
    );
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
        "replication_mode=raft replicas={} quorum={}",
        config.replicas,
        config.replicas / 2 + 1
    );
    let _ = expected_total;
    if errors != 0 {
        return Err(format!("replicated run observed {errors} failed operations").into());
    }
    Ok(())
}

fn run_replicated_worker(
    worker_id: usize,
    count: u64,
    accounts: u64,
    cluster: Arc<ReplicatedCluster>,
    expected_balances: Arc<Vec<AtomicI64>>,
    committed_progress: Arc<AtomicU64>,
) -> ReplicatedWorkerResult {
    let mut latencies_ns = Vec::new();
    let mut committed = 0;
    let mut retried = 0;
    let mut errors = 0;
    let mut random = XorShift::new(0x9E3779B97F4A7C15 ^ (worker_id as u64 + 1));
    for index in 0..count {
        let from = random.next() % accounts + 1;
        let mut to = random.next() % accounts + 1;
        if to == from {
            to = from % accounts + 1;
        }
        let request_id = ((worker_id as u128 + 1) << 64) | index as u128;
        let command = Command::transfer(request_id, from, to, 1);
        let started = Instant::now();
        let mut outcome: Result<CommandResult, SubmitError> = cluster.try_submit(command);
        let mut attempts = 0;
        loop {
            match outcome {
                Ok(result) => {
                    if result.is_success() {
                        expected_balances[from as usize].fetch_sub(1, AtomicOrdering::Relaxed);
                        expected_balances[to as usize].fetch_add(1, AtomicOrdering::Relaxed);
                        committed += 1;
                        committed_progress.fetch_add(1, AtomicOrdering::Relaxed);
                    } else {
                        errors += 1;
                    }
                    break;
                }
                // A failover in progress: retry the same request id, the
                // replicas deduplicate it, so no double transfer can happen.
                Err(SubmitError::NoLeader) | Err(SubmitError::ForwardToLeader { .. })
                    if attempts < WORKER_RETRIES =>
                {
                    attempts += 1;
                    retried += 1;
                    outcome = cluster.try_submit(command);
                }
                Err(_) => {
                    errors += 1;
                    break;
                }
            }
        }
        latencies_ns.push(started.elapsed().as_nanos() as u64);
    }
    ReplicatedWorkerResult {
        latencies_ns,
        committed,
        retried,
        errors,
    }
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

fn wait_for_replica_convergence(
    cluster: &ReplicatedCluster,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let summaries = cluster.summary();
        let mut leader_applied: Option<u64> = None;
        let mut converged = true;
        for replicas in summaries.values() {
            let leader = replicas
                .iter()
                .find(|replica| replica.leader)
                .and_then(|replica| replica.last_applied)
                .unwrap_or(0);
            leader_applied = Some(leader_applied.unwrap_or(leader).max(leader));
            if replicas
                .iter()
                .any(|replica| !replica.alive || replica.last_applied.unwrap_or(0) < leader)
            {
                converged = false;
            }
        }
        if converged && leader_applied.is_some() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
    Err("replicas did not converge within the reconciliation timeout".into())
}

fn verify_cluster(
    cluster: &ReplicatedCluster,
    config: &Config,
    expected_balances: &Arc<Vec<AtomicI64>>,
    setup_ops: u128,
    committed: u64,
) -> Result<(), Box<dyn Error>> {
    let expected_total = config.accounts as u128 * INITIAL_BALANCE as u128;
    let expected_results = setup_ops as u64 + committed;
    for (shard_id, replicas) in cluster.summary() {
        let leaders: Vec<&ReplicaSummary> =
            replicas.iter().filter(|replica| replica.leader).collect();
        if leaders.len() != 1 {
            return Err(format!("shard {shard_id} has {} leaders", leaders.len()).into());
        }
        let leader = leaders[0];
        let Some(leader_results) = leader.result_count else {
            return Err(format!("shard {shard_id} leader reports no state").into());
        };
        if leader_results as u64 > expected_results {
            return Err(format!(
                "shard {shard_id} idempotency results {leader_results} exceed submitted {expected_results}"
            )
            .into());
        }
        for replica in &replicas {
            if !replica.alive {
                return Err(format!("shard {shard_id} replica {} is down", replica.id).into());
            }
            let Some(total) = replica.total_balance else {
                return Err(format!("shard {shard_id} replica {} has no state", replica.id).into());
            };
            if total != expected_total {
                return Err(format!(
                    "shard {shard_id} replica {} balance conservation failed: expected {expected_total}, got {total}",
                    replica.id
                )
                .into());
            }
            let Some(accounts) = replica.account_count else {
                return Err(
                    format!("shard {shard_id} replica {} has no accounts", replica.id).into(),
                );
            };
            if accounts as u64 != config.accounts {
                return Err(format!(
                    "shard {shard_id} replica {} account count mismatch: expected {}, got {accounts}",
                    replica.id, config.accounts
                )
                .into());
            }
        }
    }
    // The client-side ledger must also close: transfers conserve the total.
    let ledger_total: u128 = (1..=config.accounts)
        .map(|account| {
            expected_balances[account as usize]
                .load(AtomicOrdering::Relaxed)
                .max(0) as u128
        })
        .sum();
    if ledger_total != expected_total {
        return Err(format!(
            "client ledger diverged: expected {expected_total}, got {ledger_total}"
        )
        .into());
    }
    Ok(())
}
