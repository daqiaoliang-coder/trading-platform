use std::error::Error;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use trading_partition::{ClusterConfig, RouteTable, ShardedCluster, SyncPolicy};
use trading_projection::PostgresProjection;
use trading_protocol::{Command, SubmitError};

mod replicated;

pub(crate) const INITIAL_BALANCE: u64 = 10_000_000_000;
pub(crate) const ROUTE_VERSION: u64 = 1;

#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) ops: u64,
    pub(crate) concurrency: usize,
    pub(crate) accounts: u64,
    pub(crate) shards: u16,
    pub(crate) queue_capacity: usize,
    pub(crate) max_batch_size: usize,
    pub(crate) data_dir: PathBuf,
    pub(crate) sync_policy: SyncPolicy,
    pub(crate) checkpoint: bool,
    pub(crate) postgres_url: Option<String>,
    pub(crate) replicas: u16,
    pub(crate) failover: bool,
    pub(crate) migrate: bool,
}

impl Config {
    fn parse() -> Result<Option<Self>, Box<dyn Error>> {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mut config = Self {
            ops: 100_000,
            concurrency: 32,
            accounts: 1024,
            shards: 1,
            queue_capacity: 4096,
            max_batch_size: 64,
            data_dir: std::env::temp_dir()
                .join(format!("trading-loadgen-{}-{unique}", std::process::id())),
            sync_policy: SyncPolicy::Durable,
            checkpoint: false,
            postgres_url: None,
            replicas: 0,
            failover: false,
            migrate: false,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--ops" => config.ops = next(&mut args, "--ops")?,
                "--concurrency" => config.concurrency = next(&mut args, "--concurrency")?,
                "--accounts" => config.accounts = next(&mut args, "--accounts")?,
                "--shards" => config.shards = next(&mut args, "--shards")?,
                "--queue" => config.queue_capacity = next(&mut args, "--queue")?,
                "--batch" => config.max_batch_size = next(&mut args, "--batch")?,
                "--data-dir" => {
                    config.data_dir = PathBuf::from(next::<String, _>(&mut args, "--data-dir")?)
                }
                "--unsafe-buffered" => config.sync_policy = SyncPolicy::UnsafeBuffered,
                "--checkpoint" => config.checkpoint = true,
                "--postgres-url" => config.postgres_url = Some(next(&mut args, "--postgres-url")?),
                "--replicas" => config.replicas = next(&mut args, "--replicas")?,
                "--failover" => config.failover = true,
                "--migrate" => config.migrate = true,
                "--help" | "-h" => {
                    print_help();
                    return Ok(None);
                }
                _ => return Err(format!("unknown argument: {arg}").into()),
            }
        }
        if config.ops == 0 || config.concurrency == 0 || config.accounts < 2 {
            return Err("ops/concurrency must be positive and accounts must be at least 2".into());
        }
        if config.shards == 0 || config.accounts < config.shards as u64 * 2 {
            return Err("shards must be positive and accounts must be at least 2 per shard".into());
        }
        if config.queue_capacity == 0 || config.max_batch_size == 0 {
            return Err("queue and batch must be positive".into());
        }
        if config.replicas == 1 {
            return Err("--replicas must be 0 (single-writer) or >= 2 (replicated)".into());
        }
        if config.replicas > 0 && config.replicas.is_multiple_of(2) {
            return Err("--replicas must be odd for a healthy quorum".into());
        }
        if config.failover && config.replicas == 0 {
            return Err("--failover requires --replicas >= 3".into());
        }
        if config.failover && config.replicas < 3 {
            return Err("--failover requires --replicas >= 3 to keep a majority".into());
        }
        if config.migrate && config.replicas == 0 {
            return Err("--migrate requires --replicas >= 3".into());
        }
        if config.migrate && config.replicas < 3 {
            return Err("--migrate requires --replicas >= 3 to keep a majority".into());
        }
        if config.migrate && config.failover {
            return Err(
                "--migrate and --failover are separate drills; run them one at a time".into(),
            );
        }
        if config.replicas > 0 && (config.checkpoint || config.postgres_url.is_some()) {
            return Err("--replicas mode does not combine with --checkpoint/--postgres-url".into());
        }
        if config.data_dir.exists() && std::fs::read_dir(&config.data_dir)?.next().is_some() {
            return Err("data-dir must be new or empty".into());
        }
        Ok(Some(config))
    }
}

fn next<T, I>(args: &mut I, name: &str) -> Result<T, Box<dyn Error>>
where
    T: std::str::FromStr,
    T::Err: Error + 'static,
    I: Iterator<Item = String>,
{
    let value = args
        .next()
        .ok_or_else(|| format!("missing value for {name}"))?;
    Ok(value.parse()?)
}

fn print_help() {
    println!("trading-loadgen [--ops N] [--concurrency N] [--accounts N] [--shards N]");
    println!("                [--queue N] [--batch N] [--data-dir PATH]");
    println!("                [--unsafe-buffered] [--checkpoint] [--postgres-url URL]");
    println!("                [--replicas N] [--failover] [--migrate]");
    println!();
    println!("  --replicas N   shard-level Raft replication with N in-process replicas");
    println!("  --failover     drill: kill each shard leader mid-run, measure failover");
    println!("  --migrate      drill: relocate shard 0 to a fresh voter mid-run and retire");
    println!("                 its old leader (scale-out + move + scale-in under load)");
}

struct WorkerResult {
    shard_latencies_ns: Vec<Vec<u64>>,
    committed_by_shard: Vec<u64>,
    errors: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let Some(config) = Config::parse()? else {
        return Ok(());
    };
    println!("data_dir={}", config.data_dir.display());
    println!(
        "ops={} concurrency={} accounts={} shards={} route_version={} batch={} durability={:?}",
        config.ops,
        config.concurrency,
        config.accounts,
        config.shards,
        ROUTE_VERSION,
        config.max_batch_size,
        config.sync_policy
    );
    if config.replicas > 0 {
        return replicated::run(config);
    }

    let cluster_config = ClusterConfig {
        data_dir: config.data_dir.clone(),
        sync_policy: config.sync_policy,
        engine: trading_partition::EngineConfig {
            queue_capacity: config.queue_capacity,
            max_batch_size: config.max_batch_size,
            batch_wait: Duration::from_micros(200),
        },
    };
    let routes = RouteTable::uniform(ROUTE_VERSION, config.shards)?;
    let (event_sender, projection) = match &config.postgres_url {
        Some(url) => {
            let (sender, receiver) = mpsc::channel();
            (
                Some(sender),
                Some(PostgresProjection::spawn(url, receiver)?),
            )
        }
        None => (None, None),
    };
    let cluster = match event_sender {
        Some(sender) => {
            ShardedCluster::open_with_event_sender(cluster_config.clone(), routes.clone(), sender)
        }
        None => ShardedCluster::open(cluster_config.clone(), routes.clone()),
    }?;
    let cluster = Arc::new(cluster);

    let shard_count = config.shards as usize;
    let mut accounts_by_shard = vec![Vec::new(); shard_count];
    let mut expected_sequences = vec![0_u64; shard_count];
    let mut setup_request = 1_u128;
    for account in 1..=config.accounts {
        let shard = cluster.route_account(account).shard_id as usize;
        accounts_by_shard[shard].push(account);
        require_success(cluster.submit(Command::open_account(setup_request, account))?)?;
        setup_request += 1;
        require_success(cluster.submit(Command::deposit(
            setup_request,
            account,
            INITIAL_BALANCE,
        ))?)?;
        setup_request += 1;
        expected_sequences[shard] += 2;
    }

    let accounts_by_shard = Arc::new(accounts_by_shard);
    let expected_balances = Arc::new(
        (0..=config.accounts)
            .map(|_| AtomicI64::new(INITIAL_BALANCE as i64))
            .collect::<Vec<_>>(),
    );
    let started = Instant::now();
    let mut workers = Vec::with_capacity(config.concurrency);
    for worker_id in 0..config.concurrency {
        let cluster = Arc::clone(&cluster);
        let accounts_by_shard = Arc::clone(&accounts_by_shard);
        let expected_balances = Arc::clone(&expected_balances);
        let count = config.ops / config.concurrency as u64
            + u64::from((worker_id as u64) < config.ops % config.concurrency as u64);
        let accounts = config.accounts;
        workers.push(thread::spawn(move || {
            run_worker(
                worker_id,
                count,
                accounts,
                cluster,
                accounts_by_shard,
                expected_balances,
            )
        }));
    }

    let mut shard_latencies_ns = vec![Vec::new(); shard_count];
    let mut committed_by_shard = vec![0_u64; shard_count];
    let mut errors = 0_u64;
    for worker in workers {
        let result = worker
            .join()
            .map_err(|_| "load generator worker panicked")?;
        errors += result.errors;
        for shard in 0..shard_count {
            committed_by_shard[shard] += result.committed_by_shard[shard];
            shard_latencies_ns[shard].extend(result.shard_latencies_ns[shard].iter().copied());
        }
    }
    let elapsed = started.elapsed();
    let mut latencies_ns: Vec<_> = shard_latencies_ns
        .iter()
        .flat_map(|latencies| latencies.iter().copied())
        .collect();
    latencies_ns.sort_unstable();
    for latencies in &mut shard_latencies_ns {
        latencies.sort_unstable();
    }
    for (shard, successful) in committed_by_shard.iter().enumerate() {
        expected_sequences[shard] += successful;
    }

    let metrics = cluster.metrics();
    let cluster = match Arc::try_unwrap(cluster) {
        Ok(cluster) => cluster,
        Err(_) => return Err("cluster handle still has outstanding owners".into()),
    };
    let mut partitions = cluster.shutdown()?;
    let projection_stats = projection.map(|handle| handle.shutdown()).transpose()?;
    if config.checkpoint {
        for partition in partitions.values_mut() {
            partition.checkpoint()?;
        }
        drop(partitions);
        let recovered = ShardedCluster::open(cluster_config, routes)?;
        partitions = recovered.shutdown()?;
    }

    let mut total_accounts = 0_usize;
    let mut total_results = 0_usize;
    let mut total_balance = 0_u128;
    for shard in 0..shard_count {
        let partition = partitions
            .get(&(shard as u16))
            .ok_or_else(|| format!("missing shard {shard} after execution"))?;
        let expected_sequence = expected_sequences[shard];
        if partition.last_sequence() != expected_sequence {
            return Err(format!(
                "shard {shard} sequence mismatch: expected {expected_sequence}, got {}",
                partition.last_sequence()
            )
            .into());
        }
        if partition.result_count() != expected_sequence as usize {
            return Err(format!(
                "shard {shard} idempotency result mismatch: expected {expected_sequence}, got {}",
                partition.result_count()
            )
            .into());
        }
        if partition.account_count() != accounts_by_shard[shard].len() {
            return Err(format!(
                "shard {shard} account count mismatch: expected {}, got {}",
                accounts_by_shard[shard].len(),
                partition.account_count()
            )
            .into());
        }
        for account in &accounts_by_shard[shard] {
            let expected =
                expected_balances[*account as usize].load(AtomicOrdering::Relaxed) as u64;
            let actual = partition
                .balance(*account)
                .ok_or_else(|| format!("missing account {account} after execution"))?;
            if actual != expected {
                return Err(format!(
                    "account {account} balance mismatch: expected {expected}, got {actual}"
                )
                .into());
            }
        }
        let expected_balance = accounts_by_shard[shard].len() as u128 * INITIAL_BALANCE as u128;
        if partition.total_balance() != expected_balance {
            return Err(format!(
                "shard {shard} balance conservation failed: expected {expected_balance}, got {}",
                partition.total_balance()
            )
            .into());
        }
        if metrics[&(shard as u16)].last_sequence != expected_sequence {
            return Err(format!("shard {shard} metrics sequence mismatch").into());
        }
        total_accounts += partition.account_count();
        total_results += partition.result_count();
        total_balance += partition.total_balance();
    }
    let expected_sequence = expected_sequences.iter().sum::<u64>();
    if let Some(stats) = projection_stats {
        if stats.received != expected_sequence {
            return Err(format!(
                "projection event mismatch: expected {expected_sequence}, got {}",
                stats.received
            )
            .into());
        }
    }

    let throughput = config.ops as f64 / elapsed.as_secs_f64();
    println!(
        "elapsed={:.3}s throughput={:.0} ops/s errors={errors}",
        elapsed.as_secs_f64(),
        throughput
    );
    println!(
        "p50={:.1}us p99={:.1}us p999={:.1}us",
        percentile(&latencies_ns, 500),
        percentile(&latencies_ns, 990),
        percentile(&latencies_ns, 999)
    );
    for shard in 0..shard_count {
        let metric = &metrics[&(shard as u16)];
        let average_batch_us = if metric.batches == 0 {
            0.0
        } else {
            metric.total_batch_latency_ns as f64 / metric.batches as f64 / 1000.0
        };
        let load_share = if metric.executed == 0 {
            0.0
        } else {
            metric.executed as f64 / expected_sequence as f64 * 100.0
        };
        println!(
            "shard={} accounts={} ops={} sequence={} load={:.1}% queue={}/{} rejected={} batches={} avg_batch={:.1}us max_batch={:.1}us p50={:.1}us p99={:.1}us p999={:.1}us",
            shard,
            accounts_by_shard[shard].len(),
            committed_by_shard[shard],
            partitions[&(shard as u16)].last_sequence(),
            load_share,
            metric.queue_depth,
            metric.queue_capacity,
            metric.overloaded,
            metric.batches,
            average_batch_us,
            metric.max_batch_latency_ns as f64 / 1000.0,
            percentile(&shard_latencies_ns[shard], 500),
            percentile(&shard_latencies_ns[shard], 990),
            percentile(&shard_latencies_ns[shard], 999),
        );
    }
    println!(
        "last_sequence_sum={} accounts={} idempotency_results={} total_balance={}",
        expected_sequence, total_accounts, total_results, total_balance
    );
    println!(
        "checkpoint={} recovery_verified={}",
        config.checkpoint, config.checkpoint
    );
    if let Some(stats) = projection_stats {
        println!(
            "projection_events={} projection_inserted={}",
            stats.received, stats.inserted
        );
    }
    if errors != 0 {
        return Err(format!("load generator observed {errors} failed operations").into());
    }
    Ok(())
}

fn run_worker(
    worker_id: usize,
    count: u64,
    accounts: u64,
    cluster: Arc<ShardedCluster>,
    accounts_by_shard: Arc<Vec<Vec<u64>>>,
    expected_balances: Arc<Vec<AtomicI64>>,
) -> WorkerResult {
    let mut shard_latencies_ns = vec![Vec::new(); accounts_by_shard.len()];
    let mut committed_by_shard = vec![0_u64; accounts_by_shard.len()];
    let mut errors = 0;
    let mut random = XorShift::new(0x9E3779B97F4A7C15 ^ (worker_id as u64 + 1));
    for index in 0..count {
        let from = random.next() % accounts + 1;
        let route = cluster.route_account(from);
        let shard = route.shard_id as usize;
        let shard_accounts = &accounts_by_shard[shard];
        let candidate = random.next() as usize % shard_accounts.len();
        let mut to = shard_accounts[candidate];
        if to == from {
            to = shard_accounts[(candidate + 1) % shard_accounts.len()];
        }
        let request_id = ((worker_id as u128 + 1) << 64) | index as u128;
        let started = Instant::now();
        match cluster.submit_with_route(route, Command::transfer(request_id, from, to, 1)) {
            Ok(result) => {
                committed_by_shard[shard] += 1;
                if result.is_success() {
                    expected_balances[from as usize].fetch_sub(1, AtomicOrdering::Relaxed);
                    expected_balances[to as usize].fetch_add(1, AtomicOrdering::Relaxed);
                } else {
                    errors += 1;
                }
            }
            Err(SubmitError::Overloaded)
            | Err(SubmitError::Stopped)
            | Err(SubmitError::Storage(_))
            | Err(SubmitError::Route(_))
            // Replication-only errors cannot occur in single-writer mode.
            | Err(SubmitError::ForwardToLeader { .. })
            | Err(SubmitError::NoLeader) => errors += 1,
        }
        shard_latencies_ns[shard].push(started.elapsed().as_nanos() as u64);
    }
    WorkerResult {
        shard_latencies_ns,
        committed_by_shard,
        errors,
    }
}

pub(crate) fn require_success(
    result: trading_protocol::CommandResult,
) -> Result<(), Box<dyn Error>> {
    if result.is_success() {
        Ok(())
    } else {
        Err(format!("setup command failed: {result:?}").into())
    }
}

pub(crate) fn percentile(sorted_ns: &[u64], per_mille: usize) -> f64 {
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let index = (sorted_ns.len() - 1) * per_mille / 1000;
    sorted_ns[index] as f64 / 1000.0
}

pub(crate) struct XorShift(u64);

impl XorShift {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}
