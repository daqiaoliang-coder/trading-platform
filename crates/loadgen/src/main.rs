use std::error::Error;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use trading_partition::{
    Engine, EngineConfig, EngineHandle, Partition, PartitionConfig, SyncPolicy,
};
use trading_protocol::{Command, SubmitError};

const INITIAL_BALANCE: u64 = 10_000_000_000;

#[derive(Debug)]
struct Config {
    ops: u64,
    concurrency: usize,
    accounts: u64,
    queue_capacity: usize,
    max_batch_size: usize,
    data_dir: PathBuf,
    sync_policy: SyncPolicy,
    checkpoint: bool,
}

impl Config {
    fn parse() -> Result<Option<Self>, Box<dyn Error>> {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mut config = Self {
            ops: 100_000,
            concurrency: 32,
            accounts: 1024,
            queue_capacity: 4096,
            max_batch_size: 64,
            data_dir: std::env::temp_dir()
                .join(format!("trading-loadgen-{}-{unique}", std::process::id())),
            sync_policy: SyncPolicy::Durable,
            checkpoint: false,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--ops" => config.ops = next(&mut args, "--ops")?,
                "--concurrency" => config.concurrency = next(&mut args, "--concurrency")?,
                "--accounts" => config.accounts = next(&mut args, "--accounts")?,
                "--queue" => config.queue_capacity = next(&mut args, "--queue")?,
                "--batch" => config.max_batch_size = next(&mut args, "--batch")?,
                "--data-dir" => {
                    config.data_dir = PathBuf::from(next::<String, _>(&mut args, "--data-dir")?)
                }
                "--unsafe-buffered" => config.sync_policy = SyncPolicy::UnsafeBuffered,
                "--checkpoint" => config.checkpoint = true,
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
        if config.queue_capacity == 0 || config.max_batch_size == 0 {
            return Err("queue and batch must be positive".into());
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
    println!("trading-loadgen [--ops N] [--concurrency N] [--accounts N] [--queue N] [--batch N]");
    println!("                [--data-dir PATH] [--unsafe-buffered] [--checkpoint]");
}

struct WorkerResult {
    latencies_ns: Vec<u64>,
    errors: u64,
}

fn main() -> Result<(), Box<dyn Error>> {
    let Some(config) = Config::parse()? else {
        return Ok(());
    };
    println!("data_dir={}", config.data_dir.display());
    println!(
        "ops={} concurrency={} accounts={} batch={} durability={:?}",
        config.ops, config.concurrency, config.accounts, config.max_batch_size, config.sync_policy
    );

    let partition_config = PartitionConfig {
        data_dir: config.data_dir.clone(),
        sync_policy: config.sync_policy,
    };
    let partition = Partition::open(partition_config.clone())?;
    let engine = Engine::new(
        partition,
        EngineConfig {
            queue_capacity: config.queue_capacity,
            max_batch_size: config.max_batch_size,
            batch_wait: Duration::from_micros(200),
        },
    );
    let handle = Arc::new(engine.spawn());

    let mut setup_request = 1_u128;
    for account in 1..=config.accounts {
        require_success(handle.submit(Command::open_account(setup_request, account))?)?;
        setup_request += 1;
        require_success(handle.submit(Command::deposit(
            setup_request,
            account,
            INITIAL_BALANCE,
        ))?)?;
        setup_request += 1;
    }

    let expected_balances = Arc::new(
        (0..=config.accounts)
            .map(|_| AtomicI64::new(INITIAL_BALANCE as i64))
            .collect::<Vec<_>>(),
    );
    let started = Instant::now();
    let mut workers = Vec::with_capacity(config.concurrency);
    for worker_id in 0..config.concurrency {
        let handle = Arc::clone(&handle);
        let expected_balances = Arc::clone(&expected_balances);
        let count = config.ops / config.concurrency as u64
            + u64::from((worker_id as u64) < config.ops % config.concurrency as u64);
        let accounts = config.accounts;
        workers.push(thread::spawn(move || {
            run_worker(worker_id, count, accounts, handle, expected_balances)
        }));
    }

    let mut latencies = Vec::with_capacity(config.ops as usize);
    let mut errors = 0_u64;
    for worker in workers {
        let result = worker
            .join()
            .map_err(|_| "load generator worker panicked")?;
        errors += result.errors;
        latencies.extend(result.latencies_ns);
    }
    let elapsed = started.elapsed();
    latencies.sort_unstable();

    let handle = match Arc::try_unwrap(handle) {
        Ok(handle) => handle,
        Err(_) => return Err("engine handle still has outstanding owners".into()),
    };
    let mut partition = handle.shutdown()?;
    let expected_sequence = config.accounts * 2 + config.ops - errors;
    let expected_results = expected_sequence;
    if partition.last_sequence() != expected_sequence {
        return Err(format!(
            "sequence mismatch: expected {expected_sequence}, got {}",
            partition.last_sequence()
        )
        .into());
    }
    let mut recovery_verified = false;
    if config.checkpoint {
        partition.checkpoint()?;
        drop(partition);
        partition = Partition::open(partition_config)?;
        if partition.last_sequence() != expected_sequence
            || partition.account_count() != config.accounts as usize
            || partition.result_count() != expected_results as usize
        {
            return Err("checkpoint recovery metadata mismatch".into());
        }
        recovery_verified = true;
    }

    for (account, expected) in expected_balances.iter().enumerate().skip(1) {
        let expected = expected.load(AtomicOrdering::Relaxed) as u64;
        let actual = partition
            .balance(account as u64)
            .ok_or_else(|| format!("missing account {account} after execution"))?;
        if actual != expected {
            return Err(format!(
                "account {account} balance mismatch: expected {expected}, got {actual}"
            )
            .into());
        }
    }
    if partition.account_count() != config.accounts as usize {
        return Err(format!(
            "account count mismatch: expected {}, got {}",
            config.accounts,
            partition.account_count()
        )
        .into());
    }
    if partition.result_count() != expected_results as usize {
        return Err(format!(
            "idempotency result mismatch: expected {expected_results}, got {}",
            partition.result_count()
        )
        .into());
    }
    let expected_balance = config.accounts as u128 * INITIAL_BALANCE as u128;
    if partition.total_balance() != expected_balance {
        return Err(format!(
            "balance conservation failed: expected {expected_balance}, got {}",
            partition.total_balance()
        )
        .into());
    }
    let throughput = config.ops as f64 / elapsed.as_secs_f64();
    println!(
        "elapsed={:.3}s throughput={:.0} ops/s errors={errors}",
        elapsed.as_secs_f64(),
        throughput
    );
    println!(
        "p50={:.1}us p99={:.1}us p999={:.1}us",
        percentile(&latencies, 500),
        percentile(&latencies, 990),
        percentile(&latencies, 999)
    );
    println!(
        "last_sequence={} accounts={} idempotency_results={} total_balance={}",
        partition.last_sequence(),
        partition.account_count(),
        partition.result_count(),
        partition.total_balance()
    );
    println!(
        "checkpoint={} recovery_verified={}",
        config.checkpoint, recovery_verified
    );
    if errors != 0 {
        return Err(format!("load generator observed {errors} failed operations").into());
    }
    Ok(())
}

fn run_worker(
    worker_id: usize,
    count: u64,
    accounts: u64,
    handle: Arc<EngineHandle>,
    expected_balances: Arc<Vec<AtomicI64>>,
) -> WorkerResult {
    let mut latencies_ns = Vec::with_capacity(count as usize);
    let mut errors = 0;
    let mut random = XorShift::new(0x9E3779B97F4A7C15 ^ (worker_id as u64 + 1));
    for index in 0..count {
        let from = random.next() % accounts + 1;
        let to = (from + random.next() % (accounts - 1)) % accounts + 1;
        let request_id = ((worker_id as u128 + 1) << 64) | index as u128;
        let started = Instant::now();
        match handle.submit(Command::transfer(request_id, from, to, 1)) {
            Ok(result) if result.is_success() => {
                expected_balances[from as usize].fetch_sub(1, AtomicOrdering::Relaxed);
                expected_balances[to as usize].fetch_add(1, AtomicOrdering::Relaxed);
            }
            Ok(_)
            | Err(SubmitError::Overloaded)
            | Err(SubmitError::Stopped)
            | Err(SubmitError::Storage(_)) => errors += 1,
        }
        latencies_ns.push(started.elapsed().as_nanos() as u64);
    }
    WorkerResult {
        latencies_ns,
        errors,
    }
}

fn require_success(result: trading_protocol::CommandResult) -> Result<(), Box<dyn Error>> {
    if result.is_success() {
        Ok(())
    } else {
        Err(format!("setup command failed: {result:?}").into())
    }
}

fn percentile(sorted_ns: &[u64], per_mille: usize) -> f64 {
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let index = (sorted_ns.len() - 1) * per_mille / 1000;
    sorted_ns[index] as f64 / 1000.0
}

struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}
