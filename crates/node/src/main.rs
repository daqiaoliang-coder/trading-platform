//! `trading-node`: hosts a replicated cluster and exposes the local admin API
//! consumed by the Go control plane. The admin listener is deliberately not
//! the client trading path; it only serves placement, route and migration
//! orchestration calls on the operator network.

mod admin;

use std::error::Error;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use trading_partition::RouteTable;
use trading_replication::{ReplicatedCluster, ReplicatedClusterConfig, ReplicatedShardConfig};
use trading_storage::SyncPolicy;

const ROUTE_VERSION: u64 = 1;

struct Args {
    data_dir: PathBuf,
    shards: u16,
    replicas: u64,
    sync_policy: SyncPolicy,
    snapshot_threshold: u64,
    admin_addr: String,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data/node"),
            shards: 1,
            replicas: 3,
            sync_policy: SyncPolicy::Durable,
            snapshot_threshold: 1000,
            admin_addr: "127.0.0.1:8081".to_string(),
        }
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("trading-node: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    std::fs::create_dir_all(&args.data_dir)?;
    let routes = RouteTable::uniform(ROUTE_VERSION, args.shards)?;
    let config = ReplicatedClusterConfig {
        data_dir: args.data_dir.clone(),
        shard: ReplicatedShardConfig {
            replicas: args.replicas,
            sync_policy: args.sync_policy,
            snapshot_threshold: args.snapshot_threshold,
            ..ReplicatedShardConfig::default()
        },
    };
    let cluster = Arc::new(ReplicatedCluster::open(config, routes)?);
    let listener = TcpListener::bind(&args.admin_addr)?;
    let admin_addr = listener.local_addr()?;
    // The control plane and integration tests need the actual port when the
    // listener is started with `:0`.
    let addr_file = args.data_dir.join("admin.addr");
    std::fs::write(&addr_file, format!("{admin_addr}\n"))?;
    println!(
        "trading-node admin={admin_addr} data_dir={} shards={} replicas={} route_version={} durability={:?} addr_file={}",
        args.data_dir.display(),
        args.shards,
        args.replicas,
        cluster.route_table().version(),
        args.sync_policy,
        addr_file.display(),
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    admin::serve(Arc::clone(&cluster), listener, Arc::clone(&shutdown))?;
    println!("trading-node stopping");
    Arc::try_unwrap(cluster)
        .map_err(|_| "admin server still holds a cluster reference")?
        .shutdown()?;
    Ok(())
}

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let mut args = Args::default();
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "--data-dir" => args.data_dir = next_value(&mut argv, "--data-dir")?.into(),
            "--shards" => args.shards = next_value(&mut argv, "--shards")?.parse()?,
            "--replicas" => args.replicas = next_value(&mut argv, "--replicas")?.parse()?,
            "--snapshot-threshold" => {
                args.snapshot_threshold = next_value(&mut argv, "--snapshot-threshold")?.parse()?
            }
            "--unsafe-buffered" => args.sync_policy = SyncPolicy::UnsafeBuffered,
            "--durable" => args.sync_policy = SyncPolicy::Durable,
            "--admin-addr" => args.admin_addr = next_value(&mut argv, "--admin-addr")?,
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other}; use --help").into()),
        }
    }
    if args.shards == 0 {
        return Err("--shards must be at least 1".into());
    }
    if args.replicas < 3 {
        return Err("--replicas must be at least 3 to keep a quorum".into());
    }
    Ok(args)
}

fn next_value(
    argv: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<String, Box<dyn Error>> {
    argv.next()
        .ok_or_else(|| format!("{flag} requires a value").into())
}

fn print_help() {
    println!("trading-node: replicated trading node with a local admin API");
    println!("usage: trading-node [--data-dir DIR] [--shards N] [--replicas N]");
    println!("                     [--durable|--unsafe-buffered] [--snapshot-threshold N]");
    println!("                     [--admin-addr HOST:PORT]");
    println!();
    println!("  --data-dir DIR          cluster state directory (default data/node)");
    println!("  --shards N              logical shards (default 1)");
    println!("  --replicas N            in-process Raft replicas per shard (default 3)");
    println!("  --snapshot-threshold N  log entries between snapshots (default 1000)");
    println!("  --admin-addr HOST:PORT  admin listener (default 127.0.0.1:8081)");
}
