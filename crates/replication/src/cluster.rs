//! The M3 cluster facade: route-table based sharding where every logical shard
//! is an independent Raft replication group.

use crate::shard::{ReplicaSummary, ReplicatedShard, ReplicatedShardConfig};
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::runtime::Runtime;
use trading_partition::RouteTable;
use trading_protocol::{Command, CommandResult, Route, RouteError, ShardId, SubmitError};

#[derive(Clone, Debug)]
pub struct ReplicatedClusterConfig {
    pub data_dir: PathBuf,
    pub shard: ReplicatedShardConfig,
}

impl ReplicatedClusterConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            shard: ReplicatedShardConfig::default(),
        }
    }
}

/// Every logical shard is one replicated (Raft) shard with N in-process
/// replicas. Submits follow redirects and retry idempotently until the write
/// deadline, so a leader failover is transparent to well-behaved clients.
pub struct ReplicatedCluster {
    routes: RwLock<RouteTable>,
    runtime: Arc<Runtime>,
    shards: BTreeMap<ShardId, Arc<ReplicatedShard>>,
}

impl ReplicatedCluster {
    pub fn open(config: ReplicatedClusterConfig, routes: RouteTable) -> io::Result<Self> {
        let data_dir_existed = config.data_dir.exists();
        std::fs::create_dir_all(&config.data_dir)?;
        if !data_dir_existed {
            sync_parent(&config.data_dir)?;
        }
        let runtime = Arc::new(Runtime::new()?);
        let mut shards = BTreeMap::new();
        for shard_id in routes.shard_ids() {
            let shard = ReplicatedShard::open(
                &config.data_dir,
                shard_id,
                config.shard.clone(),
                Arc::clone(&runtime),
            )?;
            shards.insert(shard_id, Arc::new(shard));
        }
        Ok(Self {
            routes: RwLock::new(routes),
            runtime,
            shards,
        })
    }

    pub fn route_table(&self) -> RouteTable {
        self.routes
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn route(&self, command: &Command) -> Result<Route, RouteError> {
        self.routes
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .route(command)
    }

    /// Publishes a newer table. Logical shard membership cannot change here;
    /// replication-level membership changes are M5 scope.
    pub fn update_route_table(&self, routes: RouteTable) -> Result<(), RouteError> {
        let mut current = self
            .routes
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if routes.version() <= current.version() {
            return Err(RouteError::StaleVersion {
                current_version: current.version(),
            });
        }
        if let Some(shard_id) = routes
            .shard_ids()
            .find(|shard_id| !self.shards.contains_key(shard_id))
        {
            return Err(RouteError::UnknownShard(shard_id));
        }
        if routes.slots() != current.slots() {
            return Err(RouteError::MappingChanged);
        }
        *current = routes;
        Ok(())
    }

    pub fn submit(&self, command: Command) -> Result<CommandResult, SubmitError> {
        let route = self.route(&command).map_err(SubmitError::Route)?;
        self.shard(route.shard_id)?.submit(command)
    }

    pub fn try_submit(&self, command: Command) -> Result<CommandResult, SubmitError> {
        let route = self.route(&command).map_err(SubmitError::Route)?;
        self.shard(route.shard_id)?.try_submit(command)
    }

    /// Crash drills target one shard's replica.
    pub fn shard(&self, shard_id: ShardId) -> Result<&Arc<ReplicatedShard>, SubmitError> {
        self.shards
            .get(&shard_id)
            .ok_or(SubmitError::Route(RouteError::UnknownShard(shard_id)))
    }

    /// Per-shard, per-replica view for reconciliation and drills.
    pub fn summary(&self) -> BTreeMap<ShardId, Vec<ReplicaSummary>> {
        self.shards
            .iter()
            .map(|(shard_id, shard)| (*shard_id, shard.summary()))
            .collect()
    }

    /// Shuts every replication group down, then stops the shared runtime.
    pub fn shutdown(self) -> io::Result<()> {
        let runtime = Arc::try_unwrap(self.runtime)
            .map_err(|_| io::Error::other("cluster handles still alive"))?;
        // Drop the shards (each Raft core sees its transport unregistered),
        // then drop the runtime so all remaining tasks stop.
        drop(self.shards);
        drop(runtime);
        Ok(())
    }
}

fn sync_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}
