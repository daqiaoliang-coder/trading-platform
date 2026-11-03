use crate::{
    Engine, EngineConfig, EngineHandle, EngineMetricsSnapshot, Partition, PartitionConfig,
};
use crate::{RouteTable, SyncPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::sync::RwLock;
use trading_protocol::{
    Command, CommandResult, CommittedEvent, Route, RouteError, ShardId, SubmitError,
};

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub data_dir: PathBuf,
    pub sync_policy: SyncPolicy,
    pub engine: EngineConfig,
}

impl ClusterConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            sync_policy: SyncPolicy::Durable,
            engine: EngineConfig::default(),
        }
    }
}

/// Owns one independent writer, queue, WAL, and snapshot per logical shard.
pub struct ShardedCluster {
    routes: RwLock<RouteTable>,
    engines: BTreeMap<ShardId, EngineHandle>,
}

impl ShardedCluster {
    pub fn open(config: ClusterConfig, routes: RouteTable) -> io::Result<Self> {
        Self::open_inner(config, routes, None)
    }

    /// Emits committed events in sequence order on each shard's writer thread.
    pub fn open_with_event_sender(
        config: ClusterConfig,
        routes: RouteTable,
        event_sender: Sender<CommittedEvent>,
    ) -> io::Result<Self> {
        Self::open_inner(config, routes, Some(event_sender))
    }

    fn open_inner(
        config: ClusterConfig,
        routes: RouteTable,
        event_sender: Option<Sender<CommittedEvent>>,
    ) -> io::Result<Self> {
        let data_dir_existed = config.data_dir.exists();
        std::fs::create_dir_all(&config.data_dir)?;
        if !data_dir_existed {
            sync_parent(&config.data_dir)?;
        }
        let shard_ids: BTreeSet<_> = routes.shard_ids().collect();
        let mut partitions = BTreeMap::new();
        for shard_id in shard_ids {
            let partition = Partition::open(PartitionConfig {
                data_dir: config.data_dir.join(format!("shard-{shard_id}")),
                sync_policy: config.sync_policy,
            })?;
            partitions.insert(shard_id, partition);
        }
        let engines = partitions
            .into_iter()
            .map(|(shard_id, partition)| {
                let mut engine = Engine::new(partition, config.engine.clone());
                if let Some(sender) = &event_sender {
                    engine = engine.with_event_sender(shard_id, sender.clone());
                }
                let engine = engine.spawn_named(format!("shard-{shard_id}-writer"));
                (shard_id, engine)
            })
            .collect();
        Ok(Self {
            routes: RwLock::new(routes),
            engines,
        })
    }

    pub fn route_table(&self) -> RouteTable {
        self.routes
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    pub fn route_account(&self, account: u64) -> Route {
        self.routes
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .route_account(account)
    }

    pub fn route(&self, command: &Command) -> Result<Route, RouteError> {
        self.routes
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .route(command)
    }

    /// Publishes a newer table without taking any partition writer lock.
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
            .find(|shard_id| !self.engines.contains_key(shard_id))
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
        self.engine(route.shard_id)?
            .submit_at_version(route.version, command)
    }

    pub fn try_submit(&self, command: Command) -> Result<CommandResult, SubmitError> {
        let route = self.route(&command).map_err(SubmitError::Route)?;
        self.engine(route.shard_id)?
            .try_submit_at_version(route.version, command)
    }

    /// Submits through a previously returned route, or returns the current redirect.
    pub fn submit_with_route(
        &self,
        route: Route,
        command: Command,
    ) -> Result<CommandResult, SubmitError> {
        let current = self.resolve(route, &command)?;
        self.engine(current.shard_id)?
            .submit_at_version(current.version, command)
    }

    pub fn try_submit_with_route(
        &self,
        route: Route,
        command: Command,
    ) -> Result<CommandResult, SubmitError> {
        let current = self.resolve(route, &command)?;
        self.engine(current.shard_id)?
            .try_submit_at_version(current.version, command)
    }

    pub fn metrics(&self) -> BTreeMap<ShardId, EngineMetricsSnapshot> {
        self.engines
            .iter()
            .map(|(shard_id, engine)| (*shard_id, engine.metrics()))
            .collect()
    }

    /// Stops each writer independently and returns every recovered partition.
    pub fn shutdown(self) -> io::Result<BTreeMap<ShardId, Partition>> {
        let mut partitions = BTreeMap::new();
        let mut first_error = None;
        for (shard_id, engine) in self.engines {
            match engine.shutdown() {
                Ok(partition) => {
                    partitions.insert(shard_id, partition);
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(partitions)
    }

    fn resolve(&self, requested: Route, command: &Command) -> Result<Route, SubmitError> {
        let current = self.route(command).map_err(SubmitError::Route)?;
        if requested != current {
            return Err(SubmitError::Route(RouteError::StaleRoute { current }));
        }
        Ok(current)
    }

    fn engine(&self, shard_id: ShardId) -> Result<&EngineHandle, SubmitError> {
        self.engines
            .get(&shard_id)
            .ok_or(SubmitError::Route(RouteError::UnknownShard(shard_id)))
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
