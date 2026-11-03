//! One replicated shard: an openraft group over the trading WAL, with leader
//! discovery, redirect retries, crash/restart drills and replica summaries.

use crate::log_store::RaftLogStore;
use crate::machine::ReplicaStateMachine;
use crate::network::{serve_inbound, GroupNetworkFactory, GroupTransport};
use crate::types::{Raft, RaftError};
use openraft::error::{ClientWriteError, ForwardToLeader};
use openraft::{BasicNode, Config, SnapshotPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, Semaphore};
use trading_protocol::{Command, CommandResult, ShardId, SubmitError};
use trading_storage::SyncPolicy;

/// Tunables of one replication group. All timeouts are milliseconds, matching
/// openraft's `Config`.
#[derive(Clone, Debug)]
pub struct ReplicatedShardConfig {
    pub replicas: u64,
    pub sync_policy: SyncPolicy,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
    pub heartbeat_interval: u64,
    /// Snapshot after this many log entries since the last snapshot.
    pub snapshot_threshold: u64,
    /// Concurrent client writes allowed per shard (backpressure).
    pub max_in_flight: usize,
    /// Total submit deadline: leader discovery + redirect retries.
    pub write_timeout: Duration,
    /// Per-attempt timeout on one leader before probing another candidate.
    /// Bounds the stall window when the old leader is partitioned from the
    /// quorum and cannot commit anything.
    pub attempt_timeout: Duration,
    /// Capacity of the per-replica inbound RPC channel.
    pub transport_capacity: usize,
}

impl Default for ReplicatedShardConfig {
    fn default() -> Self {
        Self {
            replicas: 3,
            sync_policy: SyncPolicy::Durable,
            // Election window must span several tick slots (tick is
            // heartbeat*1.5 = 75ms); a narrow window risks same-tick
            // split-vote livelock between two candidates.
            election_timeout_min: 300,
            election_timeout_max: 900,
            heartbeat_interval: 50,
            // Snapshots fsync under Durable sync; a bigger threshold amortizes
            // that cost. Catch-up drills override this to force frequent
            // snapshots.
            snapshot_threshold: 1000,
            max_in_flight: 1024,
            write_timeout: Duration::from_secs(8),
            attempt_timeout: Duration::from_secs(1),
            transport_capacity: 1024,
        }
    }
}

#[derive(Clone)]
struct ReplicaHandle {
    raft: Raft,
    machine: ReplicaStateMachine,
}

/// A single shard replicated across N in-process replicas (one Raft group).
pub struct ReplicatedShard {
    shard: ShardId,
    config: ReplicatedShardConfig,
    runtime: Arc<Runtime>,
    transport: Arc<GroupTransport>,
    raft_config: Arc<Config>,
    data_dir: PathBuf,
    replicas: Mutex<BTreeMap<u64, ReplicaHandle>>,
    in_flight: Arc<Semaphore>,
}

/// Read-only view of one replica for verification and failover drills.
#[derive(Clone, Debug)]
pub struct ReplicaSummary {
    pub id: u64,
    pub alive: bool,
    pub leader: bool,
    pub state: String,
    pub term: u64,
    pub last_applied: Option<u64>,
    pub total_balance: Option<u128>,
    pub account_count: Option<usize>,
    pub result_count: Option<usize>,
}

impl ReplicatedShard {
    /// Opens (or recovers) the shard's replication group under
    /// `data_dir/shard-{shard}/replica-{id}/`.
    pub fn open(
        data_dir: impl AsRef<Path>,
        shard: ShardId,
        config: ReplicatedShardConfig,
        runtime: Arc<Runtime>,
    ) -> io::Result<Self> {
        let raft_config = Config {
            cluster_name: format!("trading-shard-{shard}"),
            election_timeout_min: config.election_timeout_min,
            election_timeout_max: config.election_timeout_max,
            heartbeat_interval: config.heartbeat_interval,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_threshold),
            ..Default::default()
        }
        .validate()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{e}")))?;
        let raft_config = Arc::new(raft_config);

        let transport = GroupTransport::new();
        let data_dir = data_dir.as_ref().join(format!("shard-{shard}"));
        std::fs::create_dir_all(&data_dir)?;
        let in_flight = Arc::new(Semaphore::new(config.max_in_flight));

        let shard_ref = Self {
            shard,
            config,
            raft_config,
            runtime,
            transport,
            data_dir,
            replicas: Mutex::new(BTreeMap::new()),
            in_flight,
        };

        // Every configured replica must be openable on open(); restart is the
        // recovery path for crashed replicas.
        for id in 1..=shard_ref.config.replicas {
            shard_ref.start_replica(id)?;
        }
        shard_ref.bootstrap_if_pristine()?;
        shard_ref.wait_for_leader(Duration::from_secs(10))?;
        Ok(shard_ref)
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard
    }

    pub fn config(&self) -> &ReplicatedShardConfig {
        &self.config
    }

    pub fn transport(&self) -> &Arc<GroupTransport> {
        &self.transport
    }

    pub fn live_replicas(&self) -> Vec<u64> {
        self.replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .keys()
            .copied()
            .collect()
    }

    /// Submits a command to the group's current leader, following redirects
    /// until the deadline. Blocks the calling thread via the group runtime.
    pub fn submit(&self, command: Command) -> Result<CommandResult, SubmitError> {
        self.runtime.block_on(self.submit_async(command))
    }

    /// Non-blocking submit: `Overloaded` when the in-flight budget is spent.
    pub fn try_submit(&self, command: Command) -> Result<CommandResult, SubmitError> {
        if Arc::clone(&self.in_flight).try_acquire().is_err() {
            return Err(SubmitError::Overloaded);
        }
        self.runtime
            .block_on(async { self.submit_inner(command).await })
    }

    pub async fn submit_async(&self, command: Command) -> Result<CommandResult, SubmitError> {
        let _permit = Arc::clone(&self.in_flight)
            .acquire_owned()
            .await
            .map_err(|_| SubmitError::Stopped)?;
        self.submit_inner(command).await
    }

    async fn submit_inner(&self, command: Command) -> Result<CommandResult, SubmitError> {
        let deadline = Instant::now() + self.config.write_timeout;
        let mut hint: Option<u64> = None;
        let mut excluded: Option<u64> = None;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(SubmitError::NoLeader);
            }
            let Some(leader) = self.resolve_leader(hint, excluded) else {
                // Election in progress: wait briefly and re-probe.
                hint = None;
                excluded = None;
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
            hint = Some(leader);
            let Some(handle) = self
                .replicas
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .get(&leader)
                .cloned()
            else {
                // Leader crashed between discovery and submit.
                hint = None;
                continue;
            };
            let attempt = deadline
                .saturating_duration_since(now)
                .min(self.config.attempt_timeout);
            let write = tokio::time::timeout(attempt, handle.raft.client_write(command)).await;
            match write {
                Ok(Ok(response)) => return Ok(response.data),
                // Redirect to the hinted replica; forget this candidate.
                Ok(Err(error)) => match redirect_hint(&error) {
                    Some(next) => {
                        if next == hint {
                            hint = None;
                            excluded = Some(leader);
                        } else {
                            hint = next;
                        }
                    }
                    None => {
                        hint = None;
                        excluded = Some(leader);
                    }
                },
                // The leader cannot commit (partitioned or stuck): fence it
                // for this request and probe the rest of the group.
                Err(_elapsed) => {
                    hint = None;
                    excluded = Some(leader);
                }
            }
            if Instant::now() >= deadline {
                return Err(SubmitError::NoLeader);
            }
        }
    }

    /// Crashes a replica: unregisters it from the transport and shuts its Raft
    /// core down, releasing its directory lock. Committed logs stay on disk.
    pub fn crash_replica(&self, id: u64) {
        self.transport.unregister(id);
        let handle = self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&id);
        if let Some(handle) = handle {
            self.runtime.spawn(async move {
                let _ = handle.raft.shutdown().await;
            });
        }
    }

    /// Restarts a previously crashed replica from its on-disk WAL, vote and
    /// snapshot. Retries briefly while the crashed core is still releasing its
    /// directory lock. Refuses replicas that are already running: the fs2
    /// directory lock only fences *other* processes (flock is idempotent
    /// within one process), so the facade must guard in-process double-opens.
    pub fn restart_replica(&self, id: u64) -> io::Result<()> {
        if self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .contains_key(&id)
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "replica is already running in this process",
            ));
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match self.start_replica(id) {
                Ok(()) => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(error);
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Current leader of the group, if any live replica reports one.
    pub fn leader(&self) -> Option<u64> {
        self.resolve_leader(None, None)
    }

    /// Read-only view of every configured replica (alive or crashed).
    pub fn summary(&self) -> Vec<ReplicaSummary> {
        let replicas = self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        (1..=self.config.replicas)
            .map(|id| match replicas.get(&id) {
                Some(handle) => {
                    let metrics = handle.raft.metrics();
                    let metrics = metrics.borrow();
                    ReplicaSummary {
                        id,
                        alive: true,
                        leader: metrics.current_leader == Some(id)
                            && metrics.state == openraft::ServerState::Leader,
                        state: format!("{:?}", metrics.state),
                        term: metrics.current_term,
                        last_applied: metrics.last_applied.map(|log_id| log_id.index),
                        total_balance: Some(handle.machine.total_balance()),
                        account_count: Some(handle.machine.account_count()),
                        result_count: Some(handle.machine.result_count()),
                    }
                }
                None => ReplicaSummary {
                    id,
                    alive: false,
                    leader: false,
                    state: "Down".to_string(),
                    term: 0,
                    last_applied: None,
                    total_balance: None,
                    account_count: None,
                    result_count: None,
                },
            })
            .collect()
    }

    fn resolve_leader(&self, hint: Option<u64>, excluded: Option<u64>) -> Option<u64> {
        let replicas = self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(id) = hint {
            if excluded != Some(id) && replicas.contains_key(&id) {
                return Some(id);
            }
        }
        // A replica is a healthy leader candidate when it reports itself as
        // leader AND a quorum has acknowledged it recently. A partitioned
        // old leader fails the lease check and is skipped.
        let quorum_lease_ms = self.config.election_timeout_max.saturating_mul(4);
        let candidate = replicas.values().find_map(|handle| {
            let metrics = handle.raft.metrics();
            let metrics = metrics.borrow();
            if metrics.state != openraft::ServerState::Leader || Some(metrics.id) == excluded {
                return None;
            }
            let healthy = metrics
                .millis_since_quorum_ack
                .map(|ms| ms <= quorum_lease_ms)
                .unwrap_or(true);
            healthy.then_some(metrics.current_leader)
        });
        if let Some(leader) = candidate {
            return leader;
        }
        // Otherwise trust any live replica's view of the group.
        replicas.values().find_map(|handle| {
            let metrics = handle.raft.metrics();
            let metrics = metrics.borrow();
            metrics
                .current_leader
                .filter(|id| excluded != Some(*id) && replicas.contains_key(id))
        })
    }

    fn start_replica(&self, id: u64) -> io::Result<()> {
        let dir = self.replica_dir(id);
        self.runtime.block_on(async move {
            let log_store = RaftLogStore::open(&dir, self.shard, id, self.config.sync_policy)?;
            let machine = ReplicaStateMachine::open(&dir, self.config.sync_policy)?;
            let network = GroupNetworkFactory::new(Arc::clone(&self.transport));
            let raft = Raft::new(
                id,
                Arc::clone(&self.raft_config),
                network,
                log_store,
                machine.clone(),
            )
            .await
            .map_err(|e| io::Error::other(format!("raft open failed: {e}")))?;
            let (inbound_tx, inbound_rx) = mpsc::channel(self.config.transport_capacity);
            self.transport.register(id, inbound_tx);
            self.runtime.spawn(serve_inbound(raft.clone(), inbound_rx));
            self.replicas
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .insert(id, ReplicaHandle { raft, machine });
            Ok(())
        })
    }

    fn bootstrap_if_pristine(&self) -> io::Result<()> {
        // Clone the Raft cores out of the map first: a std MutexGuard is not
        // async-aware and must not be held across an await point.
        let cores: Vec<Raft> = self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .values()
            .map(|handle| handle.raft.clone())
            .collect();
        let pristine = self.runtime.block_on(async {
            for raft in &cores {
                if raft.is_initialized().await.map_err(fatal_io)? {
                    return Ok::<_, io::Error>(false);
                }
            }
            Ok(true)
        })?;
        if !pristine {
            return Ok(());
        }
        let members: BTreeMap<u64, BasicNode> = (1..=self.config.replicas)
            .map(|id| {
                (
                    id,
                    BasicNode {
                        addr: format!("shard-{}-replica-{}", self.shard, id),
                    },
                )
            })
            .collect();
        self.runtime.block_on(async {
            if let Some(raft) = cores.first() {
                raft.initialize(members)
                    .await
                    .map_err(|e| io::Error::other(format!("raft initialize failed: {e}")))?;
            }
            Ok(())
        })
    }

    fn wait_for_leader(&self, timeout: Duration) -> io::Result<()> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.resolve_leader(None, None).is_some() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "shard {} elected no leader within {:?}",
                self.shard, timeout
            ),
        ))
    }

    fn replica_dir(&self, id: u64) -> PathBuf {
        self.data_dir.join(format!("replica-{id}"))
    }
}

fn redirect_hint(error: &RaftError<ClientWriteError<u64, BasicNode>>) -> Option<Option<u64>> {
    match error {
        RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
            leader_id,
            ..
        })) => Some(*leader_id),
        _ => None,
    }
}

fn fatal_io<E: std::fmt::Display>(error: E) -> io::Error {
    io::Error::other(format!("raft storage error: {error}"))
}

/// Marker type re-exported for callers that need the configured ids.
pub type ReplicaIds = BTreeSet<u64>;
