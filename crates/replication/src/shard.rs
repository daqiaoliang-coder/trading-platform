//! One replicated shard: an openraft group over the trading WAL, with leader
//! discovery, redirect retries, crash/restart drills, dynamic membership and
//! the submit gate used by online shard migration.

use crate::log_store::RaftLogStore;
use crate::machine::ReplicaStateMachine;
use crate::network::{serve_inbound, GroupNetworkFactory, GroupTransport};
use crate::types::{Raft, RaftError};
use openraft::error::{ClientWriteError, ForwardToLeader};
use openraft::{BasicNode, ChangeMembers, Config, SnapshotPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
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

/// A single shard replicated across a dynamically managed set of in-process
/// replicas (one Raft group).
pub struct ReplicatedShard {
    shard: ShardId,
    config: ReplicatedShardConfig,
    runtime: Arc<Runtime>,
    transport: Arc<GroupTransport>,
    raft_config: Arc<Config>,
    data_dir: PathBuf,
    replicas: Mutex<BTreeMap<u64, ReplicaHandle>>,
    in_flight: Arc<Semaphore>,
    /// Read side: ordinary submits. Write side: the migration critical window
    /// (fence + membership change + epoch publication). The write lock drains
    /// in-flight writes before freezing, so migration causes a bounded pause
    /// instead of failed requests.
    gate: RwLock<()>,
}

/// Read-only view of one replica for verification and failover drills.
#[derive(Clone, Debug)]
pub struct ReplicaSummary {
    pub id: u64,
    pub alive: bool,
    pub leader: bool,
    pub voter: bool,
    pub learner: bool,
    pub state: String,
    pub term: u64,
    pub last_applied: Option<u64>,
    pub total_balance: Option<u128>,
    pub account_count: Option<usize>,
    pub result_count: Option<usize>,
}

/// Applied-log positions of one replica. Used by migration to prove that the
/// target replica has caught up before it is promoted.
#[derive(Clone, Debug)]
pub struct ReplicaPosition {
    pub id: u64,
    pub leader: bool,
    pub voter: bool,
    pub last_log_index: Option<u64>,
    pub last_applied: Option<u64>,
}

/// Result of waiting for a target replica to catch up with the leader.
#[derive(Clone, Debug)]
pub struct CatchUpReport {
    pub target: u64,
    pub leader: Option<u64>,
    pub leader_applied: Option<u64>,
    pub target_applied: Option<u64>,
    pub target_last_log: Option<u64>,
    pub lag: Option<u64>,
}

/// Exclusive migration window. While it is alive, submits to this shard block
/// (they do not fail) until the window is released.
pub struct SubmitFreeze<'a> {
    _guard: RwLockWriteGuard<'a, ()>,
}

impl ReplicatedShard {
    /// Opens (or recovers) the shard's replication group under
    /// `data_dir/shard-{shard}/replica-{id}/`.
    ///
    /// Existing replica directories define the membership candidates, so a
    /// group that was migrated before a restart reopens its *current* replica
    /// set (retired replicas are skipped) instead of the original config.
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
            gate: RwLock::new(()),
        };

        let ids = discover_replica_ids(&shard_ref.data_dir);
        let ids = if ids.is_empty() {
            (1..=shard_ref.config.replicas).collect()
        } else {
            ids
        };
        for id in ids {
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
        let _gate = self.read_gate();
        self.runtime.block_on(self.submit_async(command))
    }

    /// Non-blocking submit: `Overloaded` when the in-flight budget is spent.
    pub fn try_submit(&self, command: Command) -> Result<CommandResult, SubmitError> {
        let _gate = self.read_gate();
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

    /// Starts a fresh replica for this shard without adding it to the Raft
    /// membership. The migration orchestrator then adds it as a learner.
    pub fn create_replica(&self, id: u64) -> io::Result<()> {
        if self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .contains_key(&id)
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("replica {id} is already running on shard {}", self.shard),
            ));
        }
        if self.replica_dir(id).exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "replica {id} data already exists on shard {}; refusing to reuse it",
                    self.shard
                ),
            ));
        }
        self.start_replica(id)
    }

    /// Stops a replica and waits until its Raft core released the directory
    /// lock. Committed logs stay on disk.
    pub fn stop_replica(&self, id: u64) -> io::Result<()> {
        self.transport.unregister(id);
        let handle = self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&id);
        if let Some(handle) = handle {
            self.runtime
                .block_on(handle.raft.shutdown())
                .map_err(|e| io::Error::other(format!("raft shutdown failed: {e}")))?;
        }
        Ok(())
    }

    /// Fences and stops a replica while keeping its data for audit. A marker
    /// excludes it from replica discovery on the next open, so the old primary
    /// can never be restarted as part of the group implicitly.
    pub fn retire_replica(&self, id: u64) -> io::Result<()> {
        self.stop_replica(id)?;
        std::fs::write(self.data_dir.join(format!("retired-{id}")), b"retired\n")
    }

    /// Stops and deletes a replica that was only ever a temporary learner.
    pub fn discard_replica(&self, id: u64) -> io::Result<()> {
        self.stop_replica(id)?;
        let dir = self.replica_dir(id);
        if dir.exists() {
            std::fs::remove_dir_all(dir)?;
        }
        let marker = self.data_dir.join(format!("retired-{id}"));
        if marker.exists() {
            std::fs::remove_file(marker)?;
        }
        Ok(())
    }

    /// Adds a running replica as a learner and (optionally) waits until the
    /// leader sees it caught up. Retries across leader changes until the
    /// deadline. A target that never receives replication makes this fail with
    /// `TimedOut`, which lets the orchestrator roll back.
    pub fn add_learner(&self, id: u64, timeout: Duration) -> io::Result<()> {
        let node = BasicNode {
            addr: format!("shard-{}-replica-{}", self.shard, id),
        };
        let deadline = Instant::now() + timeout;
        let mut last_error = String::from("no leader available");
        while Instant::now() < deadline {
            let Some(leader) = self.leader() else {
                std::thread::sleep(Duration::from_millis(20));
                continue;
            };
            let Some(handle) = self.handle(leader) else {
                continue;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            let attempt = remaining.min(Duration::from_secs(1));
            let result = self.runtime.block_on(async {
                tokio::time::timeout(attempt, handle.raft.add_learner(id, node.clone(), true)).await
            });
            match result {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(error)) => last_error = format!("add learner on {leader}: {error}"),
                Err(_) => last_error = format!("add learner on {leader}: timed out"),
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("replica {id} did not join as learner: {last_error}"),
        ))
    }

    /// Waits until the target replica's applied index is within `max_lag` of
    /// the leader. `max_lag = 0` is the final check performed while the submit
    /// gate is frozen, where the leader index is stable.
    pub fn wait_for_catchup(
        &self,
        target: u64,
        timeout: Duration,
        max_lag: u64,
    ) -> io::Result<CatchUpReport> {
        let deadline = Instant::now() + timeout;
        let mut report = CatchUpReport {
            target,
            leader: None,
            leader_applied: None,
            target_applied: None,
            target_last_log: None,
            lag: None,
        };
        while Instant::now() < deadline {
            let Some(leader) = self.leader() else {
                std::thread::sleep(Duration::from_millis(20));
                continue;
            };
            let Some(leader_handle) = self.handle(leader) else {
                continue;
            };
            let Some(target_handle) = self.handle(target) else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("target replica {target} is not running"),
                ));
            };
            let leader_metrics = leader_handle.raft.metrics();
            let leader_metrics = leader_metrics.borrow();
            let target_metrics = target_handle.raft.metrics();
            let target_metrics = target_metrics.borrow();
            let leader_applied = leader_metrics.last_applied.map(|log_id| log_id.index);
            let target_applied = target_metrics.last_applied.map(|log_id| log_id.index);
            let target_last_log = target_metrics.last_log_index;
            let lag = match (leader_applied, target_applied) {
                (Some(leader), Some(target)) => Some(leader.saturating_sub(target)),
                (Some(leader), None) => Some(leader),
                _ => None,
            };
            report = CatchUpReport {
                target,
                leader: Some(leader),
                leader_applied,
                target_applied,
                target_last_log,
                lag,
            };
            if lag.map(|lag| lag <= max_lag).unwrap_or(false) {
                return Ok(report);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "replica {target} lagged behind leader (leader_applied={:?}, target_applied={:?})",
                report.leader_applied, report.target_applied
            ),
        ))
    }

    /// Commits a uniform voter configuration. All new voters must already be
    /// learners (or existing voters). Retries across leader changes and joint
    /// configurations until the deadline.
    pub fn change_membership(&self, voters: &[u64], timeout: Duration) -> io::Result<Vec<u64>> {
        let voters: BTreeSet<u64> = voters.iter().copied().collect();
        let changes = ChangeMembers::ReplaceAllVoters(voters.clone());
        let deadline = Instant::now() + timeout;
        let mut last_error = String::from("no leader available");
        while Instant::now() < deadline {
            let Some(leader) = self.leader() else {
                std::thread::sleep(Duration::from_millis(20));
                continue;
            };
            let Some(handle) = self.handle(leader) else {
                continue;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            let attempt = remaining.min(Duration::from_secs(2));
            let result = self.runtime.block_on(async {
                tokio::time::timeout(
                    attempt,
                    handle.raft.change_membership(changes.clone(), false),
                )
                .await
            });
            match result {
                Ok(Ok(_)) => return Ok(voters.into_iter().collect()),
                Ok(Err(error)) => last_error = format!("change membership via {leader}: {error}"),
                Err(_) => last_error = format!("change membership via {leader}: timed out"),
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("membership change did not commit: {last_error}"),
        ))
    }

    /// Steers the next election to `target` by disabling tick elections on the
    /// other live replicas, then triggering an election on the target. The
    /// election flags are restored before returning, even on failure.
    pub fn prefer_leader(&self, target: u64, timeout: Duration) -> io::Result<Option<u64>> {
        let handles: Vec<(u64, ReplicaHandle)> = self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .map(|(id, handle)| (*id, handle.clone()))
            .collect();
        let Some(target_handle) = handles
            .iter()
            .find(|(id, _)| *id == target)
            .map(|(_, handle)| handle.clone())
        else {
            return Ok(self.leader());
        };
        for (id, handle) in &handles {
            if *id != target {
                handle.raft.runtime_config().elect(false);
            }
        }
        let deadline = Instant::now() + timeout;
        let mut leader = None;
        let mut last_trigger: Option<Instant> = None;
        while Instant::now() < deadline {
            if self.is_leader_of(target, target) {
                leader = Some(target);
                break;
            }
            // Trigger one campaign at a time: re-triggering while the target
            // is still a Candidate starts a higher-term campaign and discards
            // the votes already in flight. Voters only grant a vote after the
            // retired primary's leader lease expires (one election timeout),
            // so wait just past that before campaigning again.
            let retry_after =
                Duration::from_millis(self.config.election_timeout_max.saturating_add(150));
            if last_trigger.is_none_or(|at| at.elapsed() >= retry_after) {
                last_trigger = Some(Instant::now());
                let _ = self.runtime.block_on(async {
                    tokio::time::timeout(
                        Duration::from_millis(500),
                        target_handle.raft.trigger().elect(),
                    )
                    .await
                });
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        for (id, handle) in &handles {
            if *id != target {
                handle.raft.runtime_config().elect(true);
            }
        }
        Ok(leader.or_else(|| self.leader()))
    }

    /// Temporarily disables tick-driven elections on every live replica except
    /// `except`. Migration uses this before retiring the old primary: otherwise
    /// the surviving voters may elect a leader during the retirement window and
    /// the target preference has no election left to win.
    pub fn suppress_elections(&self, except: Option<u64>) {
        let handles: Vec<(u64, ReplicaHandle)> = self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .map(|(id, handle)| (*id, handle.clone()))
            .collect();
        for (id, handle) in handles {
            if Some(id) != except {
                handle.raft.runtime_config().elect(false);
            }
        }
    }

    /// Waits until a leader other than `previous` reports leadership. Used
    /// after the old primary was removed from the voter set.
    pub fn wait_for_leader_excluding(
        &self,
        previous: u64,
        timeout: Duration,
    ) -> io::Result<Option<u64>> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            for id in self.live_replicas() {
                if id == previous {
                    continue;
                }
                if self.is_leader_of(id, id) {
                    return Ok(Some(id));
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(None)
    }

    /// Stops every replica synchronously, releasing their directory locks.
    pub fn shutdown(self) -> io::Result<()> {
        let handles: Vec<(u64, ReplicaHandle)> = {
            let mut replicas = self
                .replicas
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            let handles = replicas
                .iter()
                .map(|(id, handle)| (*id, handle.clone()))
                .collect();
            replicas.clear();
            handles
        };
        for (id, handle) in handles {
            self.transport.unregister(id);
            self.runtime
                .block_on(handle.raft.shutdown())
                .map_err(|e| io::Error::other(format!("raft shutdown failed: {e}")))?;
        }
        Ok(())
    }

    /// Freezes submits to this shard. Waits for in-flight writes to drain, so
    /// a migration never aborts a committed transaction. Keep the guard alive
    /// for the shortest possible critical section.
    pub fn freeze(&self) -> SubmitFreeze<'_> {
        SubmitFreeze {
            _guard: self
                .gate
                .write()
                .unwrap_or_else(|poison| poison.into_inner()),
        }
    }

    /// Current voter and learner sets, preferring the leader's view.
    pub fn membership(&self) -> Option<(Vec<u64>, Vec<u64>)> {
        let handles: Vec<(u64, ReplicaHandle)> = self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .map(|(id, handle)| (*id, handle.clone()))
            .collect();
        let leader = self.leader();
        let ordered: Vec<&(u64, ReplicaHandle)> = handles
            .iter()
            .filter(|(id, _)| Some(*id) == leader)
            .chain(handles.iter().filter(|(id, _)| Some(*id) != leader))
            .collect();
        ordered.into_iter().find_map(|(_, handle)| {
            let metrics = handle.raft.metrics();
            let metrics = metrics.borrow();
            let membership = metrics.membership_config.membership();
            let voters: BTreeSet<u64> = membership.voter_ids().collect();
            if voters.is_empty() {
                return None;
            }
            let learners: BTreeSet<u64> = membership.learner_ids().collect();
            Some((voters.into_iter().collect(), learners.into_iter().collect()))
        })
    }

    /// Applied-log position of every live replica.
    pub fn positions(&self) -> Vec<ReplicaPosition> {
        let handles: Vec<(u64, ReplicaHandle)> = self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .map(|(id, handle)| (*id, handle.clone()))
            .collect();
        let membership = self.membership();
        let (voters, _) = membership.unwrap_or_default();
        handles
            .into_iter()
            .map(|(id, handle)| {
                let metrics = handle.raft.metrics();
                let metrics = metrics.borrow();
                ReplicaPosition {
                    id,
                    leader: metrics.state == openraft::ServerState::Leader,
                    voter: voters.contains(&id),
                    last_log_index: metrics.last_log_index,
                    last_applied: metrics.last_applied.map(|log_id| log_id.index),
                }
            })
            .collect()
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

    /// Read-only view of every live replica (alive or crashed in the map; a
    /// retired replica is removed from the map).
    pub fn summary(&self) -> Vec<ReplicaSummary> {
        // Resolve membership before taking the replica map: `membership()`
        // locks the same map and std::sync::Mutex is not re-entrant.
        let (voters, learners) = self.membership().unwrap_or_default();
        let replicas = self
            .replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        replicas
            .iter()
            .map(|(id, handle)| {
                let metrics = handle.raft.metrics();
                let metrics = metrics.borrow();
                ReplicaSummary {
                    id: *id,
                    alive: true,
                    leader: metrics.current_leader == Some(*id)
                        && metrics.state == openraft::ServerState::Leader,
                    voter: voters.contains(id),
                    learner: learners.contains(id),
                    state: format!("{:?}", metrics.state),
                    term: metrics.current_term,
                    last_applied: metrics.last_applied.map(|log_id| log_id.index),
                    total_balance: Some(handle.machine.total_balance()),
                    account_count: Some(handle.machine.account_count()),
                    result_count: Some(handle.machine.result_count()),
                }
            })
            .collect()
    }

    fn read_gate(&self) -> RwLockReadGuard<'_, ()> {
        self.gate
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn is_leader_of(&self, id: u64, expected: u64) -> bool {
        self.handle(id)
            .map(|handle| {
                let metrics = handle.raft.metrics();
                let metrics = metrics.borrow();
                metrics.state == openraft::ServerState::Leader
                    && metrics.current_leader == Some(expected)
            })
            .unwrap_or(false)
    }

    fn handle(&self, id: u64) -> Option<ReplicaHandle> {
        self.replicas
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&id)
            .cloned()
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
        let (cores, ids): (Vec<Raft>, Vec<u64>) = {
            let replicas = self
                .replicas
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            (
                replicas
                    .values()
                    .map(|handle| handle.raft.clone())
                    .collect(),
                replicas.keys().copied().collect(),
            )
        };
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
        let members: BTreeMap<u64, BasicNode> = ids
            .into_iter()
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

/// Existing replica directories are the recovery set. `retired-*` markers
/// fence replicas that were removed by a migration: they are skipped even
/// though their data is retained for audit.
fn discover_replica_ids(shard_dir: &Path) -> Vec<u64> {
    let mut ids = BTreeSet::new();
    let Ok(entries) = std::fs::read_dir(shard_dir) else {
        return Vec::new();
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(rest) = name.strip_prefix("replica-") else {
            continue;
        };
        let Ok(id) = rest.parse::<u64>() else {
            continue;
        };
        if !shard_dir.join(format!("retired-{id}")).exists() {
            ids.insert(id);
        }
    }
    ids.into_iter().collect()
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
