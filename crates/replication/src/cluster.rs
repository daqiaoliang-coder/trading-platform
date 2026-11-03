//! The M3 cluster facade, extended in M4 with online shard migration: every
//! logical shard is an independent Raft replication group whose voter set can
//! be relocated while the logical account->shard mapping stays unchanged.

use crate::migration::{
    MigrationEventKind, MigrationFault, MigrationMode, MigrationReport, MigrationStatus,
    ShardMigrationPlan,
};
use crate::shard::{ReplicaSummary, ReplicatedShard, ReplicatedShardConfig};
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use trading_partition::RouteTable;
use trading_protocol::{
    Command, CommandResult, Route, RouteError, RouteVersion, ShardId, SubmitError,
};

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

/// Physical placement of one logical shard at the current route epoch.
#[derive(Clone, Debug)]
pub struct ShardPlacement {
    pub shard: ShardId,
    pub epoch: RouteVersion,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub leader: Option<u64>,
    pub live_replicas: Vec<u64>,
}

/// Every logical shard is one replicated (Raft) shard with N in-process
/// replicas. Submits follow redirects and retry idempotently until the write
/// deadline, so a leader failover is transparent to well-behaved clients.
pub struct ReplicatedCluster {
    data_dir: PathBuf,
    routes: RwLock<RouteTable>,
    runtime: Arc<Runtime>,
    shards: BTreeMap<ShardId, Arc<ReplicatedShard>>,
    /// Online migrations are serialized: one frozen window and one route
    /// epoch publication at a time. Other shards keep serving throughout.
    migrations: Mutex<()>,
}

impl ReplicatedCluster {
    pub fn open(config: ReplicatedClusterConfig, routes: RouteTable) -> io::Result<Self> {
        let data_dir_existed = config.data_dir.exists();
        std::fs::create_dir_all(&config.data_dir)?;
        if !data_dir_existed {
            sync_parent(&config.data_dir)?;
        }
        // Route epochs are monotonic across restarts: a stale cached route
        // must never look newer than the epoch published by a migration.
        let persisted = read_route_version(&config.data_dir);
        let routes = match persisted {
            Some(version) if version > routes.version() => {
                RouteTable::new(version, routes.slots().to_vec())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?
            }
            _ => routes,
        };
        if persisted != Some(routes.version()) {
            write_route_version(&config.data_dir, routes.version())?;
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
            data_dir: config.data_dir,
            routes: RwLock::new(routes),
            runtime,
            shards,
            migrations: Mutex::new(()),
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
    /// physical voter changes go through [`Self::migrate_shard`].
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
        let version = routes.version();
        *current = routes;
        drop(current);
        let _ = write_route_version(&self.data_dir, version);
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

    /// Submits against a cached route epoch. A migration that published a
    /// newer epoch returns `StaleRoute`, so callers refresh and retry the same
    /// idempotent request.
    pub fn submit_with_route(
        &self,
        route: Route,
        command: Command,
    ) -> Result<CommandResult, SubmitError> {
        let current = self.route(&command).map_err(SubmitError::Route)?;
        if current != route {
            return Err(SubmitError::Route(RouteError::StaleRoute { current }));
        }
        self.shard(current.shard_id)?.submit(command)
    }

    pub fn try_submit_with_route(
        &self,
        route: Route,
        command: Command,
    ) -> Result<CommandResult, SubmitError> {
        let current = self.route(&command).map_err(SubmitError::Route)?;
        if current != route {
            return Err(SubmitError::Route(RouteError::StaleRoute { current }));
        }
        self.shard(current.shard_id)?.try_submit(command)
    }

    /// Crash drills target one shard's replica.
    pub fn shard(&self, shard_id: ShardId) -> Result<&Arc<ReplicatedShard>, SubmitError> {
        self.shards
            .get(&shard_id)
            .ok_or(SubmitError::Route(RouteError::UnknownShard(shard_id)))
    }

    /// Current physical placement of one shard.
    pub fn placement(&self, shard_id: ShardId) -> Option<ShardPlacement> {
        let shard = self.shards.get(&shard_id)?;
        let (voters, learners) = shard.membership().unwrap_or_default();
        Some(ShardPlacement {
            shard: shard_id,
            epoch: self.route_table().version(),
            voters,
            learners,
            leader: shard.leader(),
            live_replicas: shard.live_replicas(),
        })
    }

    pub fn placements(&self) -> BTreeMap<ShardId, ShardPlacement> {
        self.shards
            .keys()
            .filter_map(|shard_id| self.placement(*shard_id).map(|p| (*shard_id, p)))
            .collect()
    }

    /// Per-shard, per-replica view for reconciliation and drills.
    pub fn summary(&self) -> BTreeMap<ShardId, Vec<ReplicaSummary>> {
        self.shards
            .iter()
            .map(|(shard_id, shard)| (*shard_id, shard.summary()))
            .collect()
    }

    /// Runs one online migration to completion. The logical route mapping is
    /// immutable; the physical voter set changes and a new route epoch is
    /// published atomically.
    ///
    /// The critical window (final catch-up check + membership commit) blocks
    /// submits to *this shard only*. If anything fails before the new voter
    /// set becomes authoritative, the target is discarded and the old group
    /// continues serving without an epoch bump.
    pub fn migrate_shard(&self, plan: &ShardMigrationPlan) -> MigrationReport {
        let started = Instant::now();
        let epoch_before = self.route_table().version();
        let mut report = MigrationReport::new(plan, epoch_before);
        report.event(
            MigrationEventKind::Planned,
            format!(
                "mode={:?} target={:?} decommission={:?}",
                plan.mode, plan.target_node, plan.decommission_node
            ),
        );
        let _serial = self
            .migrations
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());

        let Some(shard) = self.shards.get(&plan.shard).cloned() else {
            return finish_failed(
                report,
                started,
                MigrationStatus::RolledBack,
                format!("unknown shard {}", plan.shard),
            );
        };
        let Some((voters_before, _learners)) = shard.membership() else {
            return finish_failed(
                report,
                started,
                MigrationStatus::RolledBack,
                "shard membership is unavailable (no live replica)".to_string(),
            );
        };
        report.voters_before = voters_before.clone();
        // Until the membership change commits, the old voters remain the
        // authoritative set; reports for early failures must reflect that.
        report.voters_after = voters_before.clone();
        let Some(leader_before) = shard.leader() else {
            return finish_failed(
                report,
                started,
                MigrationStatus::RolledBack,
                "shard has no leader".to_string(),
            );
        };
        report.leader_before = Some(leader_before);

        let target = plan.target_node;
        let removed = match plan.mode {
            MigrationMode::ScaleOut => None,
            MigrationMode::RelocateLeader => Some(leader_before),
            MigrationMode::ScaleIn => plan.decommission_node,
            MigrationMode::Replace { decommission } => Some(decommission),
        };
        if target.is_none() && removed.is_none() {
            return finish_failed(
                report,
                started,
                MigrationStatus::RolledBack,
                "plan neither adds nor removes a voter".to_string(),
            );
        }
        if let Some(target) = target {
            if voters_before.contains(&target) {
                return finish_failed(
                    report,
                    started,
                    MigrationStatus::RolledBack,
                    format!("target {target} is already a voter"),
                );
            }
            if shard.live_replicas().contains(&target) {
                return finish_failed(
                    report,
                    started,
                    MigrationStatus::RolledBack,
                    format!("target {target} is already running"),
                );
            }
        }
        if let Some(removed) = removed {
            if !voters_before.contains(&removed) {
                return finish_failed(
                    report,
                    started,
                    MigrationStatus::RolledBack,
                    format!("decommission node {removed} is not a voter"),
                );
            }
            if Some(removed) == target {
                return finish_failed(
                    report,
                    started,
                    MigrationStatus::RolledBack,
                    "target and decommission node are the same".to_string(),
                );
            }
            if voters_before.len() <= 1 {
                return finish_failed(
                    report,
                    started,
                    MigrationStatus::RolledBack,
                    "removing the last voter would destroy the group".to_string(),
                );
            }
        }
        let mut new_voters: Vec<u64> = voters_before
            .iter()
            .copied()
            .filter(|id| Some(*id) != removed)
            .collect();
        if let Some(target) = target {
            new_voters.push(target);
        }
        new_voters.sort_unstable();
        new_voters.dedup();
        if new_voters.is_empty() {
            return finish_failed(
                report,
                started,
                MigrationStatus::RolledBack,
                "new voter set would be empty".to_string(),
            );
        }
        // Only steer the election when the current leader is the node being
        // replaced; otherwise the existing healthy leader keeps serving.
        let target_leader = if plan.prefer_target_leader && removed == Some(leader_before) {
            target
        } else {
            None
        };

        // ---- 1. Create the target replica and add it as a learner ----------
        if let Some(target) = target {
            if let Err(error) = shard.create_replica(target) {
                return finish_failed(
                    report,
                    started,
                    MigrationStatus::RolledBack,
                    format!("create target replica {target}: {error}"),
                );
            }
            report.event(
                MigrationEventKind::ReplicaCreated,
                format!("target replica {target} created"),
            );
            if plan.fault == Some(MigrationFault::CrashTargetBeforeLearner) {
                let _ = shard.stop_replica(target);
            }
            if let Err(error) = shard.add_learner(target, plan.catchup_timeout) {
                let _ = shard.discard_replica(target);
                return finish_failed(
                    report,
                    started,
                    MigrationStatus::RolledBack,
                    format!("target {target} failed to join as learner: {error}"),
                );
            }
            report.event(
                MigrationEventKind::LearnerAdded,
                format!("target {} joined as learner", target),
            );
            let catchup_started = Instant::now();
            match shard.wait_for_catchup(target, plan.catchup_timeout, plan.catchup_lag) {
                Ok(catchup) => {
                    report.catchup = Some(catchup.clone());
                    report.timings.catchup_ms = elapsed_ms(catchup_started);
                    report.event(
                        MigrationEventKind::CaughtUp,
                        format!(
                            "target applied {:?} vs leader {:?} (lag {:?})",
                            catchup.target_applied, catchup.leader_applied, catchup.lag
                        ),
                    );
                }
                Err(error) => {
                    let _ = shard.discard_replica(target);
                    return finish_failed(
                        report,
                        started,
                        MigrationStatus::RolledBack,
                        format!("target {target} failed to catch up: {error}"),
                    );
                }
            }
        }

        // ---- 2. Frozen critical window: exact position + membership commit --
        let freeze_started = Instant::now();
        let freeze = shard.freeze();
        report.event(
            MigrationEventKind::FenceStarted,
            format!("submit gate frozen for shard {}", plan.shard),
        );
        if let Some(target) = target {
            match shard.wait_for_catchup(target, plan.switch_timeout, 0) {
                Ok(catchup) => {
                    report.catchup = Some(catchup);
                    report.positions_before_promote = shard.positions();
                }
                Err(error) => {
                    drop(freeze);
                    report.timings.freeze_ms = elapsed_ms(freeze_started);
                    let _ = shard.discard_replica(target);
                    return finish_failed(
                        report,
                        started,
                        MigrationStatus::RolledBack,
                        format!("target {target} failed the frozen position check: {error}"),
                    );
                }
            }
        } else {
            report.positions_before_promote = shard.positions();
        }
        let switch_started = Instant::now();
        let switch = shard.change_membership(&new_voters, plan.switch_timeout);
        report.timings.switch_ms = elapsed_ms(switch_started);
        let membership_committed;

        if let Err(error) = switch {
            // If the old voter set is still authoritative (the failure happened
            // before the joint config committed), discard the target and keep
            // serving on the old group without a new epoch.
            let actual = shard.membership().map(|m| m.0).unwrap_or_default();
            if actual == voters_before {
                if let Some(target) = target {
                    let _ = shard.discard_replica(target);
                }
                report.voters_after = actual;
                return finish_failed(
                    report,
                    started,
                    MigrationStatus::RolledBack,
                    format!("membership change did not commit: {error}"),
                );
            }
            // The new set may already be authoritative; rolling back a
            // committed config is unsafe. Publish the epoch and mark degraded.
            report.status = MigrationStatus::Degraded;
            report.error = Some(format!(
                "membership change reported an error but the voter set changed: {error}; actual={actual:?}"
            ));
            report.event(
                MigrationEventKind::Failed,
                "membership change uncertain; continuing with the committed configuration",
            );
            membership_committed = true;
        } else {
            membership_committed = true;
            report.event(
                MigrationEventKind::MembershipCommitted,
                format!("voters replaced by {new_voters:?}"),
            );
        }

        if membership_committed {
            // Stop the surviving voters from electing a leader while the
            // orchestrator is still syncing and retiring the old primary.
            // Otherwise a normal election wins during that window and the
            // target can no longer be preferred.
            if target_leader.is_some() {
                shard.suppress_elections(target_leader);
            }
        }

        // While the old primary is still alive, make sure the target has the
        // membership entries themselves. Otherwise the target's log is behind
        // the surviving voters by one entry and they reject its vote, which
        // stretches the post-fence election window into seconds.
        if membership_committed && removed == Some(leader_before) {
            if let Some(target) = target {
                match shard.wait_for_catchup(target, Duration::from_millis(500), 0) {
                    Ok(catchup) => {
                        // Keep `report.catchup` as the frozen pre-promotion
                        // check (the one paired with `positions_before_promote`);
                        // audit the post-commit sync separately.
                        report.event(
                            MigrationEventKind::CaughtUp,
                            format!(
                                "target synced after membership commit: applied {:?} vs leader {:?}",
                                catchup.target_applied, catchup.leader_applied
                            ),
                        );
                    }
                    Err(error) => report.event(
                        MigrationEventKind::Failed,
                        format!("post-membership target sync: {error}"),
                    ),
                }
            }
        }
        if membership_committed {
            // ---- 3. Fence the removed voters before any election -------------
            // Stopping the old primary first is what makes fencing reliable:
            // it cannot campaign, serve stale reads or apply new writes while
            // the survivors elect a new leader.
            if let Some(removed) = removed {
                match shard.retire_replica(removed) {
                    Ok(()) => {
                        report.retired_nodes.push(removed);
                        report.event(
                            MigrationEventKind::ReplicaRetired,
                            format!("old replica {removed} fenced and retired"),
                        );
                    }
                    Err(error) => {
                        report.status = MigrationStatus::Degraded;
                        report.error = Some(format!("retire replica {removed} failed: {error}"));
                        report.event(
                            MigrationEventKind::Failed,
                            format!("retire replica {removed} failed"),
                        );
                    }
                }
            }

            // The old primary is stopped before the gate opens, so no new
            // entry can reach the surviving voters while the target is being
            // promoted. Releasing the gate then only blocks writes for the
            // election itself, not the retirement.
            report.timings.freeze_ms = elapsed_ms(freeze_started);
            drop(freeze);

            // ---- 4. Elect a surviving leader ---------------------------------
            let election_started = Instant::now();
            let mut leader_after = None;
            if let Some(target_leader) = target_leader {
                // The target must outlive the leader lease left by the retired
                // primary (one election timeout) before votes are granted,
                // hence the allowance beyond a single campaign. If it still
                // cannot win (for example a stale log), fall back to any
                // survivor instead of stalling writes for the whole budget.
                let preference = plan.switch_timeout.min(Duration::from_secs(3));
                leader_after = shard
                    .prefer_leader(target_leader, preference)
                    .unwrap_or(None);
            }
            if leader_after.is_none() {
                leader_after = if let Some(removed) = removed {
                    shard
                        .wait_for_leader_excluding(removed, plan.switch_timeout)
                        .unwrap_or(None)
                } else {
                    shard.leader()
                };
            }
            if leader_after == removed {
                leader_after = removed.and_then(|removed| {
                    shard
                        .wait_for_leader_excluding(removed, plan.switch_timeout)
                        .unwrap_or(None)
                });
            }
            report.leader_after = leader_after;
            report.timings.election_ms = elapsed_ms(election_started);
            report.event(
                MigrationEventKind::LeaderElected,
                format!("leader after migration: {leader_after:?}"),
            );
            if leader_after.is_none() {
                report.status = MigrationStatus::Degraded;
                report.error = Some("no surviving leader was elected in time".to_string());
                report.event(
                    MigrationEventKind::Failed,
                    "no surviving leader was elected in time",
                );
            }

            // ---- 5. Publish the new route epoch -----------------------------
            match self.bump_route_version() {
                Ok(epoch) => {
                    report.epoch_after = epoch;
                    report.event(
                        MigrationEventKind::RoutePublished,
                        format!("route epoch published: {} -> {}", epoch_before, epoch),
                    );
                }
                Err(error) => {
                    report.status = MigrationStatus::Degraded;
                    report.error = Some(format!("route epoch publication failed: {error}"));
                    report.event(MigrationEventKind::Failed, "route epoch publication failed");
                }
            }
        }

        report.voters_after = shard.membership().map(|m| m.0).unwrap_or_default();
        if report.status == MigrationStatus::Degraded {
            report.timings.total_ms = elapsed_ms(started);
            return report;
        }
        report.status = MigrationStatus::Completed;
        report.event(
            MigrationEventKind::Completed,
            format!(
                "voters {:?} -> {:?}, epoch {} -> {}, leader {:?}",
                report.voters_before,
                report.voters_after,
                report.epoch_before,
                report.epoch_after,
                report.leader_after
            ),
        );
        report.timings.total_ms = elapsed_ms(started);
        report
    }

    /// Shuts every replication group down, then stops the shared runtime.
    pub fn shutdown(self) -> io::Result<()> {
        for (_, shard) in self.shards {
            match Arc::try_unwrap(shard) {
                Ok(shard) => shard.shutdown()?,
                Err(shard) => {
                    // Other handles still exist; stop the cores we can reach
                    // and let their owner release the last reference.
                    for id in shard.live_replicas() {
                        let _ = shard.stop_replica(id);
                    }
                }
            }
        }
        let runtime = Arc::try_unwrap(self.runtime)
            .map_err(|_| io::Error::other("cluster handles still alive"))?;
        drop(runtime);
        Ok(())
    }

    fn bump_route_version(&self) -> Result<RouteVersion, String> {
        let mut current = self
            .routes
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let next = current
            .version()
            .checked_add(1)
            .ok_or_else(|| "route epoch overflow".to_string())?;
        let table = RouteTable::new(next, current.slots().to_vec())
            .map_err(|error| format!("route table: {error}"))?;
        write_route_version(&self.data_dir, next)
            .map_err(|error| format!("persist route epoch: {error}"))?;
        *current = table;
        Ok(next)
    }
}

fn finish_failed(
    mut report: MigrationReport,
    started: Instant,
    status: MigrationStatus,
    error: String,
) -> MigrationReport {
    report.status = status;
    report.error = Some(error.clone());
    report.event(
        if status == MigrationStatus::RolledBack {
            MigrationEventKind::RolledBack
        } else {
            MigrationEventKind::Failed
        },
        error,
    );
    report.timings.total_ms = elapsed_ms(started);
    report
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis() as u64
}

fn read_route_version(data_dir: &Path) -> Option<RouteVersion> {
    std::fs::read_to_string(data_dir.join("route.version"))
        .ok()
        .and_then(|text| text.trim().parse().ok())
}

fn write_route_version(data_dir: &Path, version: RouteVersion) -> io::Result<()> {
    let path = data_dir.join("route.version");
    let tmp = data_dir.join("route.version.tmp");
    std::fs::write(&tmp, version.to_string().as_bytes())?;
    std::fs::rename(&tmp, &path)?;
    sync_parent(&path)
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
