//! Online shard migration: plan, audit trail and result types.
//!
//! A migration never changes the logical route mapping (account -> shard);
//! it relocates the physical Raft voter set and publishes a new route epoch.
//! The sequence is owned by [`crate::ReplicatedCluster::migrate_shard`]:
//! create target -> learner catch-up -> frozen position check -> membership
//! change -> new leader -> epoch publication -> old primary retirement.

use crate::shard::{CatchUpReport, ReplicaPosition};
use std::fmt;
use std::time::{Duration, Instant};
use trading_protocol::{RouteVersion, ShardId};

/// What a migration does to the voter set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationMode {
    /// Add a voter and keep every existing voter (3 -> 4).
    ScaleOut,
    /// Replace the current leader with a new target voter.
    RelocateLeader,
    /// Remove a voter without adding one (4 -> 3).
    ScaleIn,
    /// Replace a specific voter with a new target voter.
    Replace { decommission: u64 },
}

/// Fault injection used by drills to prove rollback paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationFault {
    /// Stop the freshly created target before it can join as a learner. The
    /// orchestrator must time out, discard the target and keep the old voters.
    CrashTargetBeforeLearner,
}

#[derive(Clone, Debug)]
pub struct ShardMigrationPlan {
    pub shard: ShardId,
    pub mode: MigrationMode,
    /// Target replica id for `ScaleOut`, `RelocateLeader` and `Replace`.
    pub target_node: Option<u64>,
    /// Voter removed by `ScaleIn` / `Replace`. `RelocateLeader` resolves this
    /// to the current leader at execution time.
    pub decommission_node: Option<u64>,
    /// How long the learner may take to join and catch up.
    pub catchup_timeout: Duration,
    /// Bound for the frozen final catch-up check and the membership change.
    pub switch_timeout: Duration,
    /// Steer the post-change election to the target node.
    pub prefer_target_leader: bool,
    /// Consistency check every 10ms; the learner must be this close before
    /// the frozen window starts.
    pub catchup_lag: u64,
    pub fault: Option<MigrationFault>,
}

impl ShardMigrationPlan {
    pub fn scale_out(shard: ShardId, target_node: u64) -> Self {
        Self::new(shard, MigrationMode::ScaleOut, Some(target_node))
    }

    pub fn relocate_leader(shard: ShardId, target_node: u64) -> Self {
        Self::new(shard, MigrationMode::RelocateLeader, Some(target_node))
    }

    pub fn scale_in(shard: ShardId, decommission: u64) -> Self {
        let mut plan = Self::new(shard, MigrationMode::ScaleIn, None);
        plan.decommission_node = Some(decommission);
        plan
    }

    pub fn replace(shard: ShardId, target_node: u64, decommission: u64) -> Self {
        let mut plan = Self::new(
            shard,
            MigrationMode::Replace { decommission },
            Some(target_node),
        );
        plan.decommission_node = Some(decommission);
        plan
    }

    fn new(shard: ShardId, mode: MigrationMode, target_node: Option<u64>) -> Self {
        Self {
            shard,
            mode,
            target_node,
            decommission_node: None,
            catchup_timeout: Duration::from_secs(10),
            switch_timeout: Duration::from_secs(5),
            prefer_target_leader: true,
            catchup_lag: 8,
            fault: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationStatus {
    Completed,
    /// The change failed before the new voter set became authoritative; the
    /// old group is intact.
    RolledBack,
    /// The voter set changed but a follow-up step (leader discovery or epoch
    /// publication) failed. The new set is authoritative; do not roll back.
    Degraded,
}

impl fmt::Display for MigrationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Completed => write!(f, "completed"),
            Self::RolledBack => write!(f, "rolled-back"),
            Self::Degraded => write!(f, "degraded"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationEventKind {
    Planned,
    ReplicaCreated,
    LearnerAdded,
    CaughtUp,
    FenceStarted,
    MembershipCommitted,
    LeaderElected,
    RoutePublished,
    ReplicaRetired,
    Completed,
    RolledBack,
    Failed,
}

#[derive(Clone, Debug)]
pub struct MigrationEvent {
    pub elapsed_ms: u64,
    pub kind: MigrationEventKind,
    pub detail: String,
}

#[derive(Clone, Debug, Default)]
pub struct MigrationTimings {
    pub catchup_ms: u64,
    pub freeze_ms: u64,
    pub switch_ms: u64,
    pub election_ms: u64,
    pub total_ms: u64,
}

#[derive(Clone, Debug)]
pub struct MigrationReport {
    pub shard: ShardId,
    pub mode: MigrationMode,
    pub target_node: Option<u64>,
    pub retired_nodes: Vec<u64>,
    pub voters_before: Vec<u64>,
    pub voters_after: Vec<u64>,
    pub leader_before: Option<u64>,
    pub leader_after: Option<u64>,
    pub epoch_before: RouteVersion,
    pub epoch_after: RouteVersion,
    pub catchup: Option<CatchUpReport>,
    pub positions_before_promote: Vec<ReplicaPosition>,
    pub timings: MigrationTimings,
    pub status: MigrationStatus,
    pub error: Option<String>,
    pub events: Vec<MigrationEvent>,
    started: Instant,
}

impl MigrationReport {
    pub(crate) fn new(plan: &ShardMigrationPlan, epoch_before: RouteVersion) -> Self {
        Self {
            shard: plan.shard,
            mode: plan.mode.clone(),
            target_node: plan.target_node,
            retired_nodes: Vec::new(),
            voters_before: Vec::new(),
            voters_after: Vec::new(),
            leader_before: None,
            leader_after: None,
            epoch_before,
            epoch_after: epoch_before,
            catchup: None,
            positions_before_promote: Vec::new(),
            timings: MigrationTimings::default(),
            status: MigrationStatus::RolledBack,
            error: None,
            events: Vec::new(),
            started: Instant::now(),
        }
    }

    pub fn is_success(&self) -> bool {
        self.status == MigrationStatus::Completed
    }

    pub(crate) fn event(&mut self, kind: MigrationEventKind, detail: impl Into<String>) {
        self.events.push(MigrationEvent {
            elapsed_ms: self.started.elapsed().as_millis() as u64,
            kind,
            detail: detail.into(),
        });
    }
}
