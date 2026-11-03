//! M3/M4: shard-level Raft replication with leader fencing, automatic failover
//! and online voter-set migration.
//!
//! The trading WAL doubles as the raft log (one command per entry, raft index
//! == command sequence), so a replicated shard inherits the exact durability
//! and crash-recovery semantics of the single-writer engine.

pub mod cluster;
pub mod codec;
pub mod log_store;
pub mod machine;
pub mod migration;
pub mod network;
pub mod shard;
pub mod types;
pub mod vote_store;

pub use cluster::{ReplicatedCluster, ReplicatedClusterConfig, ShardPlacement};
pub use log_store::{RaftLogStore, WalReader};
pub use machine::{MachineSnapshotBuilder, ReplicaStateMachine};
pub use migration::{
    MigrationEvent, MigrationEventKind, MigrationFault, MigrationMode, MigrationReport,
    MigrationStatus, MigrationTimings, ShardMigrationPlan,
};
pub use network::{serve_inbound, GroupNetworkFactory, GroupTransport, Inbound};
pub use shard::{
    CatchUpReport, ReplicaPosition, ReplicaSummary, ReplicatedShard, ReplicatedShardConfig,
};
pub use types::{Raft, TypeConfig};
