//! openraft type wiring: application data is a single trading command,
//! responses are deterministic command results.

use trading_protocol::{Command, CommandResult};

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = CommandResult,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<Self>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type Snapshot = openraft::storage::Snapshot<TypeConfig>;
pub type SnapshotMeta = openraft::storage::SnapshotMeta<u64, openraft::BasicNode>;
pub type LogId = openraft::LogId<u64>;
pub type Vote = openraft::Vote<u64>;
pub type Membership = openraft::Membership<u64, openraft::BasicNode>;
pub type StoredMembership = openraft::StoredMembership<u64, openraft::BasicNode>;
pub type LogState = openraft::storage::LogState<TypeConfig>;
pub type Entry = openraft::Entry<TypeConfig>;
pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<u64>;
pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<u64>;
pub type VoteRequest = openraft::raft::VoteRequest<u64>;
pub type VoteResponse = openraft::raft::VoteResponse<u64>;
pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<u64, E>;
pub type StorageError = openraft::StorageError<u64>;
