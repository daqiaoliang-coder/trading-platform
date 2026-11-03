//! Durable local storage primitives: sequential WAL and atomic snapshots.

mod snapshot;
mod wal;

pub use snapshot::Snapshot;
pub use wal::{LogBoundary, SyncPolicy, Wal, WalRecord};
