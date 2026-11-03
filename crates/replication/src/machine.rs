//! Raft state machine: the deterministic trading state plus durable snapshots.

use crate::codec;
use crate::types::{Entry, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership};
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::storage::SnapshotMeta as Meta;
use openraft::{EntryPayload, ErrorSubject, ErrorVerb, OptionalSend};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use trading_protocol::{CommandResult, Status};
use trading_storage::Snapshot as LocalSnapshot;
use trading_storage::SyncPolicy;

#[derive(Clone)]
pub struct ReplicaStateMachine {
    inner: Arc<Mutex<MachineInner>>,
}

pub struct MachineSnapshotBuilder {
    inner: Arc<Mutex<MachineInner>>,
}

struct MachineInner {
    snapshot_path: PathBuf,
    sync_policy: SyncPolicy,
    state: trading_partition::State,
    last_applied: Option<LogId>,
    membership: StoredMembership,
    snapshot_counter: u64,
    current: Option<CurrentSnapshot>,
}

#[derive(Clone)]
struct CurrentSnapshot {
    meta: SnapshotMeta,
    envelope: Vec<u8>,
}

impl ReplicaStateMachine {
    /// Rebuilds a replica's state machine from its local snapshot.
    pub fn open(dir: impl AsRef<Path>, sync_policy: SyncPolicy) -> io::Result<Self> {
        let snapshot_path = dir.as_ref().join("snapshot.bin");
        let mut inner = MachineInner {
            snapshot_path,
            sync_policy,
            state: trading_partition::State::default(),
            last_applied: None,
            membership: StoredMembership::default(),
            snapshot_counter: 0,
            current: None,
        };
        if inner.snapshot_path.exists() {
            let snapshot = LocalSnapshot::read(&inner.snapshot_path)?;
            let (state_bytes, membership) = codec::decode_snapshot_envelope(&snapshot.payload)
                .map_err(|m| io::Error::new(io::ErrorKind::InvalidData, m))?;
            inner.state = trading_partition::State::decode_snapshot(&state_bytes)?;
            inner.membership = StoredMembership::new(
                Some(codec::log_id_of(snapshot.last_term, snapshot.last_sequence)),
                membership,
            );
            inner.last_applied = Some(codec::log_id_of(snapshot.last_term, snapshot.last_sequence));
            inner.current = Some(CurrentSnapshot {
                meta: SnapshotMeta {
                    last_log_id: inner.last_applied,
                    last_membership: inner.membership.clone(),
                    snapshot_id: format!("local-{}", snapshot.last_sequence),
                },
                envelope: snapshot.payload,
            });
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    // -- Read accessors used by tests and tools via Raft::external_request --

    pub fn balance(&self, account: u64) -> Option<u64> {
        let inner = lock(&self.inner);
        inner.state.balance(account)
    }

    pub fn total_balance(&self) -> u128 {
        let inner = lock(&self.inner);
        inner.state.total_balance()
    }

    pub fn account_count(&self) -> usize {
        let inner = lock(&self.inner);
        inner.state.account_count()
    }

    pub fn result_count(&self) -> usize {
        let inner = lock(&self.inner);
        inner.state.result_count()
    }

    pub fn last_applied_index(&self) -> Option<u64> {
        let inner = lock(&self.inner);
        inner.last_applied.map(|log_id| log_id.index)
    }
}

fn lock(inner: &Arc<Mutex<MachineInner>>) -> MutexGuard<'_, MachineInner> {
    inner.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn io_storage_error(subject: ErrorSubject<u64>, verb: ErrorVerb, error: io::Error) -> StorageError {
    StorageError::from_io_error(subject, verb, error)
}

// StorageError's size is fixed by openraft's RaftStateMachine trait.
#[allow(clippy::result_large_err)]
fn apply_one(inner: &mut MachineInner, entry: &Entry) -> Result<CommandResult, StorageError> {
    let expected = inner.last_applied.map(|id| id.index + 1).unwrap_or(0);
    if entry.log_id.index != expected {
        return Err(StorageError::from_io_error(
            ErrorSubject::Apply(entry.log_id),
            ErrorVerb::Write,
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "non-contiguous apply: expected index {expected}, got {}",
                    entry.log_id.index
                ),
            ),
        ));
    }
    let result = match &entry.payload {
        EntryPayload::Blank => synthetic_result(entry.log_id.index),
        EntryPayload::Normal(command) => inner.state.execute(entry.log_id.index, *command),
        EntryPayload::Membership(membership) => {
            inner.membership = StoredMembership::new(Some(entry.log_id), membership.clone());
            synthetic_result(entry.log_id.index)
        }
    };
    inner.last_applied = Some(entry.log_id);
    Ok(result)
}

fn synthetic_result(sequence: u64) -> CommandResult {
    CommandResult {
        request_id: 0,
        sequence,
        status: Status::Success,
    }
}

impl RaftStateMachine<crate::types::TypeConfig> for ReplicaStateMachine {
    type SnapshotBuilder = MachineSnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), StorageError> {
        let inner = lock(&self.inner);
        Ok((inner.last_applied, inner.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CommandResult>, StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry> = entries.into_iter().collect();
        let mut inner = lock(&self.inner);
        let mut results = Vec::with_capacity(entries.len());
        for entry in &entries {
            results.push(apply_one(&mut inner, entry)?);
        }
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MachineSnapshotBuilder {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError> {
        Ok(Box::new(std::io::Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &Meta<u64, openraft::BasicNode>,
        snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let envelope = snapshot.into_inner();
        let last_log_id = meta.last_log_id.ok_or_else(|| {
            io_storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot meta without last_log_id",
                ),
            )
        })?;
        let (state_bytes, membership) =
            codec::decode_snapshot_envelope(&envelope).map_err(|m| {
                io_storage_error(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Read,
                    io::Error::new(io::ErrorKind::InvalidData, m),
                )
            })?;
        let mut inner = lock(&self.inner);
        let state = trading_partition::State::decode_snapshot(&state_bytes)
            .map_err(|e| io_storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, e))?;
        LocalSnapshot::write_atomic(
            &inner.snapshot_path,
            last_log_id.index,
            last_log_id.leader_id.term,
            &envelope,
            inner.sync_policy,
        )
        .map_err(|e| io_storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        inner.state = state;
        inner.membership = StoredMembership::new(Some(last_log_id), membership);
        inner.last_applied = Some(last_log_id);
        inner.current = Some(CurrentSnapshot {
            meta: meta.clone(),
            envelope,
        });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot>, StorageError> {
        let inner = lock(&self.inner);
        Ok(inner.current.as_ref().map(|current| Snapshot {
            meta: current.meta.clone(),
            snapshot: Box::new(std::io::Cursor::new(current.envelope.clone())),
        }))
    }
}

impl RaftSnapshotBuilder<crate::types::TypeConfig> for MachineSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
        let mut inner = lock(&self.inner);
        let last_log_id = inner.last_applied;
        let membership = inner.membership.clone();
        let envelope = codec::encode_snapshot_envelope(
            &inner.state.encode_snapshot(),
            membership.membership(),
        );
        let snapshot_id = match last_log_id {
            Some(log_id) => {
                inner.snapshot_counter += 1;
                format!(
                    "{}-{}-{}",
                    log_id.leader_id.term, log_id.index, inner.snapshot_counter
                )
            }
            None => "empty-0".to_string(),
        };
        if let Some(log_id) = last_log_id {
            LocalSnapshot::write_atomic(
                &inner.snapshot_path,
                log_id.index,
                log_id.leader_id.term,
                &envelope,
                inner.sync_policy,
            )
            .map_err(|e| io_storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        }
        let meta = SnapshotMeta {
            last_log_id,
            last_membership: membership,
            snapshot_id,
        };
        inner.current = Some(CurrentSnapshot {
            meta: meta.clone(),
            envelope,
        });
        let current = inner.current.as_ref().expect("snapshot just stored");
        Ok(Snapshot {
            meta: current.meta.clone(),
            snapshot: Box::new(std::io::Cursor::new(current.envelope.clone())),
        })
    }
}
