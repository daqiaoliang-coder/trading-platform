//! Raft log storage: the trading WAL is the raft log, one command per entry.

use crate::codec;
use crate::types::{Entry, LogId, LogState, StorageError, Vote};
use crate::vote_store::VoteStore;
use fs2::FileExt;
use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::{ErrorSubject, ErrorVerb, OptionalSend, RaftLogReader};
use std::collections::Bound;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use trading_protocol::ShardId;
use trading_storage::{LogBoundary, SyncPolicy, Wal, WalRecord};

pub struct RaftLogStore {
    inner: Arc<Mutex<LogInner>>,
    _dir_lock: File,
}

/// A cheap cloneable handle that replication tasks use to stream log entries.
pub struct WalReader {
    inner: Arc<Mutex<LogInner>>,
}

struct LogInner {
    wal: Wal,
    vote: VoteStore,
}

impl RaftLogStore {
    /// Opens (or creates) the raft log in `dir`, taking an exclusive lock on
    /// `partition.lock` so only one process owns a replica directory.
    pub fn open(
        dir: impl AsRef<Path>,
        shard: ShardId,
        replica: u64,
        sync_policy: SyncPolicy,
    ) -> io::Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let lock_path = dir.join("partition.lock");
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)?;
        lock.try_lock_exclusive().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "replica directory {} is already in use (shard {shard} replica {replica})",
                    dir.display()
                ),
            )
        })?;
        let (wal, _) = Wal::open(dir.join("partition.wal"), sync_policy)?;
        let vote = VoteStore::new(dir.join("raft-vote.bin"));
        Ok(Self {
            inner: Arc::new(Mutex::new(LogInner { wal, vote })),
            _dir_lock: lock,
        })
    }
}

fn lock_inner(inner: &Arc<Mutex<LogInner>>) -> MutexGuard<'_, LogInner> {
    inner.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// Shared implementation for both the log store and its readers.
// StorageError's size is fixed by openraft's storage traits; boxing it would
// break the trait signatures this helper feeds into.
#[allow(clippy::result_large_err)]
fn read_entries(log: &mut LogInner, from: u64, to: u64) -> Result<Vec<Entry>, StorageError> {
    let records = log
        .wal
        .read_range(from, to)
        .map_err(|e| StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Read, e))?;
    let mut entries = Vec::with_capacity(records.len());
    for record in records {
        let log_id = codec::log_id_of(record.term, record.sequence);
        let entry = codec::decode_entry(log_id, &record.payload).map_err(|message| {
            StorageError::from_io_error(
                ErrorSubject::Log(log_id),
                ErrorVerb::Read,
                io::Error::new(io::ErrorKind::InvalidData, message),
            )
        })?;
        entries.push(entry);
    }
    Ok(entries)
}

impl WalReader {
    fn from_inner(inner: Arc<Mutex<LogInner>>) -> Self {
        Self { inner }
    }
}

macro_rules! impl_log_reader {
    ($ty:ident) => {
        impl RaftLogReader<crate::types::TypeConfig> for $ty {
            async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
                &mut self,
                range: RB,
            ) -> Result<Vec<Entry>, StorageError> {
                let from = match range.start_bound() {
                    Bound::Included(value) => *value,
                    Bound::Excluded(value) => value.saturating_add(1),
                    Bound::Unbounded => 0,
                };
                let to = match range.end_bound() {
                    Bound::Included(value) => value.saturating_add(1),
                    Bound::Excluded(value) => *value,
                    Bound::Unbounded => u64::MAX,
                };
                let mut inner = lock_inner(&self.inner);
                read_entries(&mut inner, from, to)
            }
        }
    };
}

impl_log_reader!(RaftLogStore);
impl_log_reader!(WalReader);

impl RaftLogStorage<crate::types::TypeConfig> for RaftLogStore {
    type LogReader = WalReader;

    async fn get_log_state(&mut self) -> Result<LogState, StorageError> {
        let mut inner = lock_inner(&self.inner);
        let wal = &mut inner.wal;
        let last_purged_log_id = wal.purged().map(|b| codec::log_id_of(b.term, b.index));
        let last_log_id = match wal.last_sequence() {
            Some(index) => {
                let term = wal.term_of(index).unwrap_or(0);
                Some(codec::log_id_of(term, index))
            }
            None => None,
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        WalReader::from_inner(Arc::clone(&self.inner))
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError> {
        let inner = lock_inner(&self.inner);
        inner
            .vote
            .save(vote)
            .map_err(|e| StorageError::from_io_error(ErrorSubject::Vote, ErrorVerb::Write, e))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        let inner = lock_inner(&self.inner);
        inner
            .vote
            .read()
            .map_err(|e| StorageError::from_io_error(ErrorSubject::Vote, ErrorVerb::Read, e))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<crate::types::TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry> = entries.into_iter().collect();
        let result = append_entries(&mut lock_inner(&self.inner), &entries);
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                let copy = io::Error::new(e.kind(), e.to_string());
                callback.log_io_completed(Err(e));
                Err(StorageError::from_io_error(
                    ErrorSubject::Logs,
                    ErrorVerb::Write,
                    copy,
                ))
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let mut inner = lock_inner(&self.inner);
        inner
            .wal
            .truncate_from(log_id.index)
            .map_err(|e| StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, e))
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let mut inner = lock_inner(&self.inner);
        inner
            .wal
            .purge_through(LogBoundary {
                term: log_id.leader_id.term,
                index: log_id.index,
            })
            .map_err(|e| StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Delete, e))
    }
}

fn append_entries(inner: &mut LogInner, entries: &[Entry]) -> Result<(), io::Error> {
    if entries.is_empty() {
        return Ok(());
    }
    let first = entries[0].log_id.index;
    let last_live = inner.wal.last_sequence();
    match last_live {
        Some(last) => {
            if first <= last {
                // Conflicting suffix: replace it with the leader's entries.
                inner.wal.truncate_from(first)?;
            } else if first != last + 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raft log append would leave a hole",
                ));
            }
        }
        None => {
            // openraft log indexes start at 0 (the initial membership entry).
            let expected = inner
                .wal
                .purged()
                .map(|boundary| boundary.index + 1)
                .unwrap_or(0);
            if first != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raft log append does not start at the purged boundary",
                ));
            }
        }
    }
    let records = entries
        .iter()
        .map(|entry| {
            WalRecord::raft(
                entry.log_id.leader_id.term,
                entry.log_id.index,
                codec::encode_entry(entry),
            )
        })
        .collect::<Vec<_>>();
    inner.wal.append_batch(&records)
}
