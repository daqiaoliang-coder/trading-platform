use crate::State;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use trading_protocol::{Command, CommandResult};
use trading_storage::{Snapshot, SyncPolicy, Wal, WalRecord};

#[derive(Clone, Debug)]
pub struct PartitionConfig {
    pub data_dir: PathBuf,
    pub sync_policy: SyncPolicy,
}

impl PartitionConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            sync_policy: SyncPolicy::Durable,
        }
    }
}

pub struct Partition {
    state: State,
    wal: Wal,
    last_sequence: u64,
    snapshot_path: PathBuf,
    _lock_file: File,
}

impl Partition {
    pub fn open(config: PartitionConfig) -> io::Result<Self> {
        let data_dir_existed = config.data_dir.exists();
        std::fs::create_dir_all(&config.data_dir)?;
        if !data_dir_existed {
            sync_parent(&config.data_dir)?;
        }
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config.data_dir.join("partition.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock_file).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("partition data directory is already locked: {error}"),
            )
        })?;
        let wal_path = config.data_dir.join("partition.wal");
        let snapshot_path = config.data_dir.join("snapshot.bin");
        let (state, mut last_sequence) = if snapshot_path.exists() {
            let snapshot = Snapshot::read(&snapshot_path)?;
            (
                State::decode_snapshot(&snapshot.payload)?,
                snapshot.last_sequence,
            )
        } else {
            (State::default(), 0)
        };
        let (wal, records) = Wal::open(&wal_path, config.sync_policy)?;
        let mut state = state;
        for record in records {
            if record.sequence <= last_sequence {
                continue;
            }
            if record.sequence != last_sequence + 1 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "WAL sequence gap: expected {}, got {}",
                        last_sequence + 1,
                        record.sequence
                    ),
                ));
            }
            let command = Command::decode(&record.payload)?;
            state.execute(record.sequence, command);
            last_sequence = record.sequence;
        }
        Ok(Self {
            state,
            wal,
            last_sequence,
            snapshot_path,
            _lock_file: lock_file,
        })
    }

    /// Persists the entire batch before applying any command in memory.
    pub fn execute_batch(&mut self, commands: &[Command]) -> io::Result<Vec<CommandResult>> {
        if commands.is_empty() {
            return Ok(Vec::new());
        }
        let records: Vec<_> = commands
            .iter()
            .enumerate()
            .map(|(index, command)| {
                WalRecord::new(
                    self.last_sequence + index as u64 + 1,
                    command.encode().to_vec(),
                )
            })
            .collect();
        self.wal.append_batch(&records)?;
        let mut results = Vec::with_capacity(commands.len());
        for (record, command) in records.iter().zip(commands) {
            results.push(self.state.execute(record.sequence, *command));
            self.last_sequence = record.sequence;
        }
        Ok(results)
    }

    /// Writes a durable snapshot before discarding the WAL it covers.
    /// Checkpointing is always durable: it drops the WAL, so skipping fsync
    /// here could lose more than the tail.
    pub fn checkpoint(&mut self) -> io::Result<()> {
        Snapshot::write_atomic(
            &self.snapshot_path,
            self.last_sequence,
            0,
            &self.state.encode_snapshot(),
            SyncPolicy::Durable,
        )?;
        self.wal.reset()
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub fn balance(&self, account: u64) -> Option<u64> {
        self.state.balance(account)
    }

    pub fn total_balance(&self) -> u128 {
        self.state.total_balance()
    }

    pub fn account_count(&self) -> usize {
        self.state.account_count()
    }

    pub fn result_count(&self) -> usize {
        self.state.result_count()
    }

    pub fn execute(&mut self, command: Command) -> io::Result<CommandResult> {
        let mut results = self.execute_batch(&[command])?;
        Ok(results.remove(0))
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
