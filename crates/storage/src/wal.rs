use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const WAL_MAGIC: &[u8; 4] = b"TPW1";
const WAL_VERSION: u8 = 1;
const HEADER_LEN: usize = 24;
const MAX_PAYLOAD_LEN: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPolicy {
    /// Flush every committed batch before execution and response.
    Durable,
    /// Benchmark-only mode. Acknowledged data can be lost after a crash.
    UnsafeBuffered,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalRecord {
    pub sequence: u64,
    pub payload: Vec<u8>,
}

impl WalRecord {
    pub fn new(sequence: u64, payload: Vec<u8>) -> Self {
        Self { sequence, payload }
    }
}

pub struct Wal {
    file: File,
    path: PathBuf,
    sync_policy: SyncPolicy,
}

impl Wal {
    /// Opens a WAL, returns all valid records, and truncates one torn tail.
    pub fn open(
        path: impl AsRef<Path>,
        sync_policy: SyncPolicy,
    ) -> io::Result<(Self, Vec<WalRecord>)> {
        let path = path.as_ref().to_path_buf();
        let existed = path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .append(true)
            .open(&path)?;
        if !existed {
            file.sync_all()?;
            sync_parent(&path)?;
        }
        let records = recover_records(&mut file)?;
        file.seek(SeekFrom::End(0))?;
        Ok((
            Self {
                file,
                path,
                sync_policy,
            },
            records,
        ))
    }

    /// Writes one contiguous batch and performs at most one durability flush.
    pub fn append_batch(&mut self, records: &[WalRecord]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let capacity = records.iter().map(|r| HEADER_LEN + r.payload.len()).sum();
        let mut batch = Vec::with_capacity(capacity);
        for record in records {
            if record.payload.len() > MAX_PAYLOAD_LEN {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "WAL payload too large",
                ));
            }
            let payload_len = record.payload.len() as u32;
            let mut header = [0_u8; HEADER_LEN];
            header[0..4].copy_from_slice(WAL_MAGIC);
            header[4] = WAL_VERSION;
            header[8..16].copy_from_slice(&record.sequence.to_le_bytes());
            header[16..20].copy_from_slice(&payload_len.to_le_bytes());
            let checksum = checksum(&header[8..20], &record.payload);
            header[20..24].copy_from_slice(&checksum.to_le_bytes());
            batch.extend_from_slice(&header);
            batch.extend_from_slice(&record.payload);
        }
        self.file.write_all(&batch)?;
        match self.sync_policy {
            SyncPolicy::Durable => self.file.sync_data(),
            SyncPolicy::UnsafeBuffered => Ok(()),
        }
    }

    /// Atomically replaces the WAL with an empty file after a durable snapshot.
    pub fn reset(&mut self) -> io::Result<()> {
        let temp_path = self.path.with_extension("wal.tmp");
        let temp = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)?;
        temp.sync_all()?;
        std::fs::rename(&temp_path, &self.path)?;
        let directory_sync = sync_parent(&self.path);
        self.file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        self.file.seek(SeekFrom::End(0))?;
        directory_sync
    }
}

fn recover_records(file: &mut File) -> io::Result<Vec<WalRecord>> {
    file.seek(SeekFrom::Start(0))?;
    let mut records = Vec::new();
    loop {
        let record_offset = file.stream_position()?;
        let mut header = [0_u8; HEADER_LEN];
        let mut read = 0;
        while read < HEADER_LEN {
            match file.read(&mut header[read..])? {
                0 if read == 0 => return Ok(records),
                0 => {
                    file.set_len(record_offset)?;
                    return Ok(records);
                }
                count => read += count,
            }
        }
        if &header[0..4] != WAL_MAGIC || header[4] != WAL_VERSION {
            return Err(corruption("invalid WAL header"));
        }
        let payload_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        if payload_len > MAX_PAYLOAD_LEN {
            return Err(corruption("invalid WAL payload length"));
        }
        let mut payload = vec![0_u8; payload_len];
        let mut read = 0;
        while read < payload_len {
            match file.read(&mut payload[read..])? {
                0 => {
                    file.set_len(record_offset)?;
                    return Ok(records);
                }
                count => read += count,
            }
        }
        let expected = u32::from_le_bytes(header[20..24].try_into().unwrap());
        if checksum(&header[8..20], &payload) != expected {
            return Err(corruption("WAL checksum mismatch"));
        }
        records.push(WalRecord::new(
            u64::from_le_bytes(header[8..16].try_into().unwrap()),
            payload,
        ));
    }
}

fn checksum(header: &[u8], payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(header);
    hasher.update(payload);
    hasher.finalize()
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

fn corruption(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir() -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "trading-wal-test-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn batch_append_and_recovery_round_trip() {
        let path = temp_dir().join("partition.wal");
        let records = vec![
            WalRecord::new(1, vec![1, 2, 3]),
            WalRecord::new(2, vec![4, 5, 6]),
        ];
        let (mut wal, recovered) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert!(recovered.is_empty());
        wal.append_batch(&records).unwrap();
        drop(wal);
        let (_, recovered) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert_eq!(recovered, records);
    }

    #[test]
    fn recovery_truncates_a_torn_tail() {
        let path = temp_dir().join("partition.wal");
        let (mut wal, _) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        wal.append_batch(&[WalRecord::new(1, vec![7; 32])]).unwrap();
        drop(wal);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xAA; 11]).unwrap();
        drop(file);
        let (_, records) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            (HEADER_LEN + 32) as u64
        );
    }

    #[test]
    fn complete_but_corrupt_record_is_not_silently_discarded() {
        let path = temp_dir().join("partition.wal");
        let (mut wal, _) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        wal.append_batch(&[WalRecord::new(1, vec![7; 32])]).unwrap();
        drop(wal);
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start((HEADER_LEN + 2) as u64)).unwrap();
        file.write_all(&[9]).unwrap();
        drop(file);
        assert_eq!(
            Wal::open(&path, SyncPolicy::Durable).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
