use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const WAL_MAGIC_V1: &[u8; 4] = b"TPW1";
const WAL_MAGIC_V2: &[u8; 4] = b"TPW2";
const HEADER_LEN_V1: usize = 24;
const HEADER_LEN_V2: usize = 32;
const MAX_PAYLOAD_LEN: usize = 1024 * 1024;

const WAL_VERSION_V2: u8 = 2;
const META_MAGIC: &[u8; 4] = b"TPM1";
const META_LEN: usize = 24;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncPolicy {
    /// Flush every committed batch before execution and response.
    Durable,
    /// Benchmark-only mode. Acknowledged data can be lost after a crash.
    UnsafeBuffered,
}

/// One durable record. `term` carries the Raft term for replicated shards;
/// standalone partitions always write term 0.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalRecord {
    pub term: u64,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

impl WalRecord {
    pub fn new(sequence: u64, payload: Vec<u8>) -> Self {
        Self {
            term: 0,
            sequence,
            payload,
        }
    }

    pub fn raft(term: u64, sequence: u64, payload: Vec<u8>) -> Self {
        Self {
            term,
            sequence,
            payload,
        }
    }
}

/// Boundary of the log prefix that has been compacted away.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogBoundary {
    pub term: u64,
    pub index: u64,
}

/// Sorted (sequence, file offset) pairs for every live record.
pub type WalOffsets = Vec<(u64, u64)>;

pub struct Wal {
    file: File,
    path: PathBuf,
    sync_policy: SyncPolicy,
    /// Sorted (sequence, offset) pairs for every live record.
    offsets: WalOffsets,
    /// Compacted prefix boundary, persisted in `log-meta.bin` next to the WAL.
    purged: Option<LogBoundary>,
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
        let (records, offsets) = recover_records(&mut file)?;
        let purged = read_meta(&path)?;
        file.seek(SeekFrom::End(0))?;
        Ok((
            Self {
                file,
                path,
                sync_policy,
                offsets,
                purged,
            },
            records,
        ))
    }

    /// First live sequence number, if any record remains.
    pub fn first_sequence(&self) -> Option<u64> {
        self.offsets.first().map(|(seq, _)| *seq)
    }

    /// Last live sequence number, if any record remains.
    pub fn last_sequence(&self) -> Option<u64> {
        self.offsets.last().map(|(seq, _)| *seq)
    }

    /// Term of the record at `sequence`, if it is still live.
    pub fn term_of(&mut self, sequence: u64) -> Option<u64> {
        let pos = find_offset(&self.offsets, sequence)?;
        let (_, offset) = self.offsets[pos];
        self.read_at(offset).ok().map(|record| record.term)
    }

    /// Compacted prefix boundary, if a purge ever happened.
    pub fn purged(&self) -> Option<LogBoundary> {
        self.purged
    }

    /// Reads live records with `from <= sequence < to`.
    pub fn read_range(&mut self, from: u64, to: u64) -> io::Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        if to <= from {
            return Ok(records);
        }
        let start = match lower_bound(&self.offsets, from) {
            Some(pos) => pos,
            None => return Ok(records),
        };
        for pos in start..self.offsets.len() {
            let (sequence, offset) = self.offsets[pos];
            if sequence >= to {
                break;
            }
            records.push(self.read_at(offset)?);
        }
        Ok(records)
    }

    /// Writes one contiguous batch and performs at most one durability flush.
    pub fn append_batch(&mut self, records: &[WalRecord]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let capacity = records
            .iter()
            .map(|r| HEADER_LEN_V2 + r.payload.len())
            .sum();
        let mut batch = Vec::with_capacity(capacity);
        for record in records {
            if record.payload.len() > MAX_PAYLOAD_LEN {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "WAL payload too large",
                ));
            }
            let payload_len = record.payload.len() as u32;
            let mut header = [0_u8; HEADER_LEN_V2];
            header[0..4].copy_from_slice(WAL_MAGIC_V2);
            header[4] = WAL_VERSION_V2;
            header[8..16].copy_from_slice(&record.term.to_le_bytes());
            header[16..24].copy_from_slice(&record.sequence.to_le_bytes());
            header[24..28].copy_from_slice(&payload_len.to_le_bytes());
            let checksum = checksum(&header[8..28], &record.payload);
            header[28..32].copy_from_slice(&checksum.to_le_bytes());
            batch.extend_from_slice(&header);
            batch.extend_from_slice(&record.payload);
        }
        self.file.seek(SeekFrom::End(0))?;
        let start_offset = self.file.stream_position()?;
        self.file.write_all(&batch)?;
        match self.sync_policy {
            SyncPolicy::Durable => self.file.sync_data(),
            SyncPolicy::UnsafeBuffered => Ok(()),
        }?;
        let mut offset = start_offset;
        for record in records {
            self.offsets.push((record.sequence, offset));
            offset += (HEADER_LEN_V2 + record.payload.len()) as u64;
        }
        Ok(())
    }

    /// Drops every record with `sequence >= first_truncated`.
    /// Removes uncommitted suffixes after leadership changes.
    pub fn truncate_from(&mut self, first_truncated: u64) -> io::Result<()> {
        let start = match lower_bound(&self.offsets, first_truncated) {
            Some(pos) => pos,
            None => return Ok(()),
        };
        if start >= self.offsets.len() {
            return Ok(());
        }
        let cut_offset = self.offsets[start].1;
        self.file.set_len(cut_offset)?;
        match self.sync_policy {
            SyncPolicy::Durable => self.file.sync_data(),
            SyncPolicy::UnsafeBuffered => Ok(()),
        }?;
        self.file.seek(SeekFrom::End(0))?;
        self.offsets.truncate(start);
        Ok(())
    }

    /// Compacts away every record with `sequence <= boundary.index` and
    /// persists the purge boundary in `log-meta.bin`.
    pub fn purge_through(&mut self, boundary: LogBoundary) -> io::Result<()> {
        if let Some(existing) = self.purged {
            if boundary.index <= existing.index {
                return Ok(());
            }
        }
        let kept_from =
            lower_bound(&self.offsets, boundary.index + 1).unwrap_or(self.offsets.len());
        self.rewrite_keep_from(kept_from)?;
        self.purged = Some(boundary);
        write_meta(&self.path, boundary, self.sync_policy)
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
        drop(temp);
        std::fs::rename(&temp_path, &self.path)?;
        sync_parent(&self.path)?;
        self.file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        self.file.seek(SeekFrom::End(0))?;
        self.offsets.clear();
        self.purged = None;
        match std::fs::remove_file(meta_path(&self.path)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn read_at(&mut self, offset: u64) -> io::Result<WalRecord> {
        read_record_at(&mut self.file, offset)
    }

    fn rewrite_keep_from(&mut self, kept_from: usize) -> io::Result<()> {
        let temp_path = self.path.with_extension("wal.tmp");
        let mut temp = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)?;
        let mut new_offsets: WalOffsets = Vec::with_capacity(self.offsets.len() - kept_from);
        let mut write_offset = 0_u64;
        for pos in kept_from..self.offsets.len() {
            let (sequence, offset) = self.offsets[pos];
            let record = self.read_at(offset)?;
            let payload_len = record.payload.len() as u32;
            let mut header = [0_u8; HEADER_LEN_V2];
            header[0..4].copy_from_slice(WAL_MAGIC_V2);
            header[4] = WAL_VERSION_V2;
            header[8..16].copy_from_slice(&record.term.to_le_bytes());
            header[16..24].copy_from_slice(&record.sequence.to_le_bytes());
            header[24..28].copy_from_slice(&payload_len.to_le_bytes());
            let checksum = checksum(&header[8..28], &record.payload);
            header[28..32].copy_from_slice(&checksum.to_le_bytes());
            temp.write_all(&header)?;
            temp.write_all(&record.payload)?;
            new_offsets.push((sequence, write_offset));
            write_offset += (HEADER_LEN_V2 + record.payload.len()) as u64;
        }
        if self.sync_policy == SyncPolicy::Durable {
            temp.sync_all()?;
        }
        drop(temp);
        std::fs::rename(&temp_path, &self.path)?;
        if self.sync_policy == SyncPolicy::Durable {
            sync_parent(&self.path)?;
        }
        self.file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        self.file.seek(SeekFrom::End(0))?;
        self.offsets = new_offsets;
        Ok(())
    }
}

fn recover_records(file: &mut File) -> io::Result<(Vec<WalRecord>, WalOffsets)> {
    file.seek(SeekFrom::Start(0))?;
    let mut records = Vec::new();
    let mut offsets = Vec::new();
    loop {
        let record_offset = file.stream_position()?;
        let mut magic = [0_u8; 4];
        match read_exact_or_eof(file, &mut magic, record_offset)? {
            Some(()) => {}
            None => return Ok((records, offsets)),
        }
        let record = if &magic == WAL_MAGIC_V1 || &magic == WAL_MAGIC_V2 {
            let v1 = &magic == WAL_MAGIC_V1;
            let mut rest = [0_u8; HEADER_LEN_V2 - 4];
            let rest_len = if v1 {
                HEADER_LEN_V1 - 4
            } else {
                HEADER_LEN_V2 - 4
            };
            match read_exact_or_eof(file, &mut rest[..rest_len], record_offset)? {
                Some(()) => {}
                None => return Ok((records, offsets)),
            }
            if v1 {
                let payload_len = u32::from_le_bytes(rest[12..16].try_into().unwrap()) as usize;
                if payload_len > MAX_PAYLOAD_LEN {
                    return Err(corruption("invalid WAL payload length"));
                }
                let mut payload = vec![0_u8; payload_len];
                match read_exact_or_eof(file, &mut payload, record_offset)? {
                    Some(()) => {}
                    None => return Ok((records, offsets)),
                }
                let expected = u32::from_le_bytes(rest[16..20].try_into().unwrap());
                if checksum(&rest[4..16], &payload) != expected {
                    return Err(corruption("WAL checksum mismatch"));
                }
                WalRecord::new(u64::from_le_bytes(rest[4..12].try_into().unwrap()), payload)
            } else {
                let payload_len = u32::from_le_bytes(rest[20..24].try_into().unwrap()) as usize;
                if payload_len > MAX_PAYLOAD_LEN {
                    return Err(corruption("invalid WAL payload length"));
                }
                let mut payload = vec![0_u8; payload_len];
                match read_exact_or_eof(file, &mut payload, record_offset)? {
                    Some(()) => {}
                    None => return Ok((records, offsets)),
                }
                let expected = u32::from_le_bytes(rest[24..28].try_into().unwrap());
                if checksum(&rest[4..24], &payload) != expected {
                    return Err(corruption("WAL checksum mismatch"));
                }
                WalRecord::raft(
                    u64::from_le_bytes(rest[4..12].try_into().unwrap()),
                    u64::from_le_bytes(rest[12..20].try_into().unwrap()),
                    payload,
                )
            }
        } else {
            // A trailing fragment shorter than a full header is a torn write;
            // anything else with an unknown magic is real corruption.
            let file_len = file.metadata()?.len();
            if file_len - record_offset < HEADER_LEN_V2 as u64 {
                file.set_len(record_offset)?;
                return Ok((records, offsets));
            }
            return Err(corruption("invalid WAL header"));
        };
        offsets.push((record.sequence, record_offset));
        records.push(record);
    }
}

/// Fills `buffer` completely, or truncates the torn tail at `record_offset`
/// and returns `None` when the file ends mid-record.
fn read_exact_or_eof(
    file: &mut File,
    buffer: &mut [u8],
    record_offset: u64,
) -> io::Result<Option<()>> {
    let mut read = 0;
    while read < buffer.len() {
        match file.read(&mut buffer[read..])? {
            0 => {
                file.set_len(record_offset)?;
                return Ok(None);
            }
            count => read += count,
        }
    }
    Ok(Some(()))
}

fn read_record_at(file: &mut File, offset: u64) -> io::Result<WalRecord> {
    file.seek(SeekFrom::Start(offset))?;
    let mut header = [0_u8; HEADER_LEN_V2];
    file.read_exact(&mut header)?;
    if &header[0..4] == WAL_MAGIC_V1 {
        let payload_len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
        let mut payload = vec![0_u8; payload_len];
        file.read_exact(&mut payload)?;
        let expected = u32::from_le_bytes(header[20..24].try_into().unwrap());
        if checksum(&header[8..20], &payload) != expected {
            return Err(corruption("WAL checksum mismatch"));
        }
        Ok(WalRecord::new(
            u64::from_le_bytes(header[8..16].try_into().unwrap()),
            payload,
        ))
    } else if &header[0..4] == WAL_MAGIC_V2 {
        let payload_len = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;
        let mut payload = vec![0_u8; payload_len];
        file.read_exact(&mut payload)?;
        let expected = u32::from_le_bytes(header[28..32].try_into().unwrap());
        if checksum(&header[8..28], &payload) != expected {
            return Err(corruption("WAL checksum mismatch"));
        }
        Ok(WalRecord::raft(
            u64::from_le_bytes(header[8..16].try_into().unwrap()),
            u64::from_le_bytes(header[16..24].try_into().unwrap()),
            payload,
        ))
    } else {
        Err(corruption("invalid WAL header"))
    }
}

fn checksum(header: &[u8], payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(header);
    hasher.update(payload);
    hasher.finalize()
}

/// Position of the first entry with `sequence >= target`, if any slot matches.
fn lower_bound(offsets: &[(u64, u64)], target: u64) -> Option<usize> {
    let mut lo = 0usize;
    let mut hi = offsets.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if offsets[mid].0 < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == offsets.len() {
        None
    } else {
        Some(lo)
    }
}

/// Position of the exact sequence, if present.
fn find_offset(offsets: &[(u64, u64)], sequence: u64) -> Option<usize> {
    let pos = lower_bound(offsets, sequence)?;
    if offsets[pos].0 == sequence {
        Some(pos)
    } else {
        None
    }
}

fn meta_path(wal_path: &Path) -> PathBuf {
    wal_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("log-meta.bin")
}

fn write_meta(wal_path: &Path, boundary: LogBoundary, sync_policy: SyncPolicy) -> io::Result<()> {
    let path = meta_path(wal_path);
    let temp_path = path.with_extension("bin.tmp");
    let mut payload = [0_u8; META_LEN];
    payload[0..4].copy_from_slice(META_MAGIC);
    payload[8..16].copy_from_slice(&boundary.term.to_le_bytes());
    payload[16..24].copy_from_slice(&boundary.index.to_le_bytes());
    let checksum = crc32fast::hash(&payload[8..24]);
    payload[4..8].copy_from_slice(&checksum.to_le_bytes());
    let mut temp = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)?;
    temp.write_all(&payload)?;
    if sync_policy == SyncPolicy::Durable {
        temp.sync_all()?;
    }
    drop(temp);
    std::fs::rename(&temp_path, &path)?;
    if sync_policy == SyncPolicy::Durable {
        sync_parent(&path)
    } else {
        Ok(())
    }
}

fn read_meta(wal_path: &Path) -> io::Result<Option<LogBoundary>> {
    let path = meta_path(wal_path);
    if !path.exists() {
        return Ok(None);
    }
    let payload = std::fs::read(&path)?;
    if payload.len() != META_LEN || &payload[0..4] != META_MAGIC {
        return Err(corruption("invalid log-meta header"));
    }
    let checksum = u32::from_le_bytes(payload[4..8].try_into().unwrap());
    if crc32fast::hash(&payload[8..24]) != checksum {
        return Err(corruption("log-meta checksum mismatch"));
    }
    Ok(Some(LogBoundary {
        term: u64::from_le_bytes(payload[8..16].try_into().unwrap()),
        index: u64::from_le_bytes(payload[16..24].try_into().unwrap()),
    }))
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
    fn raft_records_round_trip_with_terms() {
        let path = temp_dir().join("partition.wal");
        let records = vec![
            WalRecord::raft(1, 1, vec![1]),
            WalRecord::raft(1, 2, vec![2, 2]),
            WalRecord::raft(3, 3, vec![3, 3, 3]),
        ];
        let (mut wal, _) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        wal.append_batch(&records).unwrap();
        drop(wal);
        let (mut wal, recovered) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert_eq!(recovered, records);
        assert_eq!(wal.first_sequence(), Some(1));
        assert_eq!(wal.last_sequence(), Some(3));
        assert_eq!(wal.term_of(3), Some(3));
        assert_eq!(wal.term_of(9), None);
        let range = wal.read_range(2, 4).unwrap();
        assert_eq!(range, records[1..]);
        drop(wal);
    }

    #[test]
    fn v1_records_are_still_readable() {
        let path = temp_dir().join("partition.wal");
        {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .unwrap();
            let payload = vec![9, 9, 9, 9];
            let mut header = [0_u8; HEADER_LEN_V1];
            header[0..4].copy_from_slice(WAL_MAGIC_V1);
            header[4] = 1;
            header[8..16].copy_from_slice(&7_u64.to_le_bytes());
            header[16..20].copy_from_slice(&(payload.len() as u32).to_le_bytes());
            let checksum = checksum(&header[8..20], &payload);
            header[20..24].copy_from_slice(&checksum.to_le_bytes());
            file.write_all(&header).unwrap();
            file.write_all(&payload).unwrap();
        }
        let (_, records) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert_eq!(records, vec![WalRecord::new(7, vec![9, 9, 9, 9])]);
    }

    #[test]
    fn truncate_from_drops_suffix() {
        let path = temp_dir().join("partition.wal");
        let records: Vec<_> = (1..=4_u64)
            .map(|i| WalRecord::raft(1, i, vec![i as u8; 16]))
            .collect();
        let (mut wal, _) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        wal.append_batch(&records).unwrap();
        wal.truncate_from(3).unwrap();
        drop(wal);
        let (mut wal, recovered) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert_eq!(recovered, records[..2]);
        assert_eq!(wal.last_sequence(), Some(2));
        wal.append_batch(&[WalRecord::raft(2, 3, vec![7; 16])])
            .unwrap();
        drop(wal);
        let (_, recovered) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert_eq!(recovered.len(), 3);
        assert_eq!(recovered[2].term, 2);
    }

    #[test]
    fn purge_through_compacts_prefix_and_persists_boundary() {
        let path = temp_dir().join("partition.wal");
        let records: Vec<_> = (1..=5_u64)
            .map(|i| WalRecord::raft(2, i, vec![i as u8; 8]))
            .collect();
        let (mut wal, _) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        wal.append_batch(&records).unwrap();
        wal.purge_through(LogBoundary { term: 2, index: 3 })
            .unwrap();
        drop(wal);
        let (mut wal, recovered) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert_eq!(wal.purged(), Some(LogBoundary { term: 2, index: 3 }));
        assert_eq!(recovered, records[3..]);
        assert_eq!(wal.first_sequence(), Some(4));
        let range = wal.read_range(0, 10).unwrap();
        assert_eq!(range, records[3..]);
        // Idempotent repeat.
        wal.purge_through(LogBoundary { term: 2, index: 3 })
            .unwrap();
        drop(wal);
        let (wal, _) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert_eq!(wal.purged(), Some(LogBoundary { term: 2, index: 3 }));
    }

    #[test]
    fn purge_beyond_last_empties_log_and_keeps_boundary() {
        let path = temp_dir().join("partition.wal");
        let (mut wal, _) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        wal.append_batch(&[
            WalRecord::raft(1, 1, vec![1]),
            WalRecord::raft(1, 2, vec![2]),
        ])
        .unwrap();
        wal.purge_through(LogBoundary { term: 1, index: 9 })
            .unwrap();
        drop(wal);
        let (mut wal, recovered) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert!(recovered.is_empty());
        assert_eq!(wal.purged(), Some(LogBoundary { term: 1, index: 9 }));
        wal.append_batch(&[WalRecord::raft(2, 10, vec![3])])
            .unwrap();
        drop(wal);
        let (_, recovered) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert_eq!(recovered, vec![WalRecord::raft(2, 10, vec![3])]);
    }

    #[test]
    fn reset_clears_purge_boundary() {
        let path = temp_dir().join("partition.wal");
        let (mut wal, _) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        wal.append_batch(&[WalRecord::raft(1, 1, vec![1])]).unwrap();
        wal.purge_through(LogBoundary { term: 1, index: 1 })
            .unwrap();
        wal.reset().unwrap();
        drop(wal);
        let (wal, recovered) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        assert!(recovered.is_empty());
        assert_eq!(wal.purged(), None);
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
            (HEADER_LEN_V2 + 32) as u64
        );
    }

    #[test]
    fn complete_but_corrupt_record_is_not_silently_discarded() {
        let path = temp_dir().join("partition.wal");
        let (mut wal, _) = Wal::open(&path, SyncPolicy::Durable).unwrap();
        wal.append_batch(&[WalRecord::new(1, vec![7; 32])]).unwrap();
        drop(wal);
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start((HEADER_LEN_V2 + 2) as u64))
            .unwrap();
        file.write_all(&[9]).unwrap();
        drop(file);
        assert_eq!(
            Wal::open(&path, SyncPolicy::Durable).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
