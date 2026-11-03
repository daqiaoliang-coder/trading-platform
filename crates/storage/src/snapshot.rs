use crate::SyncPolicy;
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

const SNAPSHOT_MAGIC_V1: &[u8; 4] = b"TPS1";
const SNAPSHOT_MAGIC_V2: &[u8; 4] = b"TPS2";
const SNAPSHOT_VERSION_V2: u8 = 2;
const HEADER_LEN_V1: usize = 32;
const HEADER_LEN_V2: usize = 40;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub last_sequence: u64,
    /// Raft term of the entry at `last_sequence`; 0 for standalone partitions.
    pub last_term: u64,
    pub payload: Vec<u8>,
}

impl Snapshot {
    pub fn read(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        let snapshot = if bytes.len() >= HEADER_LEN_V1 && &bytes[0..4] == SNAPSHOT_MAGIC_V1 {
            let last_sequence = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
            let payload_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
            if bytes.len() != HEADER_LEN_V1 + payload_len {
                return Err(corruption("snapshot length mismatch"));
            }
            let expected = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
            let mut hasher = Hasher::new();
            hasher.update(&bytes[8..24]);
            hasher.update(&bytes[HEADER_LEN_V1..]);
            if hasher.finalize() != expected {
                return Err(corruption("snapshot checksum mismatch"));
            }
            Self {
                last_sequence,
                last_term: 0,
                payload: bytes[HEADER_LEN_V1..].to_vec(),
            }
        } else if bytes.len() >= HEADER_LEN_V2 && &bytes[0..4] == SNAPSHOT_MAGIC_V2 {
            if bytes[4] != SNAPSHOT_VERSION_V2 {
                return Err(corruption("invalid snapshot header"));
            }
            let last_sequence = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
            let last_term = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
            let payload_len = u64::from_le_bytes(bytes[24..32].try_into().unwrap()) as usize;
            if bytes.len() != HEADER_LEN_V2 + payload_len {
                return Err(corruption("snapshot length mismatch"));
            }
            let expected = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
            let mut hasher = Hasher::new();
            hasher.update(&bytes[8..32]);
            hasher.update(&bytes[HEADER_LEN_V2..]);
            if hasher.finalize() != expected {
                return Err(corruption("snapshot checksum mismatch"));
            }
            Self {
                last_sequence,
                last_term,
                payload: bytes[HEADER_LEN_V2..].to_vec(),
            }
        } else {
            return Err(corruption("invalid snapshot header"));
        };
        Ok(snapshot)
    }

    /// Atomically replaces the snapshot. Durability follows `sync_policy`:
    /// `Durable` fsyncs the file and its parent directory; `UnsafeBuffered`
    /// only renames (recent snapshots may be lost on a crash, matching the
    /// policy's contract for the WAL).
    pub fn write_atomic(
        path: impl AsRef<Path>,
        last_sequence: u64,
        last_term: u64,
        payload: &[u8],
        sync_policy: SyncPolicy,
    ) -> io::Result<()> {
        let path = path.as_ref();
        let temp_path = path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)?;
        let mut header = [0_u8; HEADER_LEN_V2];
        header[0..4].copy_from_slice(SNAPSHOT_MAGIC_V2);
        header[4] = SNAPSHOT_VERSION_V2;
        header[8..16].copy_from_slice(&last_sequence.to_le_bytes());
        header[16..24].copy_from_slice(&last_term.to_le_bytes());
        header[24..32].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        let mut hasher = Hasher::new();
        hasher.update(&header[8..32]);
        hasher.update(payload);
        header[32..36].copy_from_slice(&hasher.finalize().to_le_bytes());
        file.write_all(&header)?;
        file.write_all(payload)?;
        if sync_policy == SyncPolicy::Durable {
            file.sync_all()?;
        }
        drop(file);
        std::fs::rename(&temp_path, path)?;
        if sync_policy == SyncPolicy::Durable {
            if let Some(parent) = path.parent() {
                File::open(parent)?.sync_all()?;
            }
        }
        Ok(())
    }
}

fn corruption(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "trading-snapshot-test-{}-{}.bin",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn snapshot_round_trip() {
        let path = temp_path();
        Snapshot::write_atomic(&path, 42, 3, b"state", SyncPolicy::Durable).unwrap();
        assert_eq!(
            Snapshot::read(&path).unwrap(),
            Snapshot {
                last_sequence: 42,
                last_term: 3,
                payload: b"state".to_vec()
            }
        );
    }

    #[test]
    fn snapshot_rejects_corruption() {
        let path = temp_path();
        Snapshot::write_atomic(&path, 42, 3, b"state", SyncPolicy::Durable).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(
            Snapshot::read(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn snapshot_rejects_corrupted_last_sequence() {
        let path = temp_path();
        Snapshot::write_atomic(&path, 42, 3, b"state", SyncPolicy::Durable).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[8] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(
            Snapshot::read(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn v1_snapshot_is_still_readable() {
        let path = temp_path();
        let payload = b"legacy".to_vec();
        let mut header = [0_u8; HEADER_LEN_V1];
        header[0..4].copy_from_slice(SNAPSHOT_MAGIC_V1);
        header[4] = 1;
        header[8..16].copy_from_slice(&42_u64.to_le_bytes());
        header[16..24].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        let mut hasher = Hasher::new();
        hasher.update(&header[8..24]);
        hasher.update(&payload);
        header[24..28].copy_from_slice(&hasher.finalize().to_le_bytes());
        let mut bytes = header.to_vec();
        bytes.extend_from_slice(&payload);
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(
            Snapshot::read(&path).unwrap(),
            Snapshot {
                last_sequence: 42,
                last_term: 0,
                payload: b"legacy".to_vec()
            }
        );
    }
}
