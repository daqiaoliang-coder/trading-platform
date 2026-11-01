use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

const SNAPSHOT_MAGIC: &[u8; 4] = b"TPS1";
const SNAPSHOT_VERSION: u8 = 1;
const HEADER_LEN: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub last_sequence: u64,
    pub payload: Vec<u8>,
}

impl Snapshot {
    pub fn read(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        if bytes.len() < HEADER_LEN {
            return Err(corruption("snapshot is shorter than its header"));
        }
        if &bytes[0..4] != SNAPSHOT_MAGIC || bytes[4] != SNAPSHOT_VERSION {
            return Err(corruption("invalid snapshot header"));
        }
        let last_sequence = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let payload_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        if bytes.len() != HEADER_LEN + payload_len {
            return Err(corruption("snapshot length mismatch"));
        }
        let payload = bytes[HEADER_LEN..].to_vec();
        let expected = u32::from_le_bytes(bytes[24..28].try_into().unwrap());
        let mut hasher = Hasher::new();
        hasher.update(&bytes[8..24]);
        hasher.update(&payload);
        if hasher.finalize() != expected {
            return Err(corruption("snapshot checksum mismatch"));
        }
        Ok(Self {
            last_sequence,
            payload,
        })
    }

    /// Persists the temporary file before atomically replacing the snapshot.
    pub fn write_atomic(
        path: impl AsRef<Path>,
        last_sequence: u64,
        payload: &[u8],
    ) -> io::Result<()> {
        let path = path.as_ref();
        let temp_path = path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)?;
        let mut header = [0_u8; HEADER_LEN];
        header[0..4].copy_from_slice(SNAPSHOT_MAGIC);
        header[4] = SNAPSHOT_VERSION;
        header[8..16].copy_from_slice(&last_sequence.to_le_bytes());
        header[16..24].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        let mut hasher = Hasher::new();
        hasher.update(&header[8..24]);
        hasher.update(payload);
        header[24..28].copy_from_slice(&hasher.finalize().to_le_bytes());
        file.write_all(&header)?;
        file.write_all(payload)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temp_path, path)?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
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
        Snapshot::write_atomic(&path, 42, b"state").unwrap();
        assert_eq!(
            Snapshot::read(&path).unwrap(),
            Snapshot {
                last_sequence: 42,
                payload: b"state".to_vec()
            }
        );
    }

    #[test]
    fn snapshot_rejects_corruption() {
        let path = temp_path();
        Snapshot::write_atomic(&path, 42, b"state").unwrap();
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
        Snapshot::write_atomic(&path, 42, b"state").unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[8] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert_eq!(
            Snapshot::read(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
