//! Durable storage of the openraft vote (`term`, `voted_for`, `committed`).

use crate::types::Vote;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::PathBuf;

const VOTE_MAGIC: &[u8; 4] = b"TPV1";
const VOTE_LEN: usize = 32;

pub struct VoteStore {
    path: PathBuf,
}

impl VoteStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn read(&self) -> io::Result<Option<Vote>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let mut payload = vec![0_u8; VOTE_LEN];
        File::open(&self.path)?.read_exact(&mut payload)?;
        if &payload[0..4] != VOTE_MAGIC {
            return Err(corruption("invalid vote file header"));
        }
        let checksum = u32::from_le_bytes(payload[4..8].try_into().unwrap());
        if crc32fast::hash(&payload[8..26]) != checksum {
            return Err(corruption("vote file checksum mismatch"));
        }
        let term = u64::from_le_bytes(payload[8..16].try_into().unwrap());
        let voted_for = match payload[16] {
            0 => None,
            1 => Some(u64::from_le_bytes(payload[17..25].try_into().unwrap())),
            _ => return Err(corruption("invalid voted_for tag")),
        };
        let committed = payload[25] != 0;
        Ok(Some(Vote {
            leader_id: openraft::LeaderId { term, voted_for },
            committed,
        }))
    }

    /// Persists the vote atomically: fsync the temp file, rename, sync the parent.
    pub fn save(&self, vote: &Vote) -> io::Result<()> {
        let mut payload = [0_u8; VOTE_LEN];
        payload[0..4].copy_from_slice(VOTE_MAGIC);
        payload[8..16].copy_from_slice(&vote.leader_id.term.to_le_bytes());
        match vote.leader_id.voted_for {
            None => payload[16] = 0,
            Some(id) => {
                payload[16] = 1;
                payload[17..25].copy_from_slice(&id.to_le_bytes());
            }
        }
        payload[25] = if vote.committed { 1 } else { 0 };
        let checksum = crc32fast::hash(&payload[8..26]);
        payload[4..8].copy_from_slice(&checksum.to_le_bytes());
        let temp_path = self.path.with_extension("bin.tmp");
        let mut temp = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)?;
        temp.write_all(&payload)?;
        temp.sync_all()?;
        drop(temp);
        std::fs::rename(&temp_path, &self.path)?;
        if let Some(parent) = self.path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    }
}

fn corruption(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
