//! Binary codecs for raft log entries, membership and snapshot payloads.
//!
//! Entry payload layout: `[type: u8][body]`.
//! - `0x00` blank, `0x01` command (44 bytes), `0x02` membership.

use crate::types::{Entry, LogId, Membership};
use openraft::CommittedLeaderId;
use openraft::{BasicNode, EntryPayload};
use std::collections::{BTreeMap, BTreeSet};
use trading_protocol::Command;

const TYPE_BLANK: u8 = 0x00;
const TYPE_COMMAND: u8 = 0x01;
const TYPE_MEMBERSHIP: u8 = 0x02;
const MAX_MEMBERSHIP_NODES: u64 = 10_000;
const MAX_ADDR_LEN: u64 = 256;

/// Serializes an entry payload for the raft log (the WAL record body).
pub fn encode_entry(entry: &Entry) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(64);
    match &entry.payload {
        EntryPayload::Blank => buffer.push(TYPE_BLANK),
        EntryPayload::Normal(command) => {
            buffer.push(TYPE_COMMAND);
            buffer.extend_from_slice(&command.encode());
        }
        EntryPayload::Membership(membership) => {
            buffer.push(TYPE_MEMBERSHIP);
            encode_membership_into(membership, &mut buffer);
        }
    }
    buffer
}

/// Rebuilds an entry from its persisted form.
pub fn decode_entry(log_id: LogId, payload: &[u8]) -> Result<Entry, &'static str> {
    let tag = *payload.first().ok_or("empty entry payload")?;
    let body = &payload[1..];
    let raft_payload = match tag {
        TYPE_BLANK => EntryPayload::Blank,
        TYPE_COMMAND => EntryPayload::Normal(
            Command::decode(body).map_err(|_| "invalid command entry payload")?,
        ),
        TYPE_MEMBERSHIP => EntryPayload::Membership(decode_membership(body)?),
        _ => return Err("unknown entry payload type"),
    };
    Ok(Entry {
        log_id,
        payload: raft_payload,
    })
}

pub fn encode_membership_into(membership: &Membership, buffer: &mut Vec<u8>) {
    let configs = membership.get_joint_config();
    buffer.extend_from_slice(&(configs.len() as u32).to_le_bytes());
    for config in configs {
        buffer.extend_from_slice(&(config.len() as u32).to_le_bytes());
        for node in config {
            buffer.extend_from_slice(&node.to_le_bytes());
        }
    }
    let nodes: Vec<(u64, &BasicNode)> = membership.nodes().map(|(k, v)| (*k, v)).collect();
    buffer.extend_from_slice(&(nodes.len() as u32).to_le_bytes());
    for (node_id, node) in nodes {
        buffer.extend_from_slice(&node_id.to_le_bytes());
        buffer.extend_from_slice(&(node.addr.len() as u32).to_le_bytes());
        buffer.extend_from_slice(node.addr.as_bytes());
    }
}

pub fn decode_membership(bytes: &[u8]) -> Result<Membership, &'static str> {
    let mut cursor = 0usize;
    let configs_len = read_u32(bytes, &mut cursor)? as u64;
    if configs_len > 4 {
        return Err("membership has too many joint configs");
    }
    let mut configs = Vec::with_capacity(configs_len as usize);
    for _ in 0..configs_len {
        let nodes_len = read_u32(bytes, &mut cursor)? as u64;
        if nodes_len > MAX_MEMBERSHIP_NODES {
            return Err("membership config too large");
        }
        let mut config = BTreeSet::new();
        for _ in 0..nodes_len {
            config.insert(read_u64(bytes, &mut cursor)?);
        }
        configs.push(config);
    }
    let nodes_len = read_u32(bytes, &mut cursor)? as u64;
    if nodes_len > MAX_MEMBERSHIP_NODES {
        return Err("membership node map too large");
    }
    let mut nodes = BTreeMap::new();
    for _ in 0..nodes_len {
        let node_id = read_u64(bytes, &mut cursor)?;
        let addr_len = read_u32(bytes, &mut cursor)? as u64;
        if addr_len > MAX_ADDR_LEN {
            return Err("membership node address too long");
        }
        let addr = std::str::from_utf8(&bytes[cursor..cursor + addr_len as usize])
            .map_err(|_| "membership address is not utf-8")?;
        cursor += addr_len as usize;
        nodes.insert(
            node_id,
            BasicNode {
                addr: addr.to_string(),
            },
        );
    }
    Ok(Membership::new(configs, nodes))
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, &'static str> {
    if *cursor + 4 > bytes.len() {
        return Err("truncated membership payload");
    }
    let value = u32::from_le_bytes(bytes[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(value)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, &'static str> {
    if *cursor + 8 > bytes.len() {
        return Err("truncated membership payload");
    }
    let value = u64::from_le_bytes(bytes[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

/// Snapshot envelope: `[version: u8][membership_len: u32][membership][state]`.
const SNAPSHOT_ENVELOPE_VERSION: u8 = 1;

pub fn encode_snapshot_envelope(state: &[u8], membership: &Membership) -> Vec<u8> {
    let mut envelope = Vec::with_capacity(state.len() + 128);
    envelope.push(SNAPSHOT_ENVELOPE_VERSION);
    let mut membership_bytes = Vec::new();
    encode_membership_into(membership, &mut membership_bytes);
    envelope.extend_from_slice(&(membership_bytes.len() as u32).to_le_bytes());
    envelope.extend_from_slice(&membership_bytes);
    envelope.extend_from_slice(state);
    envelope
}

pub fn decode_snapshot_envelope(bytes: &[u8]) -> Result<(Vec<u8>, Membership), &'static str> {
    if bytes.first() != Some(&SNAPSHOT_ENVELOPE_VERSION) {
        return Err("unknown snapshot envelope version");
    }
    let mut cursor = 1usize;
    let membership_len = read_u32(bytes, &mut cursor)? as usize;
    if cursor + membership_len > bytes.len() {
        return Err("truncated snapshot envelope");
    }
    let membership = decode_membership(&bytes[cursor..cursor + membership_len])?;
    cursor += membership_len;
    Ok((bytes[cursor..].to_vec(), membership))
}

/// Builds a `LogId` from the compact (term, index) pair used on disk.
pub fn log_id_of(term: u64, index: u64) -> LogId {
    LogId::new(CommittedLeaderId::new(term, 0), index)
}
