//! In-process Raft transport: bounded channels, oneshot replies, injectable
//! directed link cuts. Cross-process transport (TCP/gRPC) is M4 scope.

use crate::types::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    Raft, RaftError, TypeConfig, VoteRequest, VoteResponse,
};
use openraft::error::{InstallSnapshotError, RPCError, RemoteError, Timeout, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::{BasicNode, RPCTypes};
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// An RPC in flight to a replica's server task.
pub enum Inbound {
    AppendEntries {
        request: Box<AppendEntriesRequest>,
        reply: oneshot::Sender<Result<AppendEntriesResponse, RaftError>>,
    },
    Vote {
        request: VoteRequest,
        reply: oneshot::Sender<Result<VoteResponse, RaftError>>,
    },
    InstallSnapshot {
        request: Box<InstallSnapshotRequest>,
        reply: oneshot::Sender<Result<InstallSnapshotResponse, RaftError<InstallSnapshotError>>>,
    },
}

/// Serves one replica's inbound RPCs until the channel closes.
pub async fn serve_inbound(raft: Raft, mut inbound: mpsc::Receiver<Inbound>) {
    while let Some(message) = inbound.recv().await {
        match message {
            Inbound::AppendEntries { request, reply } => {
                let _ = reply.send(raft.append_entries(*request).await);
            }
            Inbound::Vote { request, reply } => {
                let _ = reply.send(raft.vote(request).await);
            }
            Inbound::InstallSnapshot { request, reply } => {
                let _ = reply.send(raft.install_snapshot(*request).await);
            }
        }
    }
}

/// The shared mailbox map of one replication group plus injected link cuts.
pub struct GroupTransport {
    inbound: Mutex<BTreeMap<u64, mpsc::Sender<Inbound>>>,
    cuts: Mutex<BTreeSet<(u64, u64)>>,
}

impl GroupTransport {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inbound: Mutex::new(BTreeMap::new()),
            cuts: Mutex::new(BTreeSet::new()),
        })
    }

    /// Registers a replica's inbound channel (called when a replica starts).
    pub fn register(&self, node: u64, sender: mpsc::Sender<Inbound>) {
        self.inbound
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(node, sender);
    }

    /// Unregisters a replica (called on crash shutdown so peers see Unreachable).
    pub fn unregister(&self, node: u64) {
        self.inbound
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&node);
    }

    /// Injects a bidirectional partition between two replicas.
    pub fn cut(&self, a: u64, b: u64) {
        let mut cuts = self
            .cuts
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        cuts.insert((a, b));
        cuts.insert((b, a));
    }

    /// Heals a previously injected partition.
    pub fn heal(&self, a: u64, b: u64) {
        let mut cuts = self
            .cuts
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        cuts.remove(&(a, b));
        cuts.remove(&(b, a));
    }

    /// Heals every injected partition in the group.
    pub fn heal_all(&self) {
        self.cuts
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clear();
    }

    fn is_cut(&self, target: u64) -> bool {
        // A cut is symmetric, so testing one direction suffices. The source is
        // implied: every registered peer is a potential source.
        let cuts = self
            .cuts
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        cuts.iter().any(|&(_, to)| to == target)
    }

    fn sender(&self, target: u64) -> Option<mpsc::Sender<Inbound>> {
        self.inbound
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&target)
            .cloned()
    }
}

/// A client connection to one target replica.
pub struct ChannelNetwork {
    transport: Arc<GroupTransport>,
    target: u64,
}

fn network_unreachable<E: std::error::Error + 'static>(
    message: &'static str,
) -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&io::Error::new(
        io::ErrorKind::ConnectionAborted,
        message,
    )))
}

impl ChannelNetwork {
    // RPCError's size is fixed by openraft's RaftNetwork trait signatures.
    #[allow(clippy::result_large_err)]
    async fn call<T, E>(
        &self,
        action: RPCTypes,
        option: &RPCOption,
        build: impl FnOnce(oneshot::Sender<Result<T, E>>) -> Inbound,
    ) -> Result<T, RPCError<u64, BasicNode, E>>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        if self.transport.is_cut(self.target) {
            return Err(network_unreachable("network partition injected"));
        }
        let sender = match self.transport.sender(self.target) {
            Some(sender) => sender,
            None => return Err(network_unreachable("replica is not running")),
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        if sender.send(build(reply_tx)).await.is_err() {
            return Err(network_unreachable("replica is not running"));
        }
        let timeout = option.hard_ttl();
        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(Ok(result))) => Ok(result),
            Ok(Ok(Err(e))) => Err(RPCError::RemoteError(RemoteError::new(self.target, e))),
            Ok(Err(_)) => Err(network_unreachable("replica dropped the RPC")),
            Err(_) => Err(RPCError::Timeout(Timeout {
                action,
                id: self.target,
                target: self.target,
                timeout,
            })),
        }
    }
}

impl RaftNetwork<TypeConfig> for ChannelNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse, RPCError<u64, BasicNode, RaftError>> {
        self.call(RPCTypes::AppendEntries, &option, |reply| {
            Inbound::AppendEntries {
                request: Box::new(rpc),
                reply,
            }
        })
        .await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest,
        option: RPCOption,
    ) -> Result<InstallSnapshotResponse, RPCError<u64, BasicNode, RaftError<InstallSnapshotError>>>
    {
        self.call(RPCTypes::InstallSnapshot, &option, |reply| {
            Inbound::InstallSnapshot {
                request: Box::new(rpc),
                reply,
            }
        })
        .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest,
        option: RPCOption,
    ) -> Result<VoteResponse, RPCError<u64, BasicNode, RaftError>> {
        self.call(RPCTypes::Vote, &option, |reply| Inbound::Vote {
            request: rpc,
            reply,
        })
        .await
    }

    fn backoff(&self) -> openraft::network::Backoff {
        // Short backoff keeps failure detection tight for the failover drills.
        openraft::network::Backoff::new(std::iter::repeat(Duration::from_millis(100)))
    }
}

/// Creates per-target client connections over the shared group transport.
pub struct GroupNetworkFactory {
    transport: Arc<GroupTransport>,
}

impl GroupNetworkFactory {
    pub fn new(transport: Arc<GroupTransport>) -> Self {
        Self { transport }
    }
}

impl RaftNetworkFactory<TypeConfig> for GroupNetworkFactory {
    type Network = ChannelNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        ChannelNetwork {
            transport: Arc::clone(&self.transport),
            target,
        }
    }
}
