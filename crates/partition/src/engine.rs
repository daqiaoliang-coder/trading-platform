use crate::Partition;
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, Sender, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use trading_protocol::{
    Command, CommandResult, CommittedEvent, RouteVersion, ShardId, SubmitError,
};

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub queue_capacity: usize,
    pub max_batch_size: usize,
    pub batch_wait: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            max_batch_size: 64,
            batch_wait: Duration::from_micros(200),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineMetricsSnapshot {
    pub queue_capacity: usize,
    pub max_batch_size: usize,
    pub queue_depth: usize,
    pub submitted: u64,
    pub executed: u64,
    pub overloaded: u64,
    pub batches: u64,
    pub total_batch_latency_ns: u64,
    pub max_batch_latency_ns: u64,
    pub last_sequence: u64,
}

#[derive(Default)]
struct Metrics {
    queue_depth: AtomicUsize,
    submitted: AtomicU64,
    executed: AtomicU64,
    overloaded: AtomicU64,
    batches: AtomicU64,
    total_batch_latency_ns: AtomicU64,
    max_batch_latency_ns: AtomicU64,
    last_sequence: AtomicU64,
}

pub struct Engine {
    partition: Partition,
    config: EngineConfig,
    metrics: Arc<Metrics>,
    shard_id: ShardId,
    event_sender: Option<Sender<CommittedEvent>>,
}

impl Engine {
    pub fn new(partition: Partition, config: EngineConfig) -> Self {
        assert!(config.queue_capacity > 0, "queue capacity must be positive");
        assert!(config.max_batch_size > 0, "batch size must be positive");
        let metrics = Metrics {
            last_sequence: AtomicU64::new(partition.last_sequence()),
            ..Metrics::default()
        };
        Self {
            partition,
            config,
            metrics: Arc::new(metrics),
            shard_id: 0,
            event_sender: None,
        }
    }

    pub fn with_event_sender(
        mut self,
        shard_id: ShardId,
        event_sender: Sender<CommittedEvent>,
    ) -> Self {
        self.shard_id = shard_id;
        self.event_sender = Some(event_sender);
        self
    }

    pub fn spawn(self) -> EngineHandle {
        self.spawn_named("partition-writer")
    }

    pub fn spawn_named(self, name: impl Into<String>) -> EngineHandle {
        let (sender, receiver) = sync_channel(self.config.queue_capacity);
        let metrics = Arc::clone(&self.metrics);
        let queue_capacity = self.config.queue_capacity;
        let max_batch_size = self.config.max_batch_size;
        let join = thread::Builder::new()
            .name(name.into())
            .spawn(move || self.run(receiver))
            .expect("failed to spawn partition writer");
        EngineHandle {
            sender,
            join: Some(join),
            metrics,
            queue_capacity,
            max_batch_size,
        }
    }

    fn run(mut self, receiver: Receiver<Message>) -> io::Result<Partition> {
        loop {
            let first = match receiver.recv() {
                Ok(Message::Execute(request)) => {
                    self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
                    request
                }
                Ok(Message::Shutdown) | Err(_) => return Ok(self.partition),
            };
            let mut batch = vec![first];
            let mut stop_after_batch = false;
            let deadline = std::time::Instant::now() + self.config.batch_wait;
            while batch.len() < self.config.max_batch_size && !stop_after_batch {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match receiver.recv_timeout(remaining) {
                    Ok(Message::Execute(request)) => {
                        self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
                        batch.push(request);
                    }
                    Ok(Message::Shutdown) => stop_after_batch = true,
                    Err(_) => break,
                }
            }
            let commands: Vec<_> = batch.iter().map(|request| request.command).collect();
            let first_sequence = self.partition.last_sequence() + 1;
            let started = std::time::Instant::now();
            match self.partition.execute_batch(&commands) {
                Ok(results) => {
                    let elapsed_ns = started.elapsed().as_nanos() as u64;
                    self.metrics.batches.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .executed
                        .fetch_add(results.len() as u64, Ordering::Relaxed);
                    self.metrics
                        .total_batch_latency_ns
                        .fetch_add(elapsed_ns, Ordering::Relaxed);
                    self.metrics
                        .max_batch_latency_ns
                        .fetch_max(elapsed_ns, Ordering::Relaxed);
                    self.metrics
                        .last_sequence
                        .store(self.partition.last_sequence(), Ordering::Relaxed);
                    for (index, (request, result)) in batch.into_iter().zip(results).enumerate() {
                        if let Some(event_sender) = &self.event_sender {
                            let _ = event_sender.send(CommittedEvent {
                                shard_id: self.shard_id,
                                route_version: request.route_version,
                                sequence: first_sequence + index as u64,
                                command: request.command,
                                result,
                            });
                        }
                        let _ = request.response.send(Ok(result));
                    }
                }
                Err(error) => {
                    let message = error.to_string();
                    for request in batch {
                        let _ = request
                            .response
                            .send(Err(SubmitError::Storage(message.clone())));
                    }
                    return Err(error);
                }
            }
            if stop_after_batch {
                return Ok(self.partition);
            }
        }
    }
}

struct Request {
    command: Command,
    route_version: RouteVersion,
    response: SyncSender<Result<CommandResult, SubmitError>>,
}

enum Message {
    Execute(Request),
    Shutdown,
}

pub struct EngineHandle {
    sender: SyncSender<Message>,
    join: Option<JoinHandle<io::Result<Partition>>>,
    metrics: Arc<Metrics>,
    queue_capacity: usize,
    max_batch_size: usize,
}

impl EngineHandle {
    /// Blocks when the bounded queue is full, providing natural backpressure.
    pub fn submit(&self, command: Command) -> Result<CommandResult, SubmitError> {
        self.submit_inner(0, command)
    }

    pub(crate) fn submit_at_version(
        &self,
        route_version: RouteVersion,
        command: Command,
    ) -> Result<CommandResult, SubmitError> {
        self.submit_inner(route_version, command)
    }

    fn submit_inner(
        &self,
        route_version: RouteVersion,
        command: Command,
    ) -> Result<CommandResult, SubmitError> {
        let (response, receiver) = sync_channel(1);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
        if self
            .sender
            .send(Message::Execute(Request {
                command,
                route_version,
                response,
            }))
            .is_err()
        {
            self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
            return Err(SubmitError::Stopped);
        }
        self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
        receiver.recv().map_err(|_| SubmitError::Stopped)?
    }

    /// Fails immediately instead of waiting when the queue is full.
    pub fn try_submit(&self, command: Command) -> Result<CommandResult, SubmitError> {
        self.try_submit_inner(0, command)
    }

    pub(crate) fn try_submit_at_version(
        &self,
        route_version: RouteVersion,
        command: Command,
    ) -> Result<CommandResult, SubmitError> {
        self.try_submit_inner(route_version, command)
    }

    fn try_submit_inner(
        &self,
        route_version: RouteVersion,
        command: Command,
    ) -> Result<CommandResult, SubmitError> {
        let (response, receiver) = sync_channel(1);
        self.metrics.queue_depth.fetch_add(1, Ordering::Relaxed);
        match self.sender.try_send(Message::Execute(Request {
            command,
            route_version,
            response,
        })) {
            Ok(()) => {
                self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
                receiver.recv().map_err(|_| SubmitError::Stopped)?
            }
            Err(TrySendError::Full(_)) => {
                self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
                self.metrics.overloaded.fetch_add(1, Ordering::Relaxed);
                Err(SubmitError::Overloaded)
            }
            Err(TrySendError::Disconnected(_)) => {
                self.metrics.queue_depth.fetch_sub(1, Ordering::Relaxed);
                Err(SubmitError::Stopped)
            }
        }
    }

    pub fn metrics(&self) -> EngineMetricsSnapshot {
        EngineMetricsSnapshot {
            queue_capacity: self.queue_capacity,
            max_batch_size: self.max_batch_size,
            queue_depth: self.metrics.queue_depth.load(Ordering::Relaxed),
            submitted: self.metrics.submitted.load(Ordering::Relaxed),
            executed: self.metrics.executed.load(Ordering::Relaxed),
            overloaded: self.metrics.overloaded.load(Ordering::Relaxed),
            batches: self.metrics.batches.load(Ordering::Relaxed),
            total_batch_latency_ns: self.metrics.total_batch_latency_ns.load(Ordering::Relaxed),
            max_batch_latency_ns: self.metrics.max_batch_latency_ns.load(Ordering::Relaxed),
            last_sequence: self.metrics.last_sequence.load(Ordering::Relaxed),
        }
    }

    /// Waits for all previously queued commands and returns the recovered state owner.
    pub fn shutdown(mut self) -> io::Result<Partition> {
        let _ = self.sender.send(Message::Shutdown);
        match self.join.take().unwrap().join() {
            Ok(result) => result,
            Err(_) => Err(io::Error::other("partition writer panicked")),
        }
    }
}
