use crate::Partition;
use std::io;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use trading_protocol::{Command, CommandResult, SubmitError};

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

pub struct Engine {
    partition: Partition,
    config: EngineConfig,
}

impl Engine {
    pub fn new(partition: Partition, config: EngineConfig) -> Self {
        assert!(config.queue_capacity > 0, "queue capacity must be positive");
        assert!(config.max_batch_size > 0, "batch size must be positive");
        Self { partition, config }
    }

    pub fn spawn(self) -> EngineHandle {
        let (sender, receiver) = sync_channel(self.config.queue_capacity);
        let join = thread::Builder::new()
            .name("partition-writer".to_string())
            .spawn(move || self.run(receiver))
            .expect("failed to spawn partition writer");
        EngineHandle {
            sender,
            join: Some(join),
        }
    }

    fn run(mut self, receiver: Receiver<Message>) -> io::Result<Partition> {
        loop {
            let first = match receiver.recv() {
                Ok(Message::Execute(request)) => request,
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
                    Ok(Message::Execute(request)) => batch.push(request),
                    Ok(Message::Shutdown) => stop_after_batch = true,
                    Err(_) => break,
                }
            }
            let commands: Vec<_> = batch.iter().map(|request| request.command).collect();
            match self.partition.execute_batch(&commands) {
                Ok(results) => {
                    for (request, result) in batch.into_iter().zip(results) {
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
    response: SyncSender<Result<CommandResult, SubmitError>>,
}

enum Message {
    Execute(Request),
    Shutdown,
}

pub struct EngineHandle {
    sender: SyncSender<Message>,
    join: Option<JoinHandle<io::Result<Partition>>>,
}

impl EngineHandle {
    /// Blocks when the bounded queue is full, providing natural backpressure.
    pub fn submit(&self, command: Command) -> Result<CommandResult, SubmitError> {
        let (response, receiver) = sync_channel(1);
        self.sender
            .send(Message::Execute(Request { command, response }))
            .map_err(|_| SubmitError::Stopped)?;
        receiver.recv().map_err(|_| SubmitError::Stopped)?
    }

    /// Fails immediately instead of waiting when the queue is full.
    pub fn try_submit(&self, command: Command) -> Result<CommandResult, SubmitError> {
        let (response, receiver) = sync_channel(1);
        match self
            .sender
            .try_send(Message::Execute(Request { command, response }))
        {
            Ok(()) => receiver.recv().map_err(|_| SubmitError::Stopped)?,
            Err(TrySendError::Full(_)) => Err(SubmitError::Overloaded),
            Err(TrySendError::Disconnected(_)) => Err(SubmitError::Stopped),
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
