//! Asynchronous, idempotent PostgreSQL projection of committed shard events.

use postgres::{Client, NoTls};
use std::fmt;
use std::sync::mpsc::Receiver;
use std::thread::{self, JoinHandle};
use trading_protocol::CommittedEvent;

const MAX_BATCH_SIZE: usize = 1024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProjectionStats {
    pub received: u64,
    pub inserted: u64,
}

#[derive(Debug)]
pub enum ProjectionError {
    Postgres(postgres::Error),
    Spawn(std::io::Error),
    ValueOverflow(&'static str),
    ThreadPanic,
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Postgres(error) => write!(f, "PostgreSQL projection failed: {error}"),
            Self::Spawn(error) => write!(f, "failed to spawn PostgreSQL projector: {error}"),
            Self::ValueOverflow(field) => write!(f, "{field} does not fit in PostgreSQL BIGINT"),
            Self::ThreadPanic => write!(f, "PostgreSQL projector panicked"),
        }
    }
}

impl std::error::Error for ProjectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Postgres(error) => Some(error),
            Self::Spawn(error) => Some(error),
            Self::ValueOverflow(_) | Self::ThreadPanic => None,
        }
    }
}

impl From<postgres::Error> for ProjectionError {
    fn from(error: postgres::Error) -> Self {
        Self::Postgres(error)
    }
}

pub struct ProjectionHandle {
    join: JoinHandle<Result<ProjectionStats, ProjectionError>>,
}

impl ProjectionHandle {
    /// Waits until every event sender is dropped and all received events are committed.
    pub fn shutdown(self) -> Result<ProjectionStats, ProjectionError> {
        self.join.join().map_err(|_| ProjectionError::ThreadPanic)?
    }
}

pub struct PostgresProjection;

impl PostgresProjection {
    /// Connects and initializes the query model before starting the asynchronous consumer.
    pub fn spawn(
        database_url: &str,
        receiver: Receiver<CommittedEvent>,
    ) -> Result<ProjectionHandle, ProjectionError> {
        let mut client = Client::connect(database_url, NoTls)?;
        initialize_schema(&mut client)?;
        let join = thread::Builder::new()
            .name("postgres-projector".to_string())
            .spawn(move || run(&mut client, receiver))
            .map_err(ProjectionError::Spawn)?;
        Ok(ProjectionHandle { join })
    }
}

fn run(
    client: &mut Client,
    receiver: Receiver<CommittedEvent>,
) -> Result<ProjectionStats, ProjectionError> {
    let mut stats = ProjectionStats::default();
    while let Ok(first) = receiver.recv() {
        let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
        batch.push(first);
        batch.extend(receiver.try_iter().take(MAX_BATCH_SIZE - 1));
        project_batch(client, &batch, &mut stats)?;
    }
    Ok(stats)
}

fn project_batch(
    client: &mut Client,
    events: &[CommittedEvent],
    stats: &mut ProjectionStats,
) -> Result<(), ProjectionError> {
    let mut transaction = client.transaction()?;
    for event in events {
        let shard_id = i32::from(event.shard_id);
        let route_version = bigint(event.route_version, "route_version")?;
        let sequence = bigint(event.sequence, "sequence")?;
        let result_sequence = bigint(event.result.sequence, "result_sequence")?;
        let request_id = event.command.request_id.to_le_bytes().to_vec();
        let command = event.command.encode().to_vec();
        let status = i16::from(event.result.status.to_u8());
        stats.inserted += transaction.execute(
            "INSERT INTO trading_committed_events (\
                shard_id, route_version, sequence, request_id, command, result_sequence, status\
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)\
             ON CONFLICT (shard_id, route_version, sequence) DO NOTHING",
            &[
                &shard_id,
                &route_version,
                &sequence,
                &request_id,
                &command,
                &result_sequence,
                &status,
            ],
        )?;
        stats.received += 1;
    }
    transaction.commit()?;
    Ok(())
}

fn initialize_schema(client: &mut Client) -> Result<(), postgres::Error> {
    client.batch_execute(
        "CREATE TABLE IF NOT EXISTS trading_committed_events (\
            shard_id INTEGER NOT NULL,\
            route_version BIGINT NOT NULL,\
            sequence BIGINT NOT NULL,\
            request_id BYTEA NOT NULL,\
            command BYTEA NOT NULL,\
            result_sequence BIGINT NOT NULL,\
            status SMALLINT NOT NULL,\
            PRIMARY KEY (shard_id, route_version, sequence)\
         )",
    )
}

fn bigint(value: u64, field: &'static str) -> Result<i64, ProjectionError> {
    i64::try_from(value).map_err(|_| ProjectionError::ValueOverflow(field))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use trading_protocol::{Command, CommandResult, Status};

    /// Run with TRADING_POSTGRES_URL and `cargo test -p trading-projection -- --ignored`.
    #[test]
    #[ignore = "requires a PostgreSQL connection string"]
    fn repeated_delivery_does_not_duplicate_rows() {
        let url = std::env::var("TRADING_POSTGRES_URL").unwrap();
        let shard_id = (std::process::id() % 30_000) as u16 + 1;
        let mut client = Client::connect(&url, NoTls).unwrap();
        initialize_schema(&mut client).unwrap();
        client
            .execute(
                "DELETE FROM trading_committed_events WHERE shard_id = $1",
                &[&i32::from(shard_id)],
            )
            .unwrap();

        let (sender, receiver) = mpsc::channel();
        let projection = PostgresProjection::spawn(&url, receiver).unwrap();
        let command = Command::deposit(77, 2, 100);
        let event = CommittedEvent {
            shard_id,
            route_version: 3,
            sequence: 9,
            command,
            result: CommandResult {
                request_id: command.request_id,
                sequence: 9,
                status: Status::Success,
            },
        };
        sender.send(event).unwrap();
        sender.send(event).unwrap();
        drop(sender);
        let stats = projection.shutdown().unwrap();
        assert_eq!(stats.received, 2);
        assert_eq!(stats.inserted, 1);

        let row = client
            .query_one(
                "SELECT COUNT(*) FROM trading_committed_events WHERE shard_id = $1",
                &[&i32::from(shard_id)],
            )
            .unwrap();
        assert_eq!(row.get::<usize, i64>(0), 1);
        client
            .execute(
                "DELETE FROM trading_committed_events WHERE shard_id = $1",
                &[&i32::from(shard_id)],
            )
            .unwrap();
    }
}
