//! Local admin API for the Go control plane.
//!
//! Deliberately dependency-free over `std::net`: the endpoints are small,
//! JSON-over-HTTP/1.1, one request per connection. They expose placement and
//! route state and run the migration orchestrator synchronously so the
//! control plane receives the full audit report, including rollback/degraded
//! outcomes.

use dto::{MigrationRequest, MigrationResponse, PlacementDto, ReplicaDto, ShardsResponse};
use serde_json::{json, Value};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use trading_protocol::ShardId;
use trading_replication::{
    MigrationEventKind, MigrationFault, MigrationMode, MigrationReport, MigrationStatus,
    ReplicatedCluster, ShardMigrationPlan,
};

const MAX_BODY_BYTES: usize = 64 * 1024;

/// Serves until `POST /admin/shutdown`, the listener fails or the process exits.
pub fn serve(
    cluster: Arc<ReplicatedCluster>,
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
) -> io::Result<()> {
    listener.set_nonblocking(true)?;
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _peer)) => {
                if let Err(error) = handle_connection(stream, &cluster, &shutdown) {
                    eprintln!("trading-node admin: {error}");
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn handle_connection(
    mut stream: TcpStream,
    cluster: &Arc<ReplicatedCluster>,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    // Accepted sockets may inherit the listener's nonblocking flag.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(());
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let mut content_length = 0usize;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 {
            break;
        }
        if header == "\r\n" || header == "\n" {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }
    if content_length > MAX_BODY_BYTES {
        return write_response(&mut stream, 413, &json!({ "error": "body too large" }));
    }
    let mut body = vec![0_u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }
    let path = path.split('?').next().unwrap_or(path.as_str());
    let (status, value) = route(method.as_str(), path, &body, cluster, shutdown);
    write_response(&mut stream, status, &value)
}

fn route(
    method: &str,
    path: &str,
    body: &[u8],
    cluster: &Arc<ReplicatedCluster>,
    shutdown: &AtomicBool,
) -> (u16, Value) {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    match (method, segments.as_slice()) {
        ("GET", ["admin", "health"]) => (
            200,
            json!({
                "status": "ok",
                "route_version": cluster.route_table().version(),
                "shards": cluster.placements().len(),
                "migrations_serialized": true,
            }),
        ),
        ("GET", ["admin", "routes"]) => {
            let routes = cluster.route_table();
            (
                200,
                json!({ "version": routes.version(), "slots": routes.slots() }),
            )
        }
        ("GET", ["admin", "shards"]) => {
            let shards: Vec<PlacementDto> = cluster
                .placements()
                .into_values()
                .map(PlacementDto::from)
                .collect();
            (
                200,
                serde_json::json!(ShardsResponse {
                    route_version: cluster.route_table().version(),
                    shards,
                }),
            )
        }
        ("GET", ["admin", "shards", shard]) => {
            let Some(shard_id) = parse_shard(shard) else {
                return (400, json!({ "error": "invalid shard id" }));
            };
            let Some(placement) = cluster.placement(shard_id) else {
                return (404, json!({ "error": "unknown shard" }));
            };
            let mut summary = cluster.summary();
            let replicas: Vec<ReplicaDto> = summary
                .remove(&shard_id)
                .unwrap_or_default()
                .into_iter()
                .map(ReplicaDto::from)
                .collect();
            (
                200,
                json!({ "placement": PlacementDto::from(placement), "replicas": replicas }),
            )
        }
        ("POST", ["admin", "shards", shard, "migrate"]) => migrate(shard, body, cluster),
        ("POST", ["admin", "shutdown"]) => {
            shutdown.store(true, Ordering::Relaxed);
            (200, json!({ "status": "shutting-down" }))
        }
        _ => (404, json!({ "error": "not found" })),
    }
}

fn migrate(shard: &str, body: &[u8], cluster: &Arc<ReplicatedCluster>) -> (u16, Value) {
    let Some(shard_id) = parse_shard(shard) else {
        return (400, json!({ "error": "invalid shard id" }));
    };
    let request: MigrationRequest = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(error) => return (400, json!({ "error": format!("invalid plan: {error}") })),
    };
    let Some(placement) = cluster.placement(shard_id) else {
        return (404, json!({ "error": "unknown shard" }));
    };
    let plan = match build_plan(shard_id, &request) {
        Ok(plan) => plan,
        Err(error) => return (400, json!({ "error": error })),
    };
    if let Some(target) = plan.target_node {
        if placement.live_replicas.contains(&target) {
            return (
                409,
                json!({ "error": format!("target replica {target} is already live") }),
            );
        }
    }
    if let Some(decommission) = plan.decommission_node {
        if !placement.voters.contains(&decommission) {
            return (
                409,
                json!({ "error": format!("replica {decommission} is not a voter") }),
            );
        }
    }
    let report = cluster.migrate_shard(&plan);
    let status = match report.status {
        MigrationStatus::Completed => 200,
        MigrationStatus::RolledBack => 409,
        MigrationStatus::Degraded => 503,
    };
    (status, serde_json::json!(MigrationResponse::from(report)))
}

fn parse_shard(value: &str) -> Option<ShardId> {
    value.parse::<ShardId>().ok()
}

fn build_plan(shard: ShardId, request: &MigrationRequest) -> Result<ShardMigrationPlan, String> {
    let target = request.target_node;
    let decommission = request.decommission_node;
    let mut plan = match request.mode {
        dto::Mode::ScaleOut => {
            if decommission.is_some() {
                return Err("scale-out cannot decommission a voter".into());
            }
            ShardMigrationPlan::scale_out(shard, require_target(target)?)
        }
        dto::Mode::RelocateLeader => {
            if decommission.is_some() {
                return Err("relocate-leader resolves the old leader itself".into());
            }
            ShardMigrationPlan::relocate_leader(shard, require_target(target)?)
        }
        dto::Mode::ScaleIn => {
            if target.is_some() {
                return Err("scale-in cannot add a target".into());
            }
            ShardMigrationPlan::scale_in(
                shard,
                decommission.ok_or("scale-in requires decommission_node")?,
            )
        }
        dto::Mode::Replace => {
            let target = require_target(target)?;
            let decommission = decommission.ok_or("replace requires decommission_node")?;
            if target == decommission {
                return Err("target and decommission_node must differ".into());
            }
            ShardMigrationPlan::replace(shard, target, decommission)
        }
    };
    if let (Some(target), Some(decommission)) = (plan.target_node, request.decommission_node) {
        if target == decommission && plan.decommission_node.is_some() {
            return Err("target and decommission_node must differ".into());
        }
    }
    plan.prefer_target_leader = request.prefer_target_leader.unwrap_or(true);
    if let Some(catchup_ms) = request.catchup_timeout_ms {
        plan.catchup_timeout = Duration::from_millis(catchup_ms.clamp(500, 120_000));
    }
    if let Some(switch_ms) = request.switch_timeout_ms {
        plan.switch_timeout = Duration::from_millis(switch_ms.clamp(500, 60_000));
    }
    if let Some(lag) = request.catchup_lag {
        plan.catchup_lag = lag.min(1000);
    }
    plan.fault = match request.fault.as_deref() {
        None | Some("") => None,
        Some("crash-target-before-learner") => Some(MigrationFault::CrashTargetBeforeLearner),
        Some(other) => return Err(format!("unknown fault {other}")),
    };
    Ok(plan)
}

fn require_target(target: Option<u64>) -> Result<u64, String> {
    target.ok_or_else(|| "plan requires target_node".to_string())
}

fn write_response(stream: &mut TcpStream, status: u16, value: &Value) -> io::Result<()> {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"{\"error\":\"encode\"}".to_vec());
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        503 => "Service Unavailable",
        _ => "Error",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)?;
    stream.flush()
}

/// Conversions from replication domain types to the control-plane contract.
mod dto {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    pub(super) struct MigrationRequest {
        pub mode: Mode,
        pub target_node: Option<u64>,
        pub decommission_node: Option<u64>,
        pub prefer_target_leader: Option<bool>,
        pub catchup_timeout_ms: Option<u64>,
        pub switch_timeout_ms: Option<u64>,
        pub catchup_lag: Option<u64>,
        pub fault: Option<String>,
    }

    #[derive(Clone, Copy, Debug, serde::Deserialize)]
    #[serde(rename_all = "kebab-case")]
    pub(super) enum Mode {
        ScaleOut,
        RelocateLeader,
        ScaleIn,
        Replace,
    }

    #[derive(serde::Serialize)]
    pub(super) struct ShardsResponse {
        pub route_version: u64,
        pub shards: Vec<PlacementDto>,
    }

    #[derive(Clone, serde::Serialize)]
    pub(super) struct PlacementDto {
        pub shard: ShardId,
        pub epoch: u64,
        pub voters: Vec<u64>,
        pub learners: Vec<u64>,
        pub leader: Option<u64>,
        pub live_replicas: Vec<u64>,
    }

    impl From<trading_replication::ShardPlacement> for PlacementDto {
        fn from(placement: trading_replication::ShardPlacement) -> Self {
            Self {
                shard: placement.shard,
                epoch: placement.epoch,
                voters: placement.voters,
                learners: placement.learners,
                leader: placement.leader,
                live_replicas: placement.live_replicas,
            }
        }
    }

    #[derive(serde::Serialize)]
    pub(super) struct ReplicaDto {
        pub id: u64,
        pub alive: bool,
        pub leader: bool,
        pub voter: bool,
        pub learner: bool,
        pub state: String,
        pub term: u64,
        pub last_applied: Option<u64>,
        pub total_balance: Option<u128>,
        pub account_count: Option<usize>,
        pub result_count: Option<usize>,
    }

    impl From<trading_replication::ReplicaSummary> for ReplicaDto {
        fn from(replica: trading_replication::ReplicaSummary) -> Self {
            Self {
                id: replica.id,
                alive: replica.alive,
                leader: replica.leader,
                voter: replica.voter,
                learner: replica.learner,
                state: replica.state,
                term: replica.term,
                last_applied: replica.last_applied,
                total_balance: replica.total_balance,
                account_count: replica.account_count,
                result_count: replica.result_count,
            }
        }
    }

    #[derive(serde::Serialize)]
    pub(super) struct MigrationResponse {
        pub status: String,
        pub error: Option<String>,
        pub report: ReportDto,
    }

    impl From<MigrationReport> for MigrationResponse {
        fn from(report: MigrationReport) -> Self {
            Self {
                status: report.status.to_string(),
                error: report.error.clone(),
                report: ReportDto::from(report),
            }
        }
    }

    #[derive(serde::Serialize)]
    pub(super) struct ReportDto {
        pub shard: ShardId,
        pub mode: String,
        pub target_node: Option<u64>,
        pub retired_nodes: Vec<u64>,
        pub voters_before: Vec<u64>,
        pub voters_after: Vec<u64>,
        pub leader_before: Option<u64>,
        pub leader_after: Option<u64>,
        pub epoch_before: u64,
        pub epoch_after: u64,
        pub catchup: Option<CatchUpDto>,
        pub positions_before_promote: Vec<PositionDto>,
        pub timings: TimingsDto,
        pub events: Vec<EventDto>,
    }

    impl From<MigrationReport> for ReportDto {
        fn from(report: MigrationReport) -> Self {
            Self {
                shard: report.shard,
                mode: match &report.mode {
                    MigrationMode::ScaleOut => "scale-out".to_string(),
                    MigrationMode::RelocateLeader => "relocate-leader".to_string(),
                    MigrationMode::ScaleIn => "scale-in".to_string(),
                    MigrationMode::Replace { .. } => "replace".to_string(),
                },
                target_node: report.target_node,
                retired_nodes: report.retired_nodes,
                voters_before: report.voters_before,
                voters_after: report.voters_after,
                leader_before: report.leader_before,
                leader_after: report.leader_after,
                epoch_before: report.epoch_before,
                epoch_after: report.epoch_after,
                catchup: report.catchup.map(CatchUpDto::from),
                positions_before_promote: report
                    .positions_before_promote
                    .into_iter()
                    .map(PositionDto::from)
                    .collect(),
                timings: TimingsDto::from(report.timings),
                events: report.events.into_iter().map(EventDto::from).collect(),
            }
        }
    }

    #[derive(serde::Serialize)]
    pub(super) struct CatchUpDto {
        pub target: u64,
        pub leader: Option<u64>,
        pub leader_applied: Option<u64>,
        pub target_applied: Option<u64>,
        pub target_last_log: Option<u64>,
        pub lag: Option<u64>,
    }

    impl From<trading_replication::CatchUpReport> for CatchUpDto {
        fn from(report: trading_replication::CatchUpReport) -> Self {
            Self {
                target: report.target,
                leader: report.leader,
                leader_applied: report.leader_applied,
                target_applied: report.target_applied,
                target_last_log: report.target_last_log,
                lag: report.lag,
            }
        }
    }

    #[derive(serde::Serialize)]
    pub(super) struct PositionDto {
        pub id: u64,
        pub leader: bool,
        pub voter: bool,
        pub last_log_index: Option<u64>,
        pub last_applied: Option<u64>,
    }

    impl From<trading_replication::ReplicaPosition> for PositionDto {
        fn from(position: trading_replication::ReplicaPosition) -> Self {
            Self {
                id: position.id,
                leader: position.leader,
                voter: position.voter,
                last_log_index: position.last_log_index,
                last_applied: position.last_applied,
            }
        }
    }

    #[derive(serde::Serialize)]
    pub(super) struct TimingsDto {
        pub catchup_ms: u64,
        pub freeze_ms: u64,
        pub switch_ms: u64,
        pub election_ms: u64,
        pub total_ms: u64,
    }

    impl From<trading_replication::MigrationTimings> for TimingsDto {
        fn from(timings: trading_replication::MigrationTimings) -> Self {
            Self {
                catchup_ms: timings.catchup_ms,
                freeze_ms: timings.freeze_ms,
                switch_ms: timings.switch_ms,
                election_ms: timings.election_ms,
                total_ms: timings.total_ms,
            }
        }
    }

    #[derive(serde::Serialize)]
    pub(super) struct EventDto {
        pub elapsed_ms: u64,
        pub kind: String,
        pub detail: String,
    }

    impl From<trading_replication::MigrationEvent> for EventDto {
        fn from(event: trading_replication::MigrationEvent) -> Self {
            let kind = match event.kind {
                MigrationEventKind::Planned => "planned",
                MigrationEventKind::ReplicaCreated => "replica-created",
                MigrationEventKind::LearnerAdded => "learner-added",
                MigrationEventKind::CaughtUp => "caught-up",
                MigrationEventKind::FenceStarted => "fence-started",
                MigrationEventKind::MembershipCommitted => "membership-committed",
                MigrationEventKind::LeaderElected => "leader-elected",
                MigrationEventKind::RoutePublished => "route-published",
                MigrationEventKind::ReplicaRetired => "replica-retired",
                MigrationEventKind::Completed => "completed",
                MigrationEventKind::RolledBack => "rolled-back",
                MigrationEventKind::Failed => "failed",
            };
            Self {
                elapsed_ms: event.elapsed_ms,
                kind: kind.to_string(),
                detail: event.detail,
            }
        }
    }
}
