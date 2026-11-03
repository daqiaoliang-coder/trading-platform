//! Drives the node admin API over real HTTP: placement discovery, a successful
//! online migration, a fault-injected rollback with retry, and shutdown.

use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "trading-node-admin-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("temp dir");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Node {
    child: Child,
    addr: String,
    dir: TempDir,
}

impl Node {
    // The child is moved into the guard; Node::drop kills and waits for it.
    #[allow(clippy::zombie_processes)]
    fn start() -> Self {
        let dir = TempDir::new();
        let child = Command::new(env!("CARGO_BIN_EXE_trading-node"))
            .args([
                "--data-dir",
                dir.0.to_str().expect("utf8 path"),
                "--shards",
                "2",
                "--replicas",
                "3",
                "--unsafe-buffered",
                "--admin-addr",
                "127.0.0.1:0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn trading-node");
        let addr_file = dir.0.join("admin.addr");
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok(contents) = std::fs::read_to_string(&addr_file) {
                let addr = contents.trim().to_string();
                if !addr.is_empty() {
                    return Self { child, addr, dir };
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("node admin listener did not report an address");
    }

    fn request(&self, method: &str, path: &str, body: Option<&str>) -> (u16, Value) {
        let mut stream = TcpStream::connect(&self.addr).expect("connect admin");
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .expect("read timeout");
        let body = body.unwrap_or("");
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.addr,
            body.len()
        );
        stream.write_all(request.as_bytes()).expect("write request");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        let (head, body) = response.split_once("\r\n\r\n").expect("http response");
        let status = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|status| status.parse().ok())
            .expect("status code");
        let value = serde_json::from_str(body).unwrap_or_else(|error| {
            panic!("invalid json response ({error}): {body}");
        });
        (status, value)
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = &self.dir;
    }
}

#[test]
fn admin_api_runs_online_migrations_and_reports_rollbacks() {
    let _serial = SERIAL.lock().unwrap_or_else(|poison| poison.into_inner());
    let mut node = Node::start();

    let (status, health) = node.request("GET", "/admin/health", None);
    assert_eq!(status, 200);
    assert_eq!(health["status"], "ok");

    let (status, shards) = node.request("GET", "/admin/shards", None);
    assert_eq!(status, 200);
    let placements = shards["shards"].as_array().expect("shard placements");
    assert_eq!(placements.len(), 2);
    for placement in placements {
        assert_eq!(placement["voters"].as_array().expect("voters").len(), 3);
        assert!(placement["leader"].as_u64().is_some());
        assert_eq!(placement["epoch"], 1);
    }
    let old_leader = placements[0]["leader"].as_u64().expect("shard 0 leader");

    // Online relocation: add replica 4, retire the old primary, publish epoch 2.
    let (status, migration) = node.request(
        "POST",
        "/admin/shards/0/migrate",
        Some(r#"{"mode":"relocate-leader","target_node":4,"switch_timeout_ms":10000}"#),
    );
    assert_eq!(status, 200, "migration response: {migration}");
    assert_eq!(migration["status"], "completed");
    assert_eq!(migration["report"]["epoch_before"], 1);
    assert_eq!(migration["report"]["epoch_after"], 2);
    assert_eq!(migration["report"]["leader_after"], 4);
    assert_eq!(migration["report"]["retired_nodes"][0], old_leader);
    assert!(migration["report"]["events"]
        .as_array()
        .expect("audit events")
        .iter()
        .any(|event| event["kind"] == "route-published"));

    let (status, shards) = node.request("GET", "/admin/shards", None);
    assert_eq!(status, 200);
    assert_eq!(shards["route_version"], 2);
    let shard0 = &shards["shards"]
        .as_array()
        .expect("placements")
        .iter()
        .find(|placement| placement["shard"] == 0)
        .expect("shard 0");
    assert_eq!(shard0["leader"], 4);
    assert_eq!(shard0["voters"], serde_json::json!([2, 3, 4]));

    // Drill the rollback path: the target is stopped before joining, so the
    // old voter set must remain authoritative and the epoch must not move.
    let faulted = node.request(
        "POST",
        "/admin/shards/0/migrate",
        Some(
            r#"{"mode":"scale-out","target_node":5,"catchup_timeout_ms":3000,
               "fault":"crash-target-before-learner"}"#,
        ),
    );
    assert_eq!(faulted.0, 409, "rollback response: {}", faulted.1);
    assert_eq!(faulted.1["status"], "rolled-back");
    assert_eq!(faulted.1["report"]["epoch_after"], 2);
    assert_eq!(
        faulted.1["report"]["voters_after"],
        serde_json::json!([2, 3, 4])
    );

    // Retrying the same target after rollback must succeed.
    let (status, retry) = node.request(
        "POST",
        "/admin/shards/0/migrate",
        Some(r#"{"mode":"scale-out","target_node":5,"switch_timeout_ms":10000}"#),
    );
    assert_eq!(status, 200, "retry response: {retry}");
    assert_eq!(retry["status"], "completed");
    assert_eq!(retry["report"]["epoch_after"], 3);
    assert_eq!(
        retry["report"]["voters_after"],
        serde_json::json!([2, 3, 4, 5])
    );

    // Invalid plans are rejected before any state changes.
    let (status, error) = node.request(
        "POST",
        "/admin/shards/0/migrate",
        Some(r#"{"mode":"scale-in"}"#),
    );
    assert_eq!(status, 400, "invalid plan response: {error}");

    let (status, routes) = node.request("GET", "/admin/routes", None);
    assert_eq!(status, 200);
    assert_eq!(routes["version"], 3);

    let (status, _) = node.request("POST", "/admin/shutdown", None);
    assert_eq!(status, 200);
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if node.child.try_wait().expect("wait").is_some() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("node did not exit after /admin/shutdown");
}
