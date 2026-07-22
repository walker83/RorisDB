//! Shared end-to-end test harness for the integration test suites.
//!
//! Every e2e suite under `tests/suites/` used to duplicate the same
//! `E2eServer` / `find_binary` / `make_conn` boilerplate, each copy spawning
//! the `harness-db` binary on its own `MYSQL_PORT`. The duplication meant a
//! server-side change (such as the auth fix that requires `--dev`) had to be
//! repeated across 14 files.
//!
//! This module centralizes the spawn/connect logic. Each suite keeps its own
//! `MYSQL_PORT` constant and its own `lazy_static! SERVER` (so suites run on
//! independent ports), but delegates the actual work here. `start()` always
//! passes `--dev` so the server accepts the root user with an empty password —
//! matching how every suite connects via `make_conn()`.

use mysql::{Opts, OptsBuilder};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// A spawned `harness-db` server process bound to a specific MySQL port.
///
/// The server is started with `--dev` (insecure root / empty password) so the
/// integration tests can connect with `root` and no password. On drop the
/// process is killed and its temporary data/meta directories are removed.
pub struct E2eServer {
    child: Child,
    meta_dir: String,
    data_dir: String,
    port: u16,
}

impl E2eServer {
    /// Start a server on `port`, using per-(pid, port) temporary directories.
    pub fn start(port: u16) -> Self {
        let pid = std::process::id();
        let meta_dir = format!("/tmp/harness_e2e_meta_{}_{}", pid, port);
        let data_dir = format!("/tmp/harness_e2e_data_{}_{}", pid, port);
        let _ = std::fs::remove_dir_all(&meta_dir);
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&meta_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();
        let binary = find_binary();
        let child = Command::new(&binary)
            .arg("--mysql-port")
            .arg(port.to_string())
            .arg("--meta-dir")
            .arg(&meta_dir)
            .arg("--data-dir")
            .arg(&data_dir)
            // Insecure root/empty-password credentials for the test suite.
            .arg("--dev")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to start harness-db '{}': {}", binary, e));
        let server = E2eServer {
            child,
            meta_dir,
            data_dir,
            port,
        };
        server.wait_ready();
        server
    }

    /// Block until the MySQL port accepts TCP connections (up to 30s).
    fn wait_ready(&self) {
        let start = std::time::Instant::now();
        loop {
            if start.elapsed() > Duration::from_secs(30) {
                panic!("Server not ready within 30s on port {}", self.port);
            }
            if std::net::TcpStream::connect(format!("127.0.0.1:{}", self.port)).is_ok() {
                thread::sleep(Duration::from_millis(500));
                return;
            }
            thread::sleep(Duration::from_millis(300));
        }
    }
}

impl Drop for E2eServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.meta_dir);
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

/// Locate the `harness-db` binary built by the workspace.
pub fn find_binary() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    for p in &[
        format!("{}/../../target/release/harness-db", manifest_dir),
        format!("{}/../../target/debug/harness-db", manifest_dir),
    ] {
        if Path::new(p).exists() {
            return p.to_string();
        }
    }
    panic!("harness-db binary not found. Build with: cargo build --release");
}

/// Open a MySQL connection to the test server on `port` as `root` (no password).
pub fn make_conn(port: u16) -> mysql::Conn {
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(port)
        .user(Some("root"))
        .pass(None::<String>);
    mysql::Conn::new(Opts::from(opts)).expect("Failed to create connection")
}

/// Build the per-suite shared server. Each suite calls this inside its own
/// `lazy_static!` block with its own `MYSQL_PORT`, yielding an `Arc<E2eServer>`
/// whose lifetime spans the whole test binary.
pub fn shared_server(port: u16) -> Arc<E2eServer> {
    Arc::new(E2eServer::start(port))
}
