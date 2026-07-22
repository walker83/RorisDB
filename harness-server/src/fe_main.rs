mod connection_tracker;
mod ddl_handler;
mod dml_handler;
mod handler_struct;
mod metrics;
mod query_executor;
mod utils;
mod web;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};

use datafusion::prelude::{SessionConfig, SessionContext};
use fe_backup::BackupManager;
use fe_catalog::CatalogManager;
use fe_common::edit_log::EditLog;
use fe_config::HarnessConfig;
use fe_config::SystemVariableManager;
use fe_datafusion::{register_doris_udfs, register_misc_udfs};
use fe_monitor::MonitoringManager;
use fe_monitor::audit_log::{AuditLogConfig, AuditLogEntry, AuditLogger, QueryStatus, QueryType};
use fe_sql_parser::{is_dml_sql, parse_sql};
use fe_storage::ParquetCatalogProvider;
use clickhouse_protocol::{ClickHouseServer, ClickHouseServerConfig};
use elasticsearch_protocol::{ElasticsearchServer, ElasticsearchServerConfig};
use influxdb_protocol::{InfluxDBServer, InfluxDBServerConfig};
use maxcompute_protocol::{McServerConfig, start_mc_server};
use mongodb_protocol::{MongoDBServer, MongoDBServerConfig};
use mysql_protocol::auth::{default_credentials, secure_credentials};
use mysql_protocol::server::{ColumnDef, ColumnType};
use mysql_protocol::{MysqlServer, QueryHandler, QueryResult, ServerConfig, auth::AuthPluginType};
use pg_protocol::{PgServer, PgServerConfig};
use redis_protocol::{RedisServer, RedisServerConfig};
use tablestore_protocol::{TableStoreServer, TableStoreServerConfig};
use oracle_protocol::{OracleServer, OracleServerConfig};
use cassandra_protocol::{CassandraServer, CassandraServerConfig};
use adb_mysql_protocol::AdbMysqlServer;
use lindorm_protocol::LindormServer;
use vector_protocol::VectorServer;
use sybase_protocol::{SybaseServer, SybaseServerConfig};
use tsql_executor::TsqlQueryHandler;

use connection_tracker::ConnectionTracker;
use handler_struct::HarnessQueryHandler;
use utils::{encode_arrow_batches_to_mysql_rows, record_batches_to_query_result_with_df_schema};
use web::{WebState, start_web_server};

#[derive(Parser)]
#[command(name = "harness-db", about = "Harness Frontend Server")]
struct Args {
    #[arg(long, default_value = "data/fe/doris-meta")]
    meta_dir: String,

    #[arg(long, default_value = "9030")]
    mysql_port: u16,

    #[arg(long, default_value = "9031")]
    maxcompute_port: u16,

    #[arg(long, default_value = "15432")]
    hologres_port: u16,

    #[arg(long, default_value = "data/fe/storage")]
    data_dir: String,

    #[arg(long, default_value = "harness.toml")]
    config_file: String,

    // Additional protocol ports (0 = disabled)
    #[arg(long, default_value_t = false)]
    enable_all_protocols: bool,

    #[arg(long, default_value = "6379")]
    redis_port: u16,

    #[arg(long, default_value = "27017")]
    mongodb_port: u16,

    #[arg(long, default_value = "8123")]
    clickhouse_port: u16,

    #[arg(long, default_value = "9200")]
    elasticsearch_port: u16,

    #[arg(long, default_value = "8086")]
    influxdb_port: u16,

    #[arg(long, default_value = "9042")]
    cassandra_port: u16,

    #[arg(long, default_value = "1521")]
    oracle_port: u16,

    #[arg(long, default_value = "8087")]
    tablestore_port: u16,

    #[arg(long, default_value = "8124")]
    adb_mysql_port: u16,

    #[arg(long, default_value = "7070")]
    lindorm_port: u16,

    #[arg(long, default_value = "9032")]
    vector_port: u16,

    #[arg(long, default_value = "5000")]
    sybase_tds_port: u16,

    /// Development mode: use insecure default credentials (root with empty
    /// password) instead of generating a random root password. Intended for
    /// local development and the integration test suite only. Never enable in
    /// production.
    #[arg(long, default_value_t = false)]
    dev: bool,
}

impl QueryHandler for HarnessQueryHandler {
    fn handle_query(&self, conn_id: u32, sql: &str) -> QueryResult {
        tracing::info!("handle_query received SQL: {:?}", sql);
        let trimmed = sql.trim().trim_end_matches(';');
        if trimmed.is_empty() {
            return QueryResult::ok();
        }

        let start = Instant::now();

        // Track query
        self.connection_tracker.record_query();
        self.connection_tracker.query_start();

        let result = if is_dml_sql(trimmed) {
            let upper = trimmed.to_uppercase();
            if upper.starts_with("INSERT") {
                // Fall through to parse_sql path
                self.dispatch_parsed(conn_id, trimmed)
            } else if upper.starts_with("DELETE") || upper.starts_with("UPDATE") {
                // DELETE and UPDATE also fall through to parse_sql path
                self.dispatch_parsed(conn_id, trimmed)
            } else {
                // Other DML (like SELECT) goes to DataFusion
                // Create a per-query context with the correct default schema for this connection
                let current_db = self.get_session(conn_id);

                let datafusion_start = Instant::now();
                let result = self.run_datafusion({
                    let catalog = self.catalog.clone();
                    let storage = self.storage.clone();
                    let sql = trimmed.to_string();
                    let rt = self.tokio_runtime.clone();
                    move || {
                        rt.block_on(async {
                            let df_catalog =
                                Arc::new(ParquetCatalogProvider::new(catalog, storage));
                            let df_config = SessionConfig::new()
                                .with_default_catalog_and_schema("harness", &current_db)
                                .with_create_default_catalog_and_schema(false)
                                .with_information_schema(false); // Use custom information_schema from ParquetCatalogProvider
                            let mut ctx = SessionContext::new_with_config(df_config);
                            ctx.register_catalog("harness", df_catalog);
                            register_doris_udfs(&mut ctx);
                            register_misc_udfs(&mut ctx);
                            fe_datafusion::register_date_udfs(&mut ctx);
                            let df = ctx.sql(&sql).await.map_err(|e| e.to_string())?;
                            let schema = df.schema().clone();
                            let batches = df.collect().await.map_err(|e| e.to_string())?;
                            Ok::<_, String>((batches, schema))
                        })
                    }
                });
                let datafusion_ms = datafusion_start.elapsed().as_millis();

                match result {
                    Ok((batches, df_schema)) => {
                        let convert_start = Instant::now();
                        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                        // Build result with `rows` populated (needed by PG protocol)
                        let mut query_result =
                            record_batches_to_query_result_with_df_schema(&batches, &df_schema);
                        // Also populate pre_encoded_rows for MySQL protocol fast path
                        let encoded_rows = encode_arrow_batches_to_mysql_rows(&batches);
                        query_result.pre_encoded_rows = Some(encoded_rows);
                        query_result.pre_encoded_row_count = total_rows;
                        let convert_ms = convert_start.elapsed().as_millis();
                        tracing::info!(
                            "Query timing: DataFusion={}ms, Arrow->MySQL={}ms, rows={}",
                            datafusion_ms, convert_ms, total_rows
                        );
                        query_result
                    }
                    Err(e) => {
                        tracing::error!("DataFusion error: {}", e);
                        QueryResult::with_rows(
                            vec![ColumnDef {
                                name: "Error".to_string(),
                                col_type: ColumnType::String,
                            }],
                            vec![vec![Some(format!("ERROR: {}", e))]],
                        )
                    }
                }
            }
        } else {
            self.dispatch_parsed(conn_id, trimmed)
        };

        let duration_ms = start.elapsed().as_millis() as u64;
        self.connection_tracker.query_end();

        // Audit logging (fire and forget)
        let has_error = result.columns.iter().any(|c| c.name == "Error");
        let query_type = QueryType::from_sql(trimmed);
        let status = if has_error {
            QueryStatus::Failed
        } else {
            QueryStatus::Success
        };
        let error_msg = if has_error {
            result
                .rows
                .first()
                .and_then(|r| r.first())
                .and_then(|v| v.clone())
        } else {
            None
        };

        // Check slow query
        let slow_threshold = self
            .sys_vars
            .get("slow_query_threshold", None)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1000);
        let is_slow = duration_ms >= slow_threshold;
        if is_slow {
            self.connection_tracker.record_slow_query();
        }

        // Record Prometheus metrics
        crate::metrics::record_query(trimmed, duration_ms as f64, is_slow, has_error);

        let audit_enabled = self
            .sys_vars
            .get("enable_audit_log", None)
            .map(|v| v == "true" || v == "1")
            .unwrap_or(true);

        if audit_enabled {
            let slow_only = self
                .sys_vars
                .get("audit_log_slow_only", None)
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false);

            if !slow_only || duration_ms >= slow_threshold {
                let entry = AuditLogEntry {
                    timestamp: chrono::Utc::now(),
                    user: "root".to_string(),
                    host: "127.0.0.1".to_string(),
                    database: Some(self.get_session(conn_id)),
                    query: trimmed.to_string(),
                    query_type,
                    status,
                    duration_ms,
                    rows_affected: None,
                    bytes_scanned: None,
                    error_message: error_msg,
                };
                let audit = self.audit_logger.clone();
                tokio::spawn(async move {
                    audit.log_entry(entry).await;
                });
            }
        }

        result
    }

    fn set_database(&self, conn_id: u32, db: &str) {
        self.set_current_database(conn_id, db.to_string());
        // No longer modify shared session_ctx - per-query contexts use get_session(conn_id)
    }

    fn on_connect(&self, conn_id: u32, user: &str, host: &str) {
        let db = self.get_session(conn_id);
        self.connection_tracker.register(conn_id, user, host, db);
    }

    fn on_disconnect(&self, conn_id: u32) {
        self.connection_tracker.unregister(conn_id);
        self.remove_session(conn_id);
    }
}

impl HarnessQueryHandler {
    fn dispatch_parsed(&self, conn_id: u32, trimmed: &str) -> QueryResult {
        match parse_sql(trimmed) {
            Ok(statements) => {
                if statements.is_empty() {
                    return QueryResult::ok();
                }
                // Execute all statements, return the last non-OK result or the final result
                let mut last_result = QueryResult::ok();
                for stmt in &statements {
                    match self.execute_statement(conn_id, stmt) {
                        Ok(result) => {
                            // If this statement produced a result set (rows), return it immediately
                            // This handles multi-statement queries where a SELECT is the last statement
                            if !result.columns.is_empty() {
                                return result;
                            }
                            last_result = result;
                        }
                        Err(e) => {
                            tracing::error!("Query error: {}", e);
                            return QueryResult::with_rows(
                                vec![ColumnDef {
                                    name: "Error".to_string(),
                                    col_type: ColumnType::String,
                                }],
                                vec![vec![Some(format!("ERROR: {}", e))]],
                            );
                        }
                    }
                }
                last_result
            }
            Err(e) => {
                tracing::error!("Parse error: {}", e);
                QueryResult::with_rows(
                    vec![ColumnDef {
                        name: "Error".to_string(),
                        col_type: ColumnType::String,
                    }],
                    vec![vec![Some(format!("PARSE ERROR: {}", e))]],
                )
            }
        }
    }
}

/// Generate a random hex secret of `bytes` random bytes (2*`bytes` hex chars).
///
/// Uses process entropy (time + pid) hashed through a simple xorshift mix,
/// matching the pattern used by `mysql_protocol::auth::secure_credentials` for
/// non-cryptographic but unpredictable startup secrets. Suitable for default
/// credentials that an operator is expected to override via env vars.
fn generate_random_secret(bytes: usize) -> String {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0) ^ (std::process::id() as u128);
    // xorshift64* style mix to spread entropy across the hex output.
    let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut out = String::with_capacity(bytes * 2);
    while out.len() < bytes * 2 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!("{:016x}", state));
    }
    out.truncate(bytes * 2);
    out
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    tracing::info!("HarnessDB starting...");

    // Load configuration
    let mut config = HarnessConfig::load_or_default(&args.config_file);
    config.apply_cli_overrides(
        Some(args.mysql_port),
        Some(args.data_dir.clone()),
        Some(args.meta_dir.clone()),
    );
    tracing::info!(
        "Config loaded: mysql_port={}, maxcompute_port={}, hologres_port={}, data_dir={}, http_port={}",
        config.server.mysql_port,
        args.maxcompute_port,
        args.hologres_port,
        config.storage.data_dir,
        config.server.http_port
    );

    // Initialize system variables
    let sys_vars = Arc::new(SystemVariableManager::new());
    // Apply config values to system variables
    let _ = sys_vars.set_global(
        "max_connections",
        &config.server.max_connections.to_string(),
    );
    let _ = sys_vars.set_global("wait_timeout", &config.server.wait_timeout.to_string());
    let _ = sys_vars.set_global("http_port", &config.server.http_port.to_string());
    let _ = sys_vars.set_global("storage_compression", &config.storage.compression);
    let _ = sys_vars.set_global(
        "enable_audit_log",
        &config.logging.enable_audit_log.to_string(),
    );
    let _ = sys_vars.set_global(
        "slow_query_threshold",
        &config.logging.slow_query_threshold_ms.to_string(),
    );
    let _ = sys_vars.set_global(
        "audit_log_slow_only",
        &config.logging.audit_log_slow_only.to_string(),
    );
    let _ = sys_vars.set_global("query_timeout", &config.query.query_timeout.to_string());
    let _ = sys_vars.set_global(
        "max_allowed_packet",
        &config.query.max_allowed_packet.to_string(),
    );
    let _ = sys_vars.set_global("sql_mode", &config.query.sql_mode);
    let _ = sys_vars.set_global("time_zone", &config.query.time_zone);
    let _ = sys_vars.set_global("max_dml_rows", &config.query.max_dml_rows.to_string());
    let _ = sys_vars.set_global(
        "max_concurrent_queries",
        &config.query.max_concurrent_queries.to_string(),
    );

    let catalog = Arc::new(CatalogManager::with_path(&args.meta_dir));
    {
        catalog.load()?;
        tracing::info!("Catalog loaded from disk");
    }

    let edit_log = Arc::new(RwLock::new(EditLog::new(&args.meta_dir)));
    {
        let mut log = edit_log.write().await;
        log.replay().await?;
        tracing::info!(
            "Edit log replayed, last_applied_index: {}",
            log.last_applied_index()
        );
    }

    {
        let log = edit_log.read().await;
        catalog.replay_edit_log(&log)?;
        tracing::info!("Edit log applied to catalog");
    }

    let _monitoring = Arc::new(MonitoringManager::new());

    // Create audit logger with config
    let audit_config = AuditLogConfig {
        enabled: config.logging.enable_audit_log,
        log_dir: std::path::PathBuf::from(&config.logging.audit_log_dir),
        max_file_size_mb: config.logging.audit_log_max_size_mb as usize,
        max_files: config.logging.audit_log_max_files as usize,
        log_queries: true,
        log_slow_queries_only: config.logging.audit_log_slow_only,
        slow_query_threshold_ms: config.logging.slow_query_threshold_ms,
    };
    let audit_logger = Arc::new(AuditLogger::with_config(audit_config));

    // Create connection tracker
    let connection_tracker = Arc::new(ConnectionTracker::new());

    // Create backup manager
    let backup_manager = Arc::new(BackupManager::new(&args.meta_dir, &config.storage.data_dir));

    // Create MySQL credentials (shared between auth and DDL handler).
    // In --dev mode (or when HARNESS_ROOT_PASSWORD is explicitly empty), use the
    // insecure default credentials (root with empty password) for local dev and
    // the integration test suite. Otherwise generate a random root password.
    let mysql_credentials = if args.dev {
        tracing::warn!(
            "Running in --dev mode: MySQL root user accepts an empty password. \
             Never use this in production."
        );
        default_credentials()
    } else {
        secure_credentials()
    };

    let query_handler = Arc::new(HarnessQueryHandler::new(
        catalog.clone(),
        config.clone(),
        sys_vars,
        audit_logger.clone(),
        connection_tracker.clone(),
        backup_manager,
        mysql_credentials.clone(),
    ));

    let mysql_config = ServerConfig {
        bind_addr: config.server.bind_addr.clone(),
        port: config.server.mysql_port,
        default_auth_plugin: AuthPluginType::NativePassword,
        auth_timeout_secs: 30,
        max_connections: config.server.max_connections,
        credentials: mysql_credentials.clone(),
    };
    let mysql_server = MysqlServer::new(mysql_config, query_handler.clone());

    let shutdown = Arc::new(AtomicBool::new(false));
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    tracing::info!(
        "HarnessDB starting MySQL server on port {} (may fail silently if port is in use)",
        config.server.mysql_port
    );
    handles.push(tokio::spawn(async move {
        match mysql_server.run().await {
            Ok(()) => tracing::info!("MySQL server stopped"),
            Err(e) => tracing::error!("MySQL server failed: {}", e),
        }
    }));

    // Start MaxCompute protocol server (HTTP/REST)
    tracing::info!(
        "HarnessDB starting MaxCompute server on port {} (may fail silently if port is in use)",
        args.maxcompute_port
    );
    let mc_access_key_id = std::env::var("HARNESS_MC_ACCESS_KEY_ID")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            // Do not fall back to a well-known default credential — generate a
            // random key and warn, mirroring secure_credentials() for MySQL.
            let generated = generate_random_secret(24);
            tracing::warn!(
                "HARNESS_MC_ACCESS_KEY_ID not set — generated random access key id: {} \
                 Set HARNESS_MC_ACCESS_KEY_ID for stable credentials.",
                generated
            );
            generated
        });
    let mc_access_key_secret = std::env::var("HARNESS_MC_ACCESS_KEY_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            let generated = generate_random_secret(32);
            tracing::warn!(
                "HARNESS_MC_ACCESS_KEY_SECRET not set — generated random access key secret. \
                 Set HARNESS_MC_ACCESS_KEY_SECRET for stable credentials."
            );
            generated
        });
    let mc_config = McServerConfig {
        bind_addr: config.server.bind_addr.clone(),
        port: args.maxcompute_port,
        access_key_id: mc_access_key_id,
        access_key_secret: mc_access_key_secret,
        default_project: "default".to_string(),
        region: None,
    };
    let mc_handler = query_handler.clone();
    handles.push(tokio::spawn(async move {
        if let Err(e) = start_mc_server(mc_handler, mc_config).await {
            tracing::error!("MaxCompute server failed: {}", e);
        }
    }));

    // Start Hologres protocol server (PostgreSQL wire protocol)
    tracing::info!(
        "HarnessDB starting Hologres server on port {} (may fail silently if port is in use)",
        args.hologres_port
    );
    let pg_username = std::env::var("HARNESS_PG_USERNAME")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "harness".to_string());
    let pg_password = std::env::var("HARNESS_PG_PASSWORD")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            // Generate a random password instead of hardcoding a well-known
            // default. The operator must set HARNESS_PG_PASSWORD to log in.
            let generated = generate_random_secret(24);
            tracing::warn!(
                "HARNESS_PG_PASSWORD not set — generated random PostgreSQL password: {} \
                 Set HARNESS_PG_PASSWORD for stable credentials.",
                generated
            );
            generated
        });
    let pg_config = PgServerConfig {
        bind_addr: config.server.bind_addr.clone(),
        port: args.hologres_port,
        max_connections: config.server.max_connections,
        username: pg_username,
        password: pg_password,
        accept_any_password: false,
    };
    let pg_server = PgServer::new(pg_config, query_handler.clone());
    handles.push(tokio::spawn(async move {
        if let Err(e) = pg_server.run().await {
            tracing::error!("Hologres (PG) server failed: {}", e);
        }
    }));

    let edit_log_clone = edit_log.clone();
    let shutdown_for_edit = shutdown.clone();
    handles.push(tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(10));
        loop {
            ticker.tick().await;
            if shutdown_for_edit.load(Ordering::Relaxed) {
                break;
            }
            let mut log = edit_log_clone.write().await;
            if let Err(e) = log.flush().await {
                tracing::error!("EditLog flush failed: {}", e);
            } else {
                tracing::debug!("EditLog flushed");
            }
        }
    }));

    tracing::info!(
        "HarnessDB servers started: MySQL={}, MaxCompute={}, Hologres={}",
        config.server.mysql_port,
        args.maxcompute_port,
        args.hologres_port
    );

    // ============================================================
    // Start all 14 protocol servers
    // ============================================================

    // Redis protocol (RESP2/RESP3)
    let redis_port = if args.enable_all_protocols { args.redis_port } else { args.redis_port };
    if redis_port > 0 {
        let redis_password = std::env::var("HARNESS_REDIS_PASSWORD").ok().filter(|p| !p.is_empty());
        if redis_password.is_some() {
            tracing::info!("HarnessDB starting Redis server on port {} with password auth", redis_port);
        } else {
            tracing::warn!("HARNESS_REDIS_PASSWORD not set — Redis server running without authentication");
        }
        let redis_config = RedisServerConfig { port: redis_port, password: redis_password, num_databases: 16 };
        let redis_server = RedisServer::new(redis_config);
        handles.push(tokio::spawn(async move {
            if let Err(e) = redis_server.start().await {
                tracing::error!("Redis server failed: {}", e);
            }
        }));
    }

    // MongoDB protocol (OP_MSG/BSON wire protocol)
    if args.mongodb_port > 0 || args.enable_all_protocols {
        let mongo_port = if args.enable_all_protocols && args.mongodb_port == 0 { 27017 } else { args.mongodb_port };
        if mongo_port > 0 {
            tracing::info!("HarnessDB starting MongoDB server on port {} (may fail silently if port is in use)", mongo_port);
            let mongo_config = MongoDBServerConfig { port: mongo_port };
            let mongo_server = MongoDBServer::new(mongo_config);
            handles.push(tokio::spawn(async move {
                if let Err(e) = mongo_server.start().await {
                    tracing::error!("MongoDB server failed: {}", e);
                }
            }));
        }
    }

    // ClickHouse protocol (HTTP)
    if args.clickhouse_port > 0 || args.enable_all_protocols {
        let ch_port = if args.enable_all_protocols && args.clickhouse_port == 0 { 8123 } else { args.clickhouse_port };
        if ch_port > 0 {
            tracing::info!("HarnessDB starting ClickHouse server on port {} (may fail silently if port is in use)", ch_port);
            let ch_config = ClickHouseServerConfig { port: ch_port };
            let ch_server = ClickHouseServer::new(ch_config);
            handles.push(tokio::spawn(async move {
                if let Err(e) = ch_server.start().await {
                    tracing::error!("ClickHouse server failed: {}", e);
                }
            }));
        }
    }

    // Elasticsearch protocol (HTTP REST)
    if args.elasticsearch_port > 0 || args.enable_all_protocols {
        let es_port = if args.enable_all_protocols && args.elasticsearch_port == 0 { 9200 } else { args.elasticsearch_port };
        if es_port > 0 {
            tracing::info!("HarnessDB starting Elasticsearch server on port {} (may fail silently if port is in use)", es_port);
            let es_config = ElasticsearchServerConfig { port: es_port };
            let es_server = ElasticsearchServer::new(es_config);
            handles.push(tokio::spawn(async move {
                if let Err(e) = es_server.start().await {
                    tracing::error!("Elasticsearch server failed: {}", e);
                }
            }));
        }
    }

    // InfluxDB protocol (HTTP)
    if args.influxdb_port > 0 || args.enable_all_protocols {
        let influx_port = if args.enable_all_protocols && args.influxdb_port == 0 { 8086 } else { args.influxdb_port };
        if influx_port > 0 {
            tracing::info!("HarnessDB starting InfluxDB server on port {} (may fail silently if port is in use)", influx_port);
            let influx_config = InfluxDBServerConfig { port: influx_port };
            let influx_server = InfluxDBServer::new(influx_config);
            handles.push(tokio::spawn(async move {
                if let Err(e) = influx_server.start().await {
                    tracing::error!("InfluxDB server failed: {}", e);
                }
            }));
        }
    }

    // Cassandra protocol (CQL native v4)
    if args.cassandra_port > 0 || args.enable_all_protocols {
        let cass_port = if args.enable_all_protocols && args.cassandra_port == 0 { 9042 } else { args.cassandra_port };
        if cass_port > 0 {
            tracing::info!("HarnessDB starting Cassandra server on port {} (may fail silently if port is in use)", cass_port);
            let cass_config = CassandraServerConfig { port: cass_port };
            let cass_server = CassandraServer::new(cass_config);
            handles.push(tokio::spawn(async move {
                if let Err(e) = cass_server.start().await {
                    tracing::error!("Cassandra server failed: {}", e);
                }
            }));
        }
    }

    // Oracle protocol (TNS simulation)
    if args.oracle_port > 0 || args.enable_all_protocols {
        let ora_port = if args.enable_all_protocols && args.oracle_port == 0 { 1521 } else { args.oracle_port };
        if ora_port > 0 {
            tracing::info!("HarnessDB starting Oracle server on port {} (may fail silently if port is in use)", ora_port);
            let ora_config = OracleServerConfig { port: ora_port };
            let ora_server = OracleServer::new(ora_config);
            handles.push(tokio::spawn(async move {
                if let Err(e) = ora_server.start().await {
                    tracing::error!("Oracle server failed: {}", e);
                }
            }));
        }
    }

    // TableStore protocol (HTTP REST)
    if args.tablestore_port > 0 || args.enable_all_protocols {
        let ts_port = if args.enable_all_protocols && args.tablestore_port == 0 { 8087 } else { args.tablestore_port };
        if ts_port > 0 {
            tracing::info!("HarnessDB starting TableStore server on port {} (may fail silently if port is in use)", ts_port);
            let ts_config = TableStoreServerConfig { port: ts_port };
            let ts_server = TableStoreServer::new(ts_config);
            handles.push(tokio::spawn(async move {
                if let Err(e) = ts_server.start().await {
                    tracing::error!("TableStore server failed: {}", e);
                }
            }));
        }
    }

    // AnalyticDB MySQL protocol
    if args.adb_mysql_port > 0 || args.enable_all_protocols {
        let adb_port = if args.enable_all_protocols && args.adb_mysql_port == 0 { 8124 } else { args.adb_mysql_port };
        if adb_port > 0 {
            tracing::info!("HarnessDB starting AnalyticDB MySQL server on port {} (may fail silently if port is in use)", adb_port);
            let adb_server = AdbMysqlServer::new(adb_port);
            handles.push(tokio::spawn(async move {
                if let Err(e) = adb_server.start().await {
                    tracing::error!("AnalyticDB MySQL server failed: {}", e);
                }
            }));
        }
    }

    // Lindorm protocol (HBase-compatible)
    if args.lindorm_port > 0 || args.enable_all_protocols {
        let lin_port = if args.enable_all_protocols && args.lindorm_port == 0 { 7070 } else { args.lindorm_port };
        if lin_port > 0 {
            tracing::info!("HarnessDB starting Lindorm server on port {} (may fail silently if port is in use)", lin_port);
            let lin_server = LindormServer::new(lin_port);
            handles.push(tokio::spawn(async move {
                if let Err(e) = lin_server.start().await {
                    tracing::error!("Lindorm server failed: {}", e);
                }
            }));
        }
    }

    // Vector protocol (ANN search)
    if args.vector_port > 0 || args.enable_all_protocols {
        let vec_port = if args.enable_all_protocols && args.vector_port == 0 { 9032 } else { args.vector_port };
        if vec_port > 0 {
            tracing::info!("HarnessDB starting Vector server on port {} (may fail silently if port is in use)", vec_port);
            let vec_server = VectorServer::new(vec_port);
            handles.push(tokio::spawn(async move {
                if let Err(e) = vec_server.start().await {
                    tracing::error!("Vector server failed: {}", e);
                }
            }));
        }
    }

    // Start SAP ASE / Sybase TDS protocol server
    if args.sybase_tds_port > 0 || args.enable_all_protocols {
        let sybase_port = if args.enable_all_protocols && args.sybase_tds_port == 0 { 5000 } else { args.sybase_tds_port };
        if sybase_port > 0 {
            tracing::info!("HarnessDB starting SAP ASE (Sybase) TDS server on port {} (may fail silently if port is in use)", sybase_port);
            let tsql_storage = Arc::new(
                fe_storage::ParquetStorage::open(&config.storage.data_dir)
                    .expect("Failed to open ParquetStorage for TSQL handler")
            );
            let tsql_handler = Arc::new(TsqlQueryHandler::new(
                catalog.clone(),
                tsql_storage,
            ));
            let sybase_config = SybaseServerConfig {
                port: sybase_port,
                ..Default::default()
            };
            let sybase_server = SybaseServer::new(sybase_config, tsql_handler);
            handles.push(tokio::spawn(async move {
                if let Err(e) = sybase_server.run().await {
                    tracing::error!("SAP ASE TDS server failed: {}", e);
                }
            }));
        }
    }

    // Initialize Prometheus server info metric
    crate::metrics::RORIS_SERVER_INFO.set(1.0);

    // Periodic memory usage collection (every 15 seconds)
    let shutdown_for_mem = shutdown.clone();
    handles.push(tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(15));
        loop {
            ticker.tick().await;
            if shutdown_for_mem.load(Ordering::Relaxed) {
                break;
            }
            crate::metrics::update_process_memory();
        }
    }));

    // Start Web SQL Editor if enabled
    if config.server.http_port > 0 {
        let web_state = Arc::new(WebState::new(
            query_handler.clone(),
            connection_tracker.clone(),
        ));
        let http_port = config.server.http_port;
        handles.push(tokio::spawn(async move {
            if let Err(e) = start_web_server(web_state, http_port).await {
                tracing::error!("Web server failed: {}", e);
            }
        }));
        tracing::info!(
            "SQL Editor available at http://127.0.0.1:{}",
            config.server.http_port
        );
    }

    let catalog_clone = catalog.clone();
    let shutdown_for_catalog = shutdown.clone();
    handles.push(tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(30));
        loop {
            ticker.tick().await;
            if shutdown_for_catalog.load(Ordering::Relaxed) {
                // One final save before exiting
                let _ = catalog_clone.save();
                break;
            }
            if let Err(e) = catalog_clone.save() {
                tracing::error!("Catalog save failed: {}", e);
            }
        }
    }));

    tokio::signal::ctrl_c().await?;
    tracing::info!("Harness FE shutting down...");
    shutdown.store(true, Ordering::SeqCst);

    // Wait for all tasks with timeout
    if tokio::time::timeout(Duration::from_secs(10), async {
        for handle in handles {
            let _ = handle.await;
        }
    })
    .await
    .is_ok()
    {
        tracing::info!("All tasks shut down gracefully");
    } else {
        tracing::warn!("Some tasks did not shut down within 10s timeout");
    }

    audit_logger.flush().await;
    tracing::info!("Audit logs flushed");

    catalog.save()?;
    tracing::info!("Catalog saved on shutdown");

    Ok(())
}
