use std::collections::HashMap;
use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use fe_backup::BackupManager;
use fe_catalog::CatalogManager;
use fe_config::{HarnessConfig, SessionVariables, SystemVariableManager};
use fe_datafusion::{register_doris_udfs, register_misc_udfs};
use fe_monitor::audit_log::AuditLogger;
use fe_storage::{ParquetCatalogProvider, ParquetStorage};
use mysql_protocol::QueryResult;
use mysql_protocol::auth::Credentials;
use parking_lot::RwLock as PlRwLock;

use crate::connection_tracker::ConnectionTracker;

/// Per-connection session state
pub(crate) struct SessionState {
    pub(crate) current_database: String,
    pub(crate) session_vars: SessionVariables,
    pub(crate) transaction: SimpleTransactionState,
}

pub(crate) struct HarnessQueryHandler {
    pub(crate) catalog: Arc<CatalogManager>,
    pub(crate) views: Arc<PlRwLock<Vec<ViewInfo>>>,
    pub(crate) session_ctx: SessionContext,
    pub(crate) storage: Arc<ParquetStorage>,
    // Configuration and system variables
    #[allow(dead_code)]
    pub(crate) config: HarnessConfig,
    pub(crate) sys_vars: Arc<SystemVariableManager>,
    // Per-connection session state
    pub(crate) sessions: Arc<PlRwLock<HashMap<u32, SessionState>>>,
    // Operations
    pub(crate) audit_logger: Arc<AuditLogger>,
    pub(crate) connection_tracker: Arc<ConnectionTracker>,
    // Backup
    pub(crate) backup_manager: Arc<BackupManager>,
    // Concurrency control: limits concurrent DataFusion query threads
    pub(crate) query_semaphore: Arc<tokio::sync::Semaphore>,
    // MySQL user credentials (shared with MySQL auth layer)
    pub(crate) mysql_credentials: Credentials,
    // Shared Tokio runtime for DataFusion async operations (avoids per-query Runtime::new())
    pub(crate) tokio_runtime: Arc<tokio::runtime::Runtime>,
}

#[derive(Clone)]
pub(crate) struct ViewInfo {
    pub(crate) database: String,
    pub(crate) name: String,
    pub(crate) query: String,
    #[allow(dead_code)]
    pub(crate) columns: Vec<String>,
}

pub(crate) struct SimpleTransactionState {
    pub(crate) in_transaction: bool,
    pub(crate) isolation_level: String,
    pub(crate) savepoints: Vec<String>,
}

impl SimpleTransactionState {
    pub(crate) fn new() -> Self {
        Self {
            in_transaction: false,
            isolation_level: "REPEATABLE READ".to_string(),
            savepoints: Vec::new(),
        }
    }

    pub(crate) fn begin(&mut self) {
        self.in_transaction = true;
    }

    pub(crate) fn rollback(&mut self) {
        self.savepoints.clear();
    }

    #[allow(dead_code)]
    pub(crate) fn savepoint(&mut self, name: String) -> Result<(), String> {
        self.savepoints.push(name);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), String> {
        if self.savepoints.contains(&name.to_string()) {
            Ok(())
        } else {
            Err(format!("Savepoint '{}' not found", name))
        }
    }

    #[allow(dead_code)]
    pub(crate) fn release_savepoint(&mut self, name: &str) -> Result<(), String> {
        self.savepoints.retain(|s| s != name);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn set_isolation_level(&mut self, level: String) {
        self.isolation_level = level;
    }
}

impl HarnessQueryHandler {
    pub(crate) fn new(
        catalog: Arc<CatalogManager>,
        config: HarnessConfig,
        sys_vars: Arc<SystemVariableManager>,
        audit_logger: Arc<AuditLogger>,
        connection_tracker: Arc<ConnectionTracker>,
        backup_manager: Arc<BackupManager>,
        mysql_credentials: Credentials,
    ) -> Self {
        let storage = Arc::new(
            ParquetStorage::open(&config.storage.data_dir)
                .expect("Failed to open ParquetStorage — check data_dir permissions and path")
                .with_max_rows(config.query.max_dml_rows),
        );
        let df_catalog = Arc::new(ParquetCatalogProvider::new(
            catalog.clone(),
            storage.clone(),
        ));
        let df_config = SessionConfig::new()
            .with_default_catalog_and_schema("harness", "information_schema")
            .with_create_default_catalog_and_schema(false)
            .with_information_schema(false); // Use custom information_schema from ParquetCatalogProvider
        let mut session_ctx = SessionContext::new_with_config(df_config);
        session_ctx.register_catalog("harness", df_catalog);
        register_doris_udfs(&mut session_ctx);
        register_misc_udfs(&mut session_ctx);
        fe_datafusion::register_date_udfs(&mut session_ctx);

        let max_concurrent = config.query.max_concurrent_queries.max(1) as usize;

        // Create a single shared Tokio runtime for all DataFusion queries.
        // This avoids the overhead and resource exhaustion of creating a new
        // runtime per query (each Runtime::new() creates its own thread pool).
        let tokio_runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(max_concurrent.max(2))
                .thread_name("harness-df-worker")
                .build()
                .expect("Failed to create shared Tokio runtime"),
        );

        Self {
            catalog,
            views: Arc::new(PlRwLock::new(Vec::new())),
            session_ctx,
            storage,
            config,
            sys_vars,
            sessions: Arc::new(PlRwLock::new(HashMap::new())),
            audit_logger,
            connection_tracker,
            backup_manager,
            query_semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
            mysql_credentials,
            tokio_runtime,
        }
    }

    /// Get session state for a connection, creating default if not exists
    pub(crate) fn get_session(&self, conn_id: u32) -> String {
        let sessions = self.sessions.read();
        sessions
            .get(&conn_id)
            .map(|s| s.current_database.clone())
            .unwrap_or_else(|| "information_schema".to_string())
    }

    /// Set current database for a connection
    pub(crate) fn set_current_database(&self, conn_id: u32, db: String) {
        let mut sessions = self.sessions.write();
        let session = sessions.entry(conn_id).or_insert_with(|| SessionState {
            current_database: db.clone(),
            session_vars: self.sys_vars.create_session(),
            transaction: SimpleTransactionState::new(),
        });
        session.current_database = db;
    }

    /// Get mutable access to session state
    pub(crate) fn with_session_mut<F, R>(&self, conn_id: u32, f: F) -> R
    where
        F: FnOnce(&mut SessionState) -> R,
    {
        let mut sessions = self.sessions.write();
        let session = sessions.entry(conn_id).or_insert_with(|| SessionState {
            current_database: "information_schema".to_string(),
            session_vars: self.sys_vars.create_session(),
            transaction: SimpleTransactionState::new(),
        });
        f(session)
    }

    /// Remove session state for a connection
    pub(crate) fn remove_session(&self, conn_id: u32) {
        let mut sessions = self.sessions.write();
        let removed = sessions.remove(&conn_id);
        if let Some(session) = removed {
            tracing::info!("Removed session for conn_id={}, database was '{}'", conn_id, session.current_database);
        } else {
            tracing::warn!("No session found for conn_id={} during removal", conn_id);
        }
    }

    /// Execute a DataFusion async operation in a spawned thread with concurrency limit.
    ///
    /// Acquires a query semaphore permit before spawning. If the semaphore is
    /// exhausted, returns an error with a retry message. The spawned thread
    /// creates its own Tokio runtime (needed because DataFusion operations
    /// are async and this trait method is sync).
    pub(crate) fn run_datafusion<F, T>(&self, f: F) -> Result<T, String>
    where
        F: FnOnce() -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        // Try to acquire a query semaphore permit
        let permit = self
            .query_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                self.connection_tracker.record_rejected_query();
                "Too many concurrent queries. Please reduce query load and retry.".to_string()
            })?;

        // Spawn the blocking work in a new thread (DataFusion needs its own tokio runtime).
        // The semaphore permit is held for the duration of the spawned thread,
        // preventing thread explosion under high concurrency.
        match std::thread::spawn(move || {
            let _permit = permit; // Hold permit for this thread's lifetime
            f()
        })
        .join()
        {
            Ok(result) => result,
            Err(_) => {
                tracing::error!("DataFusion query thread panicked");
                Err("Internal error: query thread panicked".to_string())
            }
        }
    }

    /// Transaction operations
    pub(crate) fn begin_transaction(&self, conn_id: u32) {
        self.with_session_mut(conn_id, |s| s.transaction.begin());
    }

    pub(crate) fn commit_transaction(&self, conn_id: u32) {
        self.with_session_mut(conn_id, |s| {
            s.transaction.in_transaction = false;
            s.transaction.savepoints.clear();
        });
    }

    pub(crate) fn rollback_transaction(&self, conn_id: u32) {
        self.with_session_mut(conn_id, |s| s.transaction.rollback());
    }

    #[allow(dead_code)]
    pub(crate) fn savepoint(&self, conn_id: u32, name: String) -> Result<(), String> {
        self.with_session_mut(conn_id, |s| s.transaction.savepoint(name))
    }

    #[allow(dead_code)]
    pub(crate) fn rollback_to_savepoint(&self, conn_id: u32, name: &str) -> Result<(), String> {
        self.with_session_mut(conn_id, |s| s.transaction.rollback_to_savepoint(name))
    }

    #[allow(dead_code)]
    pub(crate) fn release_savepoint(&self, conn_id: u32, name: &str) -> Result<(), String> {
        self.with_session_mut(conn_id, |s| s.transaction.release_savepoint(name))
    }

    #[allow(dead_code)]
    pub(crate) fn set_isolation_level(&self, conn_id: u32, level: String) {
        self.with_session_mut(conn_id, |s| s.transaction.set_isolation_level(level));
    }

    pub(crate) fn in_transaction(&self, conn_id: u32) -> bool {
        self.with_session_mut(conn_id, |s| s.transaction.in_transaction)
    }

    pub(crate) fn find_view(&self, db: &str, name: &str) -> Option<ViewInfo> {
        let views = self.views.read();
        views
            .iter()
            .find(|v| v.database == db && v.name == name)
            .cloned()
    }

    #[allow(dead_code)]
    pub(crate) fn update_df_table_schema(
        &self,
        db: &str,
        table: &str,
        arrow_schema: &datafusion::arrow::datatypes::Schema,
    ) -> QueryResult {
        let arrow_schema = Arc::new(arrow_schema.clone());
        if let Err(e) = self.storage.truncate(db, table, arrow_schema) {
            return QueryResult::with_rows(
                vec![mysql_protocol::server::ColumnDef {
                    name: "Error".to_string(),
                    col_type: mysql_protocol::server::ColumnType::String,
                }],
                vec![vec![Some(format!("Failed to update table schema: {}", e))]],
            );
        }
        QueryResult::ok()
    }
}
