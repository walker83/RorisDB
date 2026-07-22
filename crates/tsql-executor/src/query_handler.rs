use std::sync::Arc;
use fe_catalog::catalog::CatalogManager;
use fe_storage::ParquetStorage;
use mysql_protocol::server::{QueryHandler, QueryResult};
use tsql_parser::{TsqlParser, split_batches};
use crate::context::ExecutionContext;
use crate::interpreter::{TsqlInterpreter, TsqlExecutionResult};
use crate::transaction::TsqlTransactionManager;
use dashmap::DashMap;

pub struct TsqlQueryHandler {
    catalog: Arc<CatalogManager>,
    storage: Arc<ParquetStorage>,
    interpreter: TsqlInterpreter,
    txn_mgr: Arc<TsqlTransactionManager>,
    sessions: DashMap<u32, String>,  // conn_id -> current_database
}

impl TsqlQueryHandler {
    pub fn new(catalog: Arc<CatalogManager>, storage: Arc<ParquetStorage>) -> Self {
        Self {
            catalog: catalog.clone(),
            storage: storage.clone(),
            interpreter: TsqlInterpreter::new(),
            txn_mgr: Arc::new(TsqlTransactionManager::new()),
            sessions: DashMap::new(),
        }
    }

    fn get_or_create_context(&self, conn_id: u32) -> ExecutionContext {
        let database = self.sessions.get(&conn_id)
            .map(|r| r.value().clone())
            .unwrap_or_else(|| "master".to_string());
        ExecutionContext::new(conn_id, database, self.catalog.clone(), self.storage.clone(), self.txn_mgr.clone())
    }
}

impl QueryHandler for TsqlQueryHandler {
    fn handle_query(&self, conn_id: u32, sql: &str) -> QueryResult {
        let batches = split_batches(sql);
        let mut last_result = QueryResult::ok();

        for batch_sql in &batches {
            let trimmed = batch_sql.trim();
            if trimmed.is_empty() { continue; }

            let mut parser = TsqlParser::new();
            let statements = match parser.parse(trimmed) {
                Ok(stmts) => stmts,
                Err(e) => {
                    tracing::error!("T-SQL parse error: {}", e);
                    return QueryResult::ok();
                }
            };

            let mut ctx = self.get_or_create_context(conn_id);
            match self.interpreter.execute_batch(&mut ctx, &statements) {
                Ok(TsqlExecutionResult::ResultSet(qr)) => {
                    last_result = qr;
                }
                Ok(TsqlExecutionResult::RowsAffected(n)) => {
                    last_result = QueryResult::ok_with_affected(n);
                }
                Ok(TsqlExecutionResult::Message(_)) => {
                    // PRINT messages — in TDS these would be InfoMessage tokens
                }
                Ok(TsqlExecutionResult::ReturnStatus(_)) => {
                    // Return status from EXEC
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("T-SQL execution error: {}", e);
                }
            }

            // Save session state
            self.sessions.insert(conn_id, ctx.current_database);
        }

        last_result
    }

    fn set_database(&self, conn_id: u32, db: &str) {
        self.sessions.insert(conn_id, db.to_string());
    }
}
