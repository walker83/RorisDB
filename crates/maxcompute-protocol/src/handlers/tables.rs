//! Handlers for table listing, detail, and deletion endpoints.
//!
//! - `GET    /api/projects/{project}/tables`       → list_tables
//! - `GET    /api/projects/{project}/tables/{table}` → get_table
//! - `DELETE /api/projects/{project}/tables/{table}` → delete_table
//!
//! All delegate to the `QueryHandler` and format results as MaxCompute XML.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::info;

use crate::XmlResponse;
use crate::server::McServerState;

/// Validate that a string is a valid SQL identifier (prevents SQL injection).
///
/// Valid identifiers consist of letters, digits, and underscores, and
/// must start with a letter or underscore.
fn validate_sql_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let mut chars = s.chars();
    // First character must be a letter or underscore
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    // Remaining characters must be letters, digits, or underscores
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Optional query parameters for `list_tables`.
#[derive(Debug, Deserialize)]
pub struct ListTablesParams {
    #[serde(rename = "maxitem")]
    pub max_item: Option<u32>,
    #[serde(rename = "marker")]
    pub marker: Option<String>,
    #[serde(rename = "prefix")]
    pub prefix: Option<String>,
}

/// List all tables in the given project (database).
///
/// Uses `SHOW TABLES FROM <project>` internally.
/// Supports `prefix`, `marker`, and `maxitem` pagination parameters.
pub async fn list_tables(
    State(state): State<Arc<McServerState>>,
    Path(project): Path<String>,
    Query(params): Query<ListTablesParams>,
) -> impl IntoResponse {
    info!("GET /api/projects/{}/tables", project);

    // Validate the project.
    if !project.eq_ignore_ascii_case(&state.config.default_project) {
        return (
            StatusCode::NOT_FOUND,
            crate::error_xml("ODPS-0130161", &format!("Project '{}' not found", project)),
        )
            .into_response();
    }

    // Execute SHOW TABLES in a blocking thread
    let handler = state.handler.clone();
    let conn_id = state.next_conn_id();
    let result = tokio::task::spawn_blocking(move || handler.handle_query(conn_id, "SHOW TABLES"))
        .await
        .unwrap_or_else(|join_err| {
            tracing::error!("Blocking task join error: {}", join_err);
            mysql_protocol::server::QueryResult::ok()
        });

    let mut table_names: Vec<String> = result
        .rows
        .iter()
        .map(|r| r.first().and_then(|v| v.clone()).unwrap_or_default())
        .filter(|n| !n.is_empty())
        .collect();

    // Apply prefix filter
    if let Some(ref prefix) = params.prefix {
        if !prefix.is_empty() {
            table_names.retain(|name| name.starts_with(prefix));
        }
    }

    // Skip past marker
    if let Some(ref marker) = params.marker {
        if let Some(pos) = table_names.iter().position(|n| n == marker) {
            table_names.drain(0..=pos);
        }
    }

    // Apply maxitem limit
    if let Some(max_item) = params.max_item {
        table_names.truncate(max_item as usize);
    }

    // Build the MaxCompute Tables XML.
    let tables_xml: String = table_names
        .iter()
        .map(|name| {
            format!(
                r#"  <Table>
    <Name>{name}</Name>
    <Owner>root</Owner>
    <Type>MANAGED_TABLE</Type>
    <Comment />
  </Table>"#,
                name = crate::handlers::projects::escape_xml(name)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Tables>
{tables}
</Tables>"#,
        tables = tables_xml
    );

    (StatusCode::OK, XmlResponse(xml)).into_response()
}

/// Describe a single table in detail.
///
/// Uses `DESCRIBE <table>` internally and maps columns/types to MaxCompute format.
pub async fn get_table(
    State(state): State<Arc<McServerState>>,
    Path((project, table)): Path<(String, String)>,
) -> impl IntoResponse {
    info!("GET /api/projects/{}/tables/{}", project, table);

    // Validate the project.
    if !project.eq_ignore_ascii_case(&state.config.default_project) {
        return (
            StatusCode::NOT_FOUND,
            crate::error_xml("ODPS-0130161", &format!("Project '{}' not found", project)),
        )
            .into_response();
    }

    // Validate table name to prevent SQL injection
    if !validate_sql_identifier(&table) {
        return (
            StatusCode::BAD_REQUEST,
            crate::error_xml("ODPS-0130011", &format!("Invalid table name: '{}'", table)),
        )
            .into_response();
    }

    // Execute DESCRIBE in a blocking thread
    let handler = state.handler.clone();
    let conn_id = state.next_conn_id();
    let table_name = table.clone();
    let describe_result = tokio::task::spawn_blocking(move || {
        handler.handle_query(conn_id, &format!("DESCRIBE `{}`", table_name.replace('`', "``")))
    })
    .await
    .unwrap_or_else(|join_err| {
        tracing::error!("Blocking task join error: {}", join_err);
        mysql_protocol::server::QueryResult::ok()
    });

    if describe_result.columns.is_empty() && describe_result.rows.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            crate::error_xml(
                "ODPS-0130161",
                &format!("Table '{}' not found in project '{}'", table, project),
            ),
        )
            .into_response();
    }

    // Parse DESCRIBE output. Typically columns are: Field, Type, Null, Key, Default, Extra
    // We map MySQL types to MaxCompute types.
    let columns_xml: String = describe_result
        .rows
        .iter()
        .map(|row| {
            let col_name = row.first().and_then(|v| v.as_deref()).unwrap_or("");
            let col_type = row.get(1).and_then(|v| v.as_deref()).unwrap_or("string");
            let mc_type = mysql_type_to_maxcompute(col_type);
            format!(
                r#"    <Column>
      <Name>{name}</Name>
      <Type>{mctype}</Type>
      <Comment />
    </Column>"#,
                name = crate::handlers::projects::escape_xml(col_name),
                mctype = mc_type
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Table>
  <Name>{table}</Name>
  <Owner>root</Owner>
  <Type>MANAGED_TABLE</Type>
  <Columns>
{columns}
  </Columns>
</Table>"#,
        table = crate::handlers::projects::escape_xml(&table),
        columns = columns_xml
    );

    (StatusCode::OK, XmlResponse(xml)).into_response()
}

/// Delete a table.
///
/// Uses `DROP TABLE IF EXISTS <table>` internally.
/// Returns 200 OK even if the table does not exist (DROP TABLE IF EXISTS semantics).
pub async fn delete_table(
    State(state): State<Arc<McServerState>>,
    Path((project, table)): Path<(String, String)>,
) -> impl IntoResponse {
    info!("DELETE /api/projects/{}/tables/{}", project, table);

    // Validate the project.
    if !project.eq_ignore_ascii_case(&state.config.default_project) {
        return (
            StatusCode::NOT_FOUND,
            crate::error_xml("ODPS-0130161", &format!("Project '{}' not found", project)),
        )
            .into_response();
    }

    // Validate table name to prevent SQL injection
    if !validate_sql_identifier(&table) {
        return (
            StatusCode::BAD_REQUEST,
            crate::error_xml("ODPS-0130011", &format!("Invalid table name: '{}'", table)),
        )
            .into_response();
    }

    // Execute DROP TABLE IF EXISTS in a blocking thread
    let handler = state.handler.clone();
    let conn_id = state.next_conn_id();
    let table_name = table.clone();
    let _result = tokio::task::spawn_blocking(move || {
        handler.handle_query(conn_id, &format!("DROP TABLE IF EXISTS `{}`", table_name.replace('`', "``")))
    })
    .await
    .unwrap_or_else(|join_err| {
        tracing::error!("Blocking task join error: {}", join_err);
        mysql_protocol::server::QueryResult::ok()
    });

    // Build a success response. Note: even if the table didn't exist,
    // DROP TABLE IF EXISTS succeeds, so we always return 200.
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Table>
  <Name>{table}</Name>
</Table>"#,
        table = crate::handlers::projects::escape_xml(&table),
    );

    (StatusCode::OK, XmlResponse(xml)).into_response()
}

/// Map a MySQL type name to its MaxCompute equivalent.
fn mysql_type_to_maxcompute(mysql_type: &str) -> &'static str {
    let lower = mysql_type.to_lowercase();
    match lower.as_str() {
        // Numeric
        "tinyint" | "smallint" | "int" | "integer" | "bigint" => "bigint",
        "float" | "double" | "decimal" | "numeric" | "real" => "double",
        // String
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum" | "set" => {
            "string"
        }
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => "binary",
        // Date/Time
        "date" => "datetime",
        "datetime" | "timestamp" => "datetime",
        "time" | "year" => "string",
        // Boolean
        "bool" | "boolean" => "boolean",
        // Default
        _ => "string",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_type_to_maxcompute() {
        assert_eq!(mysql_type_to_maxcompute("int"), "bigint");
        assert_eq!(mysql_type_to_maxcompute("VARCHAR"), "string");
        assert_eq!(mysql_type_to_maxcompute("datetime"), "datetime");
        assert_eq!(mysql_type_to_maxcompute("bool"), "boolean");
        assert_eq!(mysql_type_to_maxcompute("blob"), "binary");
        assert_eq!(mysql_type_to_maxcompute("unknown_type"), "string");
    }

    #[test]
    fn test_validate_sql_identifier_valid() {
        assert!(validate_sql_identifier("my_table"));
        assert!(validate_sql_identifier("_private"));
        assert!(validate_sql_identifier("Table123"));
        assert!(validate_sql_identifier("a"));
    }

    #[test]
    fn test_validate_sql_identifier_invalid() {
        assert!(!validate_sql_identifier(""));
        assert!(!validate_sql_identifier("123abc")); // starts with digit
        assert!(!validate_sql_identifier("table name")); // space
        assert!(!validate_sql_identifier("DROP TABLE")); // space
        assert!(!validate_sql_identifier("a;b")); // semicolon
        assert!(!validate_sql_identifier("a--")); // dash
        assert!(!validate_sql_identifier("' OR '1'='1")); // SQL injection
    }

    #[tokio::test]
    async fn test_list_tables_project_not_found() {
        use crate::server::{McServerConfig, McServerState, MockQueryHandler};
        let state = Arc::new(McServerState::new(
            Arc::new(MockQueryHandler::new()),
            McServerConfig {
                default_project: "myproject".to_string(),
                ..McServerConfig::default()
            },
        ));
        let resp = list_tables(
            State(state),
            Path("wrong".to_string()),
            Query(ListTablesParams {
                max_item: None,
                marker: None,
                prefix: None,
            }),
        )
        .await;
        let resp = resp.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_table_project_not_found() {
        use crate::server::{McServerConfig, McServerState, MockQueryHandler};
        let state = Arc::new(McServerState::new(
            Arc::new(MockQueryHandler::new()),
            McServerConfig {
                default_project: "myproject".to_string(),
                ..McServerConfig::default()
            },
        ));
        let resp = get_table(
            State(state),
            Path(("wrong".to_string(), "mytable".to_string())),
        )
        .await;
        let resp = resp.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_table_invalid_table_name_sql_injection() {
        use crate::server::{McServerConfig, McServerState, MockQueryHandler};
        let state = Arc::new(McServerState::new(
            Arc::new(MockQueryHandler::new()),
            McServerConfig {
                default_project: "myproject".to_string(),
                ..McServerConfig::default()
            },
        ));
        let resp = get_table(
            State(state),
            Path((
                "myproject".to_string(),
                "users; DROP TABLE users".to_string(),
            )),
        )
        .await;
        let resp = resp.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_delete_table_success() {
        use crate::server::{McServerConfig, McServerState, MockQueryHandler};
        let state = Arc::new(McServerState::new(
            Arc::new(MockQueryHandler::new()),
            McServerConfig {
                default_project: "myproject".to_string(),
                ..McServerConfig::default()
            },
        ));
        let resp = delete_table(
            State(state),
            Path(("myproject".to_string(), "test_table".to_string())),
        )
        .await;
        let resp = resp.into_response();
        assert_eq!(resp.status(), StatusCode::OK);

        // Should contain the table name in the response body
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert!(body_str.contains("<Name>test_table</Name>"));
    }

    #[tokio::test]
    async fn test_delete_table_project_not_found() {
        use crate::server::{McServerConfig, McServerState, MockQueryHandler};
        let state = Arc::new(McServerState::new(
            Arc::new(MockQueryHandler::new()),
            McServerConfig {
                default_project: "myproject".to_string(),
                ..McServerConfig::default()
            },
        ));
        let resp = delete_table(
            State(state),
            Path(("wrong".to_string(), "mytable".to_string())),
        )
        .await;
        let resp = resp.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_table_invalid_table_name_sql_injection() {
        use crate::server::{McServerConfig, McServerState, MockQueryHandler};
        let state = Arc::new(McServerState::new(
            Arc::new(MockQueryHandler::new()),
            McServerConfig {
                default_project: "myproject".to_string(),
                ..McServerConfig::default()
            },
        ));
        let resp = delete_table(
            State(state),
            Path((
                "myproject".to_string(),
                "users; DROP TABLE users".to_string(),
            )),
        )
        .await;
        let resp = resp.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_delete_table_nonexistent_table_still_ok() {
        // DROP TABLE IF EXISTS should succeed even if the table doesn't exist
        use crate::server::{McServerConfig, McServerState, MockQueryHandler};
        let state = Arc::new(McServerState::new(
            Arc::new(MockQueryHandler::new()),
            McServerConfig {
                default_project: "myproject".to_string(),
                ..McServerConfig::default()
            },
        ));
        let resp = delete_table(
            State(state),
            Path(("myproject".to_string(), "nonexistent_table".to_string())),
        )
        .await;
        let resp = resp.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
