//! PostgreSQL system catalog simulation for Hologres compatibility.
//!
//! Many PostgreSQL clients and ORMs query system catalogs (pg_catalog) on
//! connection to discover available tables, columns, types, etc. This module
//! intercepts those queries and returns appropriate mock results so that
//! tools like psql, psycopg2, JDBC, and SQLAlchemy work correctly.
//!
//! # Supported Queries
//!
//! - `SELECT * FROM pg_tables` — list of tables
//! - `SELECT * FROM pg_class` — table metadata
//! - `SELECT * FROM pg_namespace` — schemas
//! - `SELECT * FROM pg_attribute` — column definitions
//! - `SELECT * FROM pg_database` — databases
//! - `SELECT * FROM pg_type` — type definitions
//! - `SELECT * FROM pg_index` — indexes
//! - `SELECT * FROM pg_description` — object descriptions
//! - `SELECT * FROM pg_proc` — functions
//! - `SELECT * FROM pg_trigger` — triggers
//! - `SELECT * FROM pg_enum` — enum types
//! - `SELECT * FROM pg_range` — range types
//! - `SELECT * FROM pg_cast` — type casts
//! - `SELECT * FROM pg_opclass` — operator classes
//! - `SELECT * FROM pg_am` — access methods
//! - `SELECT * FROM pg_extension` — extensions
//! - `SELECT * FROM pg_views` — views
//! - `SELECT * FROM pg_matviews` — materialized views
//! - `SELECT * FROM pg_settings` — runtime settings
//! - `SELECT version()` — version string
//! - `SELECT current_database()` — current database
//! - `SELECT current_schema()` — current schema
//! - `SELECT current_user` / `SELECT current_user()` — current user
//! - `SELECT pg_postmaster_start_time()` — server start time
//! - `SHOW search_path` / `SHOW server_version` etc.
//! - `SET xxx = yyy` — no-op
//! - `SELECT * FROM hg_stat_activity` — Hologres-specific
//! - `SELECT * FROM information_schema.tables`
//! - `SELECT * FROM information_schema.columns`
//! - `SELECT * FROM information_schema.schemata`
//! - `SELECT * FROM information_schema.views`

use fe_catalog::CatalogManager;
use mysql_protocol::server::{ColumnDef, ColumnType, QueryResult};

/// Check if a SQL query is a pg_catalog or information_schema query and return
/// mock results. Returns `Some(QueryResult)` if the query was handled, or
/// `None` if it should be passed through to the normal query handler.
///
/// # Arguments
/// * `sql` - The SQL query to check
/// * `catalog` - The catalog manager for accessing metadata
/// * `current_db` - The current database name
/// * `current_user` - The current user name
/// * `start_time` - Server start time as an ISO string
pub fn handle_pg_catalog_query(
    sql: &str,
    catalog: &CatalogManager,
    current_db: &str,
    current_user: &str,
    start_time: &str,
) -> Option<QueryResult> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    // ======================================================================
    // SET statements — no-op, return OK
    // ======================================================================
    if upper.starts_with("SET ") || upper.starts_with("SET\t") {
        return Some(handle_set_variable(trimmed));
    }

    // ======================================================================
    // SHOW statements
    // ======================================================================
    if upper.starts_with("SHOW ") || upper.starts_with("SHOW\t") {
        return Some(handle_show_variable(trimmed));
    }

    // ======================================================================
    // Scalar function queries (SELECT func())
    // ======================================================================
    // `SELECT version()`
    if trimmed.eq_ignore_ascii_case("SELECT version()")
        || trimmed.eq_ignore_ascii_case("SELECT VERSION()")
    {
        return Some(handle_version());
    }

    // `SELECT current_database()`
    if trimmed.eq_ignore_ascii_case("SELECT current_database()")
        || trimmed.eq_ignore_ascii_case("SELECT CURRENT_DATABASE()")
    {
        return Some(handle_current_database(current_db));
    }

    // `SELECT current_schema()` or `SELECT current_schema`
    if trimmed.eq_ignore_ascii_case("SELECT current_schema()")
        || trimmed.eq_ignore_ascii_case("SELECT CURRENT_SCHEMA()")
        || trimmed.eq_ignore_ascii_case("SELECT current_schema")
        || trimmed.eq_ignore_ascii_case("SELECT CURRENT_SCHEMA")
    {
        return Some(handle_current_schema());
    }

    // `SELECT current_user` or `SELECT current_user()`
    if trimmed.eq_ignore_ascii_case("SELECT current_user")
        || trimmed.eq_ignore_ascii_case("SELECT CURRENT_USER")
        || trimmed.eq_ignore_ascii_case("SELECT current_user()")
        || trimmed.eq_ignore_ascii_case("SELECT CURRENT_USER()")
    {
        return Some(handle_current_user(current_user));
    }

    // `SELECT pg_postmaster_start_time()`
    if trimmed.contains("pg_postmaster_start_time()") {
        return Some(handle_postmaster_start_time(start_time));
    }

    // ======================================================================
    // pg_catalog queries
    // ======================================================================
    if upper.starts_with("SELECT * FROM pg_tables") || upper.starts_with("SELECT * FROM PG_TABLES")
    {
        return Some(handle_pg_tables(catalog, current_db));
    }

    if upper.starts_with("SELECT * FROM pg_class") || upper.starts_with("SELECT * FROM PG_CLASS") {
        return Some(handle_pg_class(catalog, current_db));
    }

    if upper.starts_with("SELECT * FROM pg_namespace")
        || upper.starts_with("SELECT * FROM PG_NAMESPACE")
    {
        return Some(handle_pg_namespace());
    }

    if upper.starts_with("SELECT * FROM pg_attribute")
        || upper.starts_with("SELECT * FROM PG_ATTRIBUTE")
    {
        return Some(handle_pg_attribute(catalog, current_db));
    }

    if upper.starts_with("SELECT * FROM pg_database")
        || upper.starts_with("SELECT * FROM PG_DATABASE")
    {
        return Some(handle_pg_database(catalog, current_db));
    }

    if upper.starts_with("SELECT * FROM pg_type") || upper.starts_with("SELECT * FROM PG_TYPE") {
        return Some(handle_pg_type());
    }

    if upper.starts_with("SELECT * FROM pg_index") || upper.starts_with("SELECT * FROM PG_INDEX") {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_description")
        || upper.starts_with("SELECT * FROM PG_DESCRIPTION")
    {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_proc") || upper.starts_with("SELECT * FROM PG_PROC") {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_trigger")
        || upper.starts_with("SELECT * FROM PG_TRIGGER")
    {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_enum") || upper.starts_with("SELECT * FROM PG_ENUM") {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_range") || upper.starts_with("SELECT * FROM PG_RANGE") {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_cast") || upper.starts_with("SELECT * FROM PG_CAST") {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_opclass")
        || upper.starts_with("SELECT * FROM PG_OPCLASS")
    {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_am") || upper.starts_with("SELECT * FROM PG_AM") {
        return Some(handle_pg_am());
    }

    if upper.starts_with("SELECT * FROM pg_extension")
        || upper.starts_with("SELECT * FROM PG_EXTENSION")
    {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_views") || upper.starts_with("SELECT * FROM PG_VIEWS") {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_matviews")
        || upper.starts_with("SELECT * FROM PG_MATVIEWS")
    {
        return Some(handle_pg_matviews(catalog, current_db));
    }

    if upper.starts_with("SELECT * FROM pg_settings")
        || upper.starts_with("SELECT * FROM PG_SETTINGS")
    {
        return Some(handle_pg_settings());
    }

    if upper.starts_with("SELECT * FROM pg_available_extensions")
        || upper.starts_with("SELECT * FROM PG_AVAILABLE_EXTENSIONS")
    {
        return Some(handle_pg_available_extensions());
    }

    if upper.starts_with("SELECT * FROM pg_timezone_names")
        || upper.starts_with("SELECT * FROM PG_TIMEZONE_NAMES")
    {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_collation")
        || upper.starts_with("SELECT * FROM PG_COLLATION")
    {
        return Some(QueryResult::ok());
    }

    if upper.starts_with("SELECT * FROM pg_foreign_table")
        || upper.starts_with("SELECT * FROM PG_FOREIGN_TABLE")
    {
        return Some(QueryResult::ok());
    }

    // ======================================================================
    // Hologres-specific queries
    // ======================================================================
    if upper.starts_with("SELECT * FROM hg_stat_activity")
        || upper.starts_with("SELECT * FROM HG_STAT_ACTIVITY")
    {
        return Some(handle_hg_stat_activity(current_user));
    }

    // ======================================================================
    // information_schema queries
    // ======================================================================
    if upper.starts_with("SELECT") && upper.contains("INFORMATION_SCHEMA.TABLES") {
        return Some(handle_information_schema_tables(catalog, current_db));
    }

    if upper.starts_with("SELECT") && upper.contains("INFORMATION_SCHEMA.COLUMNS") {
        return Some(handle_information_schema_columns(catalog, current_db));
    }

    if upper.starts_with("SELECT") && upper.contains("INFORMATION_SCHEMA.SCHEMATA") {
        return Some(handle_information_schema_schemata(catalog));
    }

    if upper.starts_with("SELECT") && upper.contains("INFORMATION_SCHEMA.VIEWS") {
        return Some(handle_information_schema_views(catalog, current_db));
    }

    // Not a pg_catalog query
    let _ = catalog;
    let _ = start_time;
    None
}

// ============================================================================
// Individual handler functions
// ============================================================================

/// Handle `SET variable = value` — always a no-op that returns OK.
fn handle_set_variable(sql: &str) -> QueryResult {
    tracing::debug!("PG SET (no-op): {}", sql);
    QueryResult::ok()
}

/// Handle `SHOW variable` — return mock values for common variables.
fn handle_show_variable(sql: &str) -> QueryResult {
    let lower = sql.to_lowercase();

    if lower.contains("search_path") {
        return QueryResult::with_rows(
            vec![ColumnDef {
                name: "search_path".to_string(),
                col_type: ColumnType::String,
            }],
            vec![vec![Some("public".to_string())]],
        );
    }

    if lower.contains("server_version") {
        return QueryResult::with_rows(
            vec![ColumnDef {
                name: "server_version".to_string(),
                col_type: ColumnType::String,
            }],
            vec![vec![Some("15.0".to_string())]],
        );
    }

    if lower.contains("server_encoding") {
        return QueryResult::with_rows(
            vec![ColumnDef {
                name: "server_encoding".to_string(),
                col_type: ColumnType::String,
            }],
            vec![vec![Some("UTF8".to_string())]],
        );
    }

    if lower.contains("client_encoding") {
        return QueryResult::with_rows(
            vec![ColumnDef {
                name: "client_encoding".to_string(),
                col_type: ColumnType::String,
            }],
            vec![vec![Some("UTF8".to_string())]],
        );
    }

    if lower.contains("timezone") || lower.contains("time zone") {
        return QueryResult::with_rows(
            vec![ColumnDef {
                name: "TimeZone".to_string(),
                col_type: ColumnType::String,
            }],
            vec![vec![Some("UTC".to_string())]],
        );
    }

    if lower.contains("datestyle") {
        return QueryResult::with_rows(
            vec![ColumnDef {
                name: "DateStyle".to_string(),
                col_type: ColumnType::String,
            }],
            vec![vec![Some("ISO, MDY".to_string())]],
        );
    }

    if lower.contains("max_connections") {
        return QueryResult::with_rows(
            vec![ColumnDef {
                name: "max_connections".to_string(),
                col_type: ColumnType::String,
            }],
            vec![vec![Some("100".to_string())]],
        );
    }

    if lower.contains("transaction_isolation") {
        return QueryResult::with_rows(
            vec![ColumnDef {
                name: "transaction_isolation".to_string(),
                col_type: ColumnType::String,
            }],
            vec![vec![Some("read committed".to_string())]],
        );
    }

    // Default: return empty string
    QueryResult::with_rows(
        vec![ColumnDef {
            name: "?".to_string(),
            col_type: ColumnType::String,
        }],
        vec![vec![Some(String::new())]],
    )
}

/// Handle `SELECT version()`.
fn handle_version() -> QueryResult {
    QueryResult::with_rows(
        vec![ColumnDef {
            name: "version".to_string(),
            col_type: ColumnType::String,
        }],
        vec![vec![Some("PostgreSQL 15.0 (HarnessDB 1.3.0)".to_string())]],
    )
}

/// Handle `SELECT current_database()`.
fn handle_current_database(current_db: &str) -> QueryResult {
    let db = if current_db.is_empty() {
        "default"
    } else {
        current_db
    };
    QueryResult::with_rows(
        vec![ColumnDef {
            name: "current_database".to_string(),
            col_type: ColumnType::String,
        }],
        vec![vec![Some(db.to_string())]],
    )
}

/// Handle `SELECT current_schema()`.
fn handle_current_schema() -> QueryResult {
    QueryResult::with_rows(
        vec![ColumnDef {
            name: "current_schema".to_string(),
            col_type: ColumnType::String,
        }],
        vec![vec![Some("public".to_string())]],
    )
}

/// Handle `SELECT current_user`.
fn handle_current_user(current_user: &str) -> QueryResult {
    let user = if current_user.is_empty() {
        "root"
    } else {
        current_user
    };
    QueryResult::with_rows(
        vec![ColumnDef {
            name: "current_user".to_string(),
            col_type: ColumnType::String,
        }],
        vec![vec![Some(user.to_string())]],
    )
}

/// Handle `SELECT pg_postmaster_start_time()`.
fn handle_postmaster_start_time(start_time: &str) -> QueryResult {
    QueryResult::with_rows(
        vec![ColumnDef {
            name: "pg_postmaster_start_time".to_string(),
            col_type: ColumnType::DateTime,
        }],
        vec![vec![Some(start_time.to_string())]],
    )
}

/// Handle `SELECT * FROM pg_tables` — list all tables with schema info.
fn handle_pg_tables(catalog: &CatalogManager, current_db: &str) -> QueryResult {
    let db_name = if current_db.is_empty() {
        "default"
    } else {
        current_db
    };

    let columns = vec![
        ColumnDef {
            name: "schemaname".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "tablename".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "tableowner".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "tablespace".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "hasindexes".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "hasrules".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "hastriggers".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "rowsecurity".to_string(),
            col_type: ColumnType::String,
        },
    ];

    let mut rows = Vec::new();

    // Add tables from the current database
    if let Some(tables) = catalog.list_tables(db_name) {
        for table_name in tables {
            rows.push(vec![
                Some("public".to_string()),
                Some(table_name),
                Some("root".to_string()),
                Some(String::new()),
                Some("false".to_string()),
                Some("false".to_string()),
                Some("false".to_string()),
                Some("false".to_string()),
            ]);
        }
    }

    QueryResult::with_rows(columns, rows)
}

/// Handle `SELECT * FROM pg_class` — table metadata.
fn handle_pg_class(catalog: &CatalogManager, current_db: &str) -> QueryResult {
    let db_name = if current_db.is_empty() {
        "default"
    } else {
        current_db
    };

    let columns = vec![
        ColumnDef {
            name: "oid".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "relname".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "relnamespace".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "relkind".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "relowner".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "relam".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "relfilenode".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "reltablespace".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "relpages".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "reltuples".to_string(),
            col_type: ColumnType::Float,
        },
        ColumnDef {
            name: "relallvisible".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "relhasindex".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "relisshared".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "relpersistence".to_string(),
            col_type: ColumnType::String,
        },
    ];

    let mut rows = Vec::new();
    let mut oid: i64 = 10000;

    if let Some(tables) = catalog.list_tables(db_name) {
        for table_name in tables {
            rows.push(vec![
                Some(oid.to_string()),
                Some(table_name),
                Some("2200".to_string()), // pg_namespace OID for public
                Some("r".to_string()),    // ordinary table
                Some("10".to_string()),   // superuser OID
                Some("2".to_string()),    // heap access method
                Some(oid.to_string()),
                Some("0".to_string()),
                Some("0".to_string()),
                Some("0.0".to_string()),
                Some("0".to_string()),
                Some("false".to_string()),
                Some("false".to_string()),
                Some("p".to_string()), // permanent
            ]);
            oid += 1;
        }
    }

    QueryResult::with_rows(columns, rows)
}

/// Handle `SELECT * FROM pg_namespace` — schema list.
fn handle_pg_namespace() -> QueryResult {
    QueryResult::with_rows(
        vec![
            ColumnDef {
                name: "oid".to_string(),
                col_type: ColumnType::Int,
            },
            ColumnDef {
                name: "nspname".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "nspowner".to_string(),
                col_type: ColumnType::Int,
            },
            ColumnDef {
                name: "nspacl".to_string(),
                col_type: ColumnType::String,
            },
        ],
        vec![
            vec![
                Some("11".to_string()),
                Some("pg_catalog".to_string()),
                Some("10".to_string()),
                Some("{admin=admin}".to_string()),
            ],
            vec![
                Some("2200".to_string()),
                Some("public".to_string()),
                Some("10".to_string()),
                Some("{admin=admin}".to_string()),
            ],
            vec![
                Some("99".to_string()),
                Some("information_schema".to_string()),
                Some("10".to_string()),
                Some("{admin=admin}".to_string()),
            ],
        ],
    )
}

/// Handle `SELECT * FROM pg_attribute` — column definitions.
fn handle_pg_attribute(catalog: &CatalogManager, current_db: &str) -> QueryResult {
    let db_name = if current_db.is_empty() {
        "default"
    } else {
        current_db
    };

    let columns = vec![
        ColumnDef {
            name: "attrelid".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "attname".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "atttypid".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "attstattarget".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "attlen".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "attnum".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "attndims".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "attcacheoff".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "atttypmod".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "attbyval".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "attstorage".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "attalign".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "attnotnull".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "atthasdef".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "atthasmissing".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "attidentity".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "attgenerated".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "attisdropped".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "attislocal".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "attinhcount".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "attcollation".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "attacl".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "attoptions".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "attfdwoptions".to_string(),
            col_type: ColumnType::String,
        },
    ];

    let mut rows = Vec::new();
    let mut rel_oid: i64 = 10000;

    if let Some(tables) = catalog.list_tables(db_name) {
        for table_name in tables {
            if let Some(table) = catalog.get_table(db_name, &table_name) {
                for (i, col) in table.columns.iter().enumerate() {
                    let type_oid = map_harness_type_to_pg_oid(&col.data_type);
                    let type_len = map_harness_type_to_len(&col.data_type);
                    let not_null_str = if col.nullable { "false" } else { "true" };

                    rows.push(vec![
                        Some(rel_oid.to_string()),
                        Some(col.name.clone()),
                        Some(type_oid.to_string()),
                        Some("0".to_string()),
                        Some(type_len.to_string()),
                        Some((i + 1).to_string()),
                        Some("0".to_string()),
                        Some("-1".to_string()),
                        Some("-1".to_string()),
                        Some("false".to_string()),
                        Some("x".to_string()),
                        Some("i".to_string()),
                        Some(not_null_str.to_string()),
                        Some("false".to_string()),
                        Some("false".to_string()),
                        Some(String::new()),
                        Some(String::new()),
                        Some("false".to_string()),
                        Some("true".to_string()),
                        Some("0".to_string()),
                        Some("0".to_string()),
                        Some(String::new()),
                        Some(String::new()),
                        Some(String::new()),
                    ]);
                }
            }
            rel_oid += 1;
        }
    }

    QueryResult::with_rows(columns, rows)
}

/// Handle `SELECT * FROM pg_database` — database list.
fn handle_pg_database(catalog: &CatalogManager, _current_db: &str) -> QueryResult {
    let databases = catalog.list_databases();
    let mut rows = Vec::new();

    for db_name in databases {
        rows.push(vec![Some(db_name)]);
    }

    QueryResult::with_rows(
        vec![ColumnDef {
            name: "datname".to_string(),
            col_type: ColumnType::String,
        }],
        rows,
    )
}

/// Handle `SELECT * FROM pg_type` — type definitions.
fn handle_pg_type() -> QueryResult {
    QueryResult::with_rows(
        vec![
            ColumnDef {
                name: "oid".to_string(),
                col_type: ColumnType::Int,
            },
            ColumnDef {
                name: "typname".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "typtype".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "typlen".to_string(),
                col_type: ColumnType::Int,
            },
        ],
        vec![
            vec![
                Some("16".to_string()),
                Some("bool".to_string()),
                Some("b".to_string()),
                Some("1".to_string()),
            ],
            vec![
                Some("17".to_string()),
                Some("bytea".to_string()),
                Some("b".to_string()),
                Some("-1".to_string()),
            ],
            vec![
                Some("19".to_string()),
                Some("name".to_string()),
                Some("b".to_string()),
                Some("64".to_string()),
            ],
            vec![
                Some("20".to_string()),
                Some("int8".to_string()),
                Some("b".to_string()),
                Some("8".to_string()),
            ],
            vec![
                Some("21".to_string()),
                Some("int2".to_string()),
                Some("b".to_string()),
                Some("2".to_string()),
            ],
            vec![
                Some("23".to_string()),
                Some("int4".to_string()),
                Some("b".to_string()),
                Some("4".to_string()),
            ],
            vec![
                Some("25".to_string()),
                Some("text".to_string()),
                Some("b".to_string()),
                Some("-1".to_string()),
            ],
            vec![
                Some("26".to_string()),
                Some("oid".to_string()),
                Some("b".to_string()),
                Some("4".to_string()),
            ],
            vec![
                Some("700".to_string()),
                Some("float4".to_string()),
                Some("b".to_string()),
                Some("4".to_string()),
            ],
            vec![
                Some("701".to_string()),
                Some("float8".to_string()),
                Some("b".to_string()),
                Some("8".to_string()),
            ],
            vec![
                Some("1042".to_string()),
                Some("bpchar".to_string()),
                Some("b".to_string()),
                Some("-1".to_string()),
            ],
            vec![
                Some("1043".to_string()),
                Some("varchar".to_string()),
                Some("b".to_string()),
                Some("-1".to_string()),
            ],
            vec![
                Some("1082".to_string()),
                Some("date".to_string()),
                Some("b".to_string()),
                Some("4".to_string()),
            ],
            vec![
                Some("1114".to_string()),
                Some("timestamp".to_string()),
                Some("b".to_string()),
                Some("8".to_string()),
            ],
            vec![
                Some("1700".to_string()),
                Some("numeric".to_string()),
                Some("b".to_string()),
                Some("-1".to_string()),
            ],
        ],
    )
}

/// Handle `SELECT * FROM pg_am` — access methods.
fn handle_pg_am() -> QueryResult {
    QueryResult::with_rows(
        vec![
            ColumnDef {
                name: "oid".to_string(),
                col_type: ColumnType::Int,
            },
            ColumnDef {
                name: "amname".to_string(),
                col_type: ColumnType::String,
            },
        ],
        vec![
            vec![Some("2".to_string()), Some("heap".to_string())],
            vec![Some("403".to_string()), Some("btree".to_string())],
        ],
    )
}

/// Handle `SELECT * FROM pg_matviews` — materialized views.
fn handle_pg_matviews(_catalog: &CatalogManager, current_db: &str) -> QueryResult {
    let _db_name = if current_db.is_empty() {
        "default"
    } else {
        current_db
    };

    let columns = vec![
        ColumnDef {
            name: "schemaname".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "matviewname".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "matviewowner".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "tablespace".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "hasindexes".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "ispopulated".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "definition".to_string(),
            col_type: ColumnType::String,
        },
    ];

    // For now, no materialized views registered
    QueryResult::with_rows(columns, Vec::new())
}

/// Handle `SELECT * FROM pg_settings` — runtime settings.
fn handle_pg_settings() -> QueryResult {
    QueryResult::with_rows(
        vec![
            ColumnDef {
                name: "name".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "setting".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "unit".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "category".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "short_desc".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "extra_desc".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "context".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "vartype".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "source".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "min_val".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "max_val".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "enumvals".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "boot_val".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "reset_val".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "sourcefile".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "sourceline".to_string(),
                col_type: ColumnType::Int,
            },
            ColumnDef {
                name: "pending_restart".to_string(),
                col_type: ColumnType::String,
            },
        ],
        vec![
            vec![
                Some("server_version".to_string()),
                Some("15.0".to_string()),
                Some(String::new()),
                Some("Presets".to_string()),
                Some("Sets the server version".to_string()),
                Some(String::new()),
                Some("internal".to_string()),
                Some("string".to_string()),
                Some("default".to_string()),
                Some(String::new()),
                Some(String::new()),
                Some(String::new()),
                Some("15.0".to_string()),
                Some("15.0".to_string()),
                Some(String::new()),
                Some("0".to_string()),
                Some("false".to_string()),
            ],
            vec![
                Some("server_encoding".to_string()),
                Some("UTF8".to_string()),
                Some(String::new()),
                Some("Presets".to_string()),
                Some("Sets the server character set".to_string()),
                Some(String::new()),
                Some("internal".to_string()),
                Some("string".to_string()),
                Some("default".to_string()),
                Some(String::new()),
                Some(String::new()),
                Some(String::new()),
                Some("UTF8".to_string()),
                Some("UTF8".to_string()),
                Some(String::new()),
                Some("0".to_string()),
                Some("false".to_string()),
            ],
            vec![
                Some("max_connections".to_string()),
                Some("100".to_string()),
                Some(String::new()),
                Some("Connections".to_string()),
                Some("Sets maximum number of concurrent connections".to_string()),
                Some(String::new()),
                Some("postmaster".to_string()),
                Some("integer".to_string()),
                Some("default".to_string()),
                Some("1".to_string()),
                Some("262143".to_string()),
                Some(String::new()),
                Some("100".to_string()),
                Some("100".to_string()),
                Some(String::new()),
                Some("0".to_string()),
                Some("false".to_string()),
            ],
        ],
    )
}

/// Handle `SELECT * FROM pg_available_extensions`.
fn handle_pg_available_extensions() -> QueryResult {
    QueryResult::with_rows(
        vec![
            ColumnDef {
                name: "name".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "default_version".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "installed_version".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "comment".to_string(),
                col_type: ColumnType::String,
            },
        ],
        vec![vec![
            Some("hologres".to_string()),
            Some("0.1".to_string()),
            Some("0.1".to_string()),
            Some("Hologres compatibility layer for HarnessDB".to_string()),
        ]],
    )
}

/// Handle `SELECT * FROM hg_stat_activity` — Hologres-specific query.
fn handle_hg_stat_activity(_current_user: &str) -> QueryResult {
    QueryResult::with_rows(
        vec![
            ColumnDef {
                name: "datid".to_string(),
                col_type: ColumnType::Int,
            },
            ColumnDef {
                name: "datname".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "pid".to_string(),
                col_type: ColumnType::Int,
            },
            ColumnDef {
                name: "usesysid".to_string(),
                col_type: ColumnType::Int,
            },
            ColumnDef {
                name: "usename".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "application_name".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "client_addr".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "client_port".to_string(),
                col_type: ColumnType::Int,
            },
            ColumnDef {
                name: "backend_start".to_string(),
                col_type: ColumnType::DateTime,
            },
            ColumnDef {
                name: "state".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "query".to_string(),
                col_type: ColumnType::String,
            },
        ],
        Vec::new(), // Empty for now
    )
}

/// Handle `SELECT * FROM information_schema.tables`.
fn handle_information_schema_tables(catalog: &CatalogManager, current_db: &str) -> QueryResult {
    let db_name = if current_db.is_empty() {
        "default"
    } else {
        current_db
    };

    let columns = vec![
        ColumnDef {
            name: "table_catalog".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "table_schema".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "table_name".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "table_type".to_string(),
            col_type: ColumnType::String,
        },
    ];

    let mut rows = Vec::new();
    if let Some(tables) = catalog.list_tables(db_name) {
        for table_name in tables {
            rows.push(vec![
                Some(db_name.to_string()),
                Some("public".to_string()),
                Some(table_name),
                Some("BASE TABLE".to_string()),
            ]);
        }
    }

    QueryResult::with_rows(columns, rows)
}

/// Handle `SELECT * FROM information_schema.columns`.
fn handle_information_schema_columns(catalog: &CatalogManager, current_db: &str) -> QueryResult {
    let db_name = if current_db.is_empty() {
        "default"
    } else {
        current_db
    };

    let columns = vec![
        ColumnDef {
            name: "table_catalog".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "table_schema".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "table_name".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "column_name".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "ordinal_position".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnDef {
            name: "is_nullable".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "data_type".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "character_maximum_length".to_string(),
            col_type: ColumnType::Int,
        },
    ];

    let mut rows = Vec::new();
    if let Some(tables) = catalog.list_tables(db_name) {
        for table_name in tables {
            if let Some(table) = catalog.get_table(db_name, &table_name) {
                for (i, col) in table.columns.iter().enumerate() {
                    let nullable_str = if col.nullable { "YES" } else { "NO" };
                    let data_type_str = map_harness_type_to_sql_type(&col.data_type);
                    let char_len = match &col.data_type {
                        types::DataType::Varchar(n) => Some(n.to_string()),
                        types::DataType::Char(n) => Some(n.to_string()),
                        _ => None,
                    };

                    rows.push(vec![
                        Some(db_name.to_string()),
                        Some("public".to_string()),
                        Some(table_name.clone()),
                        Some(col.name.clone()),
                        Some((i + 1).to_string()),
                        Some(nullable_str.to_string()),
                        Some(data_type_str),
                        char_len,
                    ]);
                }
            }
        }
    }

    QueryResult::with_rows(columns, rows)
}

/// Handle `SELECT * FROM information_schema.schemata`.
fn handle_information_schema_schemata(catalog: &CatalogManager) -> QueryResult {
    let databases = catalog.list_databases();
    let mut rows = Vec::new();

    for db_name in databases {
        rows.push(vec![
            Some(db_name.clone()),
            Some("public".to_string()),
            Some("UTF8".to_string()),
            Some(String::new()),
        ]);
    }

    QueryResult::with_rows(
        vec![
            ColumnDef {
                name: "catalog_name".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "schema_name".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "default_character_set_name".to_string(),
                col_type: ColumnType::String,
            },
            ColumnDef {
                name: "sql_path".to_string(),
                col_type: ColumnType::String,
            },
        ],
        rows,
    )
}

/// Handle `SELECT * FROM information_schema.views`.
fn handle_information_schema_views(catalog: &CatalogManager, current_db: &str) -> QueryResult {
    let db_name = if current_db.is_empty() {
        "default"
    } else {
        current_db
    };

    let columns = vec![
        ColumnDef {
            name: "table_catalog".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "table_schema".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "table_name".to_string(),
            col_type: ColumnType::String,
        },
        ColumnDef {
            name: "view_definition".to_string(),
            col_type: ColumnType::String,
        },
    ];

    let mut rows = Vec::new();

    if let Some(tables) = catalog.list_tables(db_name) {
        for table_name in tables {
            if let Some(table) = catalog.get_table(db_name, &table_name) {
                if let Some(view_def) = &table.view_definition {
                    rows.push(vec![
                        Some(db_name.to_string()),
                        Some("public".to_string()),
                        Some(table_name),
                        Some(view_def.clone()),
                    ]);
                }
            }
        }
    }

    QueryResult::with_rows(columns, rows)
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Map a HarnessDB DataType to a PostgreSQL type OID.
fn map_harness_type_to_pg_oid(data_type: &types::DataType) -> i32 {
    use types::DataType;
    match data_type {
        DataType::Null => 0,
        DataType::Boolean => 16,          // bool
        DataType::Int8 => 21,             // int2
        DataType::Int16 => 21,            // int2
        DataType::Int32 => 23,            // int4
        DataType::Int64 => 20,            // int8
        DataType::Int128 => 1700,         // numeric
        DataType::Float32 => 700,         // float4
        DataType::Float64 => 701,         // float8
        DataType::Decimal(_) => 1700,     // numeric
        DataType::Date => 1082,           // date
        DataType::DateTime => 1114,       // timestamp
        DataType::Varchar(_) => 1043,     // varchar
        DataType::Char(_) => 1042,        // bpchar
        DataType::String => 25,           // text
        DataType::Binary => 17,           // bytea
        DataType::Json => 25,             // text (PG doesn't have native JSON in protocol)
        DataType::Array(_) => 1009,       // text[]
        DataType::Map(_, _) => 25,        // text
        DataType::Struct(_) => 25,        // text
        DataType::Float32Vector(_) => 25, // text
        DataType::UInt8 => 21,            // int2 (unsigned mapped to wider signed)
        DataType::UInt16 => 23,           // int4
        DataType::UInt32 => 20,           // int8
        DataType::UInt64 => 1700,         // numeric
        DataType::Time => 1083,           // time
        DataType::DateTimeOffset => 1186, // interval (approximation)
        DataType::FixedSizeBinary(_) => 17, // bytea
        DataType::Money => 1700,          // numeric
        DataType::SmallMoney => 1700,     // numeric
        DataType::UniqueIdentifier => 2950, // uuid
    }
}

/// Map a HarnessDB DataType to its size in bytes (for pg_attribute.attlen).
/// -1 means variable length.
fn map_harness_type_to_len(data_type: &types::DataType) -> i16 {
    use types::DataType;
    match data_type {
        DataType::Null => 0,
        DataType::Boolean => 1,
        DataType::Int8 => 2,
        DataType::Int16 => 2,
        DataType::Int32 => 4,
        DataType::Int64 => 8,
        DataType::Int128 => -1,
        DataType::Float32 => 4,
        DataType::Float64 => 8,
        DataType::Decimal(_) => -1,
        DataType::Date => 4,
        DataType::DateTime => 8,
        DataType::Varchar(_) => -1,
        DataType::Char(_) => -1,
        DataType::String => -1,
        DataType::Binary => -1,
        DataType::Json => -1,
        DataType::Array(_) => -1,
        DataType::Map(_, _) => -1,
        DataType::Struct(_) => -1,
        DataType::Float32Vector(_) => -1,
        DataType::UInt8 => 2,
        DataType::UInt16 => 4,
        DataType::UInt32 => 8,
        DataType::UInt64 => -1,
        DataType::Time => 8,
        DataType::DateTimeOffset => -1,
        DataType::FixedSizeBinary(_) => -1,
        DataType::Money => -1,
        DataType::SmallMoney => -1,
        DataType::UniqueIdentifier => 16,
    }
}

/// Map a HarnessDB DataType to a SQL type name string.
fn map_harness_type_to_sql_type(data_type: &types::DataType) -> String {
    use types::DataType;
    match data_type {
        DataType::Null => "NULL".to_string(),
        DataType::Boolean => "boolean".to_string(),
        DataType::Int8 => "smallint".to_string(),
        DataType::Int16 => "smallint".to_string(),
        DataType::Int32 => "integer".to_string(),
        DataType::Int64 => "bigint".to_string(),
        DataType::Int128 => "numeric".to_string(),
        DataType::Float32 => "real".to_string(),
        DataType::Float64 => "double precision".to_string(),
        DataType::Decimal(d) => format!("numeric({},{})", d.precision, d.scale),
        DataType::Date => "date".to_string(),
        DataType::DateTime => "timestamp".to_string(),
        DataType::Varchar(n) => format!("character varying({})", n),
        DataType::Char(n) => format!("character({})", n),
        DataType::String => "text".to_string(),
        DataType::Binary => "bytea".to_string(),
        DataType::Json => "json".to_string(),
        DataType::Array(inner) => format!("{}[]", map_harness_type_to_sql_type(inner)),
        DataType::Map(_, _) => "text".to_string(),
        DataType::Struct(_) => "text".to_string(),
        DataType::Float32Vector(dim) => format!("float32_vector({})", dim),
        DataType::UInt8 => "smallint".to_string(),
        DataType::UInt16 => "integer".to_string(),
        DataType::UInt32 => "bigint".to_string(),
        DataType::UInt64 => "numeric".to_string(),
        DataType::Time => "time".to_string(),
        DataType::DateTimeOffset => "timestamp with time zone".to_string(),
        DataType::FixedSizeBinary(n) => format!("bytea({})", n),
        DataType::Money => "numeric(19,4)".to_string(),
        DataType::SmallMoney => "numeric(10,4)".to_string(),
        DataType::UniqueIdentifier => "uuid".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fe_catalog::CatalogManager;

    #[test]
    fn test_handle_version() {
        let result = handle_version();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        assert!(result.rows[0][0].as_ref().unwrap().contains("PostgreSQL"));
        assert!(result.rows[0][0].as_ref().unwrap().contains("HarnessDB"));
    }

    #[test]
    fn test_handle_current_database() {
        let result = handle_current_database("mydb");
        assert_eq!(result.rows[0][0].as_deref(), Some("mydb"));

        let result = handle_current_database("");
        assert_eq!(result.rows[0][0].as_deref(), Some("default"));
    }

    #[test]
    fn test_handle_current_schema() {
        let result = handle_current_schema();
        assert_eq!(result.rows[0][0].as_deref(), Some("public"));
    }

    #[test]
    fn test_handle_current_user() {
        let result = handle_current_user("admin");
        assert_eq!(result.rows[0][0].as_deref(), Some("admin"));

        let result = handle_current_user("");
        assert_eq!(result.rows[0][0].as_deref(), Some("root"));
    }

    #[test]
    fn test_handle_postmaster_start_time() {
        let result = handle_postmaster_start_time("2025-01-01 00:00:00");
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0].as_deref(), Some("2025-01-01 00:00:00"));
        assert_eq!(
            result.columns[0].col_type,
            ColumnType::DateTime,
            "start_time should be DateTime type"
        );
        assert_eq!(result.columns[0].name, "pg_postmaster_start_time");
    }

    #[test]
    fn test_handle_pg_database() {
        let catalog = CatalogManager::new();
        let result = handle_pg_database(&catalog, "");
        assert!(!result.rows.is_empty());
        // information_schema should be present
        assert!(
            result
                .rows
                .iter()
                .any(|r| r[0].as_deref() == Some("information_schema"))
        );
    }

    #[test]
    fn test_handle_pg_type() {
        let result = handle_pg_type();
        assert_eq!(result.columns.len(), 4);
        assert!(result.rows.len() > 5);
        assert!(result.rows.iter().any(|r| r[1].as_deref() == Some("int4")));
    }

    #[test]
    fn test_handle_set_variable() {
        let result = handle_set_variable("SET search_path = public");
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_handle_show_search_path() {
        let result = handle_show_variable("SHOW search_path");
        assert_eq!(result.rows[0][0].as_deref(), Some("public"));
    }

    #[test]
    fn test_handle_show_server_version() {
        let result = handle_show_variable("SHOW server_version");
        assert_eq!(result.rows[0][0].as_deref(), Some("15.0"));
    }

    #[test]
    fn test_handle_show_server_encoding() {
        let result = handle_show_variable("SHOW server_encoding");
        assert_eq!(result.rows[0][0].as_deref(), Some("UTF8"));
    }

    #[test]
    fn test_handle_show_client_encoding() {
        let result = handle_show_variable("SHOW client_encoding");
        assert_eq!(result.rows[0][0].as_deref(), Some("UTF8"));
    }

    #[test]
    fn test_handle_show_timezone() {
        let result = handle_show_variable("SHOW timezone");
        assert_eq!(result.rows[0][0].as_deref(), Some("UTC"));
    }

    #[test]
    fn test_handle_show_time_zone() {
        let result = handle_show_variable("SHOW TIME ZONE");
        assert_eq!(result.rows[0][0].as_deref(), Some("UTC"));
    }

    #[test]
    fn test_handle_show_datestyle() {
        let result = handle_show_variable("SHOW datestyle");
        assert_eq!(result.rows[0][0].as_deref(), Some("ISO, MDY"));
    }

    #[test]
    fn test_handle_show_max_connections() {
        let result = handle_show_variable("SHOW max_connections");
        assert_eq!(result.rows[0][0].as_deref(), Some("100"));
    }

    #[test]
    fn test_handle_show_transaction_isolation() {
        let result = handle_show_variable("SHOW transaction_isolation");
        assert_eq!(result.rows[0][0].as_deref(), Some("read committed"));
    }

    #[test]
    fn test_handle_show_unknown_variable() {
        let result = handle_show_variable("SHOW some_unknown_var");
        assert_eq!(result.rows[0][0].as_deref(), Some(""));
    }

    #[test]
    fn test_handle_pg_tables_with_catalog() {
        let catalog = CatalogManager::new();
        catalog.create_database("testdb").unwrap();
        let table = fe_catalog::Table {
            id: 1,
            tablet_id: 0,
            name: "mytable".to_string(),
            database: "testdb".to_string(),
            columns: vec![],
            keys_type: fe_catalog::table::KeysType::Duplicate,
            unique_keys: vec![],
            partition_info: None,
            distribution_info: None,
            replication_num: 1,
            properties: std::collections::HashMap::new(),
            row_count: 0,
            data_size: 0,
            stats: None,
            view_definition: None,
        };
        catalog.create_table("testdb", table).unwrap();

        let result = handle_pg_tables(&catalog, "testdb");
        assert!(
            result
                .rows
                .iter()
                .any(|r| r[1].as_deref() == Some("mytable"))
        );
    }

    #[test]
    fn test_handle_pg_tables_with_empty_db() {
        let catalog = CatalogManager::new();
        catalog.create_database("testdb").unwrap();
        let result = handle_pg_tables(&catalog, "testdb");
        assert_eq!(result.columns.len(), 8);
        assert!(result.rows.is_empty(), "Should have no tables in empty db");
    }

    #[test]
    fn test_handle_pg_namespace() {
        let result = handle_pg_namespace();
        assert!(
            result
                .rows
                .iter()
                .any(|r| r[1].as_deref() == Some("public"))
        );
        assert!(
            result
                .rows
                .iter()
                .any(|r| r[1].as_deref() == Some("pg_catalog"))
        );
    }

    #[test]
    fn test_handle_pg_am() {
        let result = handle_pg_am();
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_handle_pg_settings() {
        let result = handle_pg_settings();
        assert!(
            result
                .rows
                .iter()
                .any(|r| r[0].as_deref() == Some("server_version"))
        );
        assert!(
            result
                .rows
                .iter()
                .any(|r| r[0].as_deref() == Some("server_encoding"))
        );
        assert!(
            result
                .rows
                .iter()
                .any(|r| r[0].as_deref() == Some("max_connections"))
        );
    }

    #[test]
    fn test_handle_pg_available_extensions() {
        let result = handle_pg_available_extensions();
        assert!(
            result
                .rows
                .iter()
                .any(|r| r[0].as_deref() == Some("hologres"))
        );
    }

    #[test]
    fn test_handle_pg_matviews_empty() {
        let catalog = CatalogManager::new();
        let result = handle_pg_matviews(&catalog, "testdb");
        assert_eq!(result.columns.len(), 7);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_handle_pg_class_with_tables() {
        let catalog = CatalogManager::new();
        catalog.create_database("testdb").unwrap();
        let table = fe_catalog::Table {
            id: 1,
            tablet_id: 0,
            name: "users".to_string(),
            database: "testdb".to_string(),
            columns: vec![],
            keys_type: fe_catalog::table::KeysType::Duplicate,
            unique_keys: vec![],
            partition_info: None,
            distribution_info: None,
            replication_num: 1,
            properties: std::collections::HashMap::new(),
            row_count: 0,
            data_size: 0,
            stats: None,
            view_definition: None,
        };
        catalog.create_table("testdb", table).unwrap();

        let result = handle_pg_class(&catalog, "testdb");
        assert_eq!(result.columns.len(), 14);
        assert!(!result.rows.is_empty());
        assert!(result.rows.iter().any(|r| r[1].as_deref() == Some("users")));
        // OID should be set starting from 10000
        assert_eq!(result.rows[0][0].as_deref(), Some("10000"));
    }

    #[test]
    fn test_handle_pg_class_empty_db() {
        let catalog = CatalogManager::new();
        catalog.create_database("emptydb").unwrap();
        let result = handle_pg_class(&catalog, "emptydb");
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_handle_pg_attribute_with_columns() {
        let catalog = CatalogManager::new();
        catalog.create_database("testdb").unwrap();
        let table = fe_catalog::Table {
            id: 1,
            tablet_id: 0,
            name: "users".to_string(),
            database: "testdb".to_string(),
            columns: vec![
                fe_catalog::table::TableColumn {
                    name: "id".to_string(),
                    data_type: types::DataType::Int32,
                    nullable: false,
                    default_value: None,
                    agg_type: None,
                    comment: String::new(),
                },
                fe_catalog::table::TableColumn {
                    name: "name".to_string(),
                    data_type: types::DataType::String,
                    nullable: true,
                    default_value: None,
                    agg_type: None,
                    comment: String::new(),
                },
                fe_catalog::table::TableColumn {
                    name: "salary".to_string(),
                    data_type: types::DataType::Float64,
                    nullable: true,
                    default_value: None,
                    agg_type: None,
                    comment: String::new(),
                },
                fe_catalog::table::TableColumn {
                    name: "active".to_string(),
                    data_type: types::DataType::Boolean,
                    nullable: false,
                    default_value: None,
                    agg_type: None,
                    comment: String::new(),
                },
            ],
            keys_type: fe_catalog::table::KeysType::Duplicate,
            unique_keys: vec![],
            partition_info: None,
            distribution_info: None,
            replication_num: 1,
            properties: std::collections::HashMap::new(),
            row_count: 0,
            data_size: 0,
            stats: None,
            view_definition: None,
        };
        catalog.create_table("testdb", table).unwrap();

        let result = handle_pg_attribute(&catalog, "testdb");
        assert_eq!(result.columns.len(), 24);
        assert_eq!(result.rows.len(), 4, "Should have 4 rows for 4 columns");

        // Check first column: id (Int32, not nullable)
        assert_eq!(
            result.rows[0][0].as_deref(),
            Some("10000"),
            "attrelid should start at 10000"
        );
        assert_eq!(result.rows[0][1].as_deref(), Some("id"), "attname");
        assert_eq!(
            result.rows[0][2].as_deref(),
            Some("23"),
            "atttypid for Int32 should be 23"
        );
        assert_eq!(
            result.rows[0][4].as_deref(),
            Some("4"),
            "attlen for Int32 should be 4"
        );
        assert_eq!(
            result.rows[0][5].as_deref(),
            Some("1"),
            "attnum should be 1"
        );
        assert_eq!(
            result.rows[0][12].as_deref(),
            Some("true"),
            "attnotnull should be true for non-nullable"
        );

        // Check second column: name (String/text, nullable)
        assert_eq!(result.rows[1][1].as_deref(), Some("name"));
        assert_eq!(
            result.rows[1][2].as_deref(),
            Some("25"),
            "atttypid for String should be 25"
        );
        assert_eq!(
            result.rows[1][4].as_deref(),
            Some("-1"),
            "attlen for String should be -1"
        );
        assert_eq!(
            result.rows[1][5].as_deref(),
            Some("2"),
            "attnum should be 2"
        );
        assert_eq!(
            result.rows[1][12].as_deref(),
            Some("false"),
            "attnotnull should be false for nullable"
        );

        // Check third column: salary (Float64, nullable)
        assert_eq!(result.rows[2][1].as_deref(), Some("salary"));
        assert_eq!(
            result.rows[2][2].as_deref(),
            Some("701"),
            "atttypid for Float64 should be 701"
        );
        assert_eq!(
            result.rows[2][4].as_deref(),
            Some("8"),
            "attlen for Float64 should be 8"
        );

        // Check fourth column: active (Boolean, not nullable)
        assert_eq!(result.rows[3][1].as_deref(), Some("active"));
        assert_eq!(
            result.rows[3][2].as_deref(),
            Some("16"),
            "atttypid for Boolean should be 16"
        );
        assert_eq!(
            result.rows[3][4].as_deref(),
            Some("1"),
            "attlen for Boolean should be 1"
        );
        assert_eq!(
            result.rows[3][12].as_deref(),
            Some("true"),
            "attnotnull should be true for non-nullable"
        );
    }

    #[test]
    fn test_handle_pg_attribute_empty_db() {
        let catalog = CatalogManager::new();
        catalog.create_database("emptydb").unwrap();
        let result = handle_pg_attribute(&catalog, "emptydb");
        assert_eq!(result.columns.len(), 24);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_handle_information_schema_tables() {
        let catalog = CatalogManager::new();
        catalog.create_database("testdb").unwrap();
        let table = fe_catalog::Table {
            id: 1,
            tablet_id: 0,
            name: "users".to_string(),
            database: "testdb".to_string(),
            columns: vec![],
            keys_type: fe_catalog::table::KeysType::Duplicate,
            unique_keys: vec![],
            partition_info: None,
            distribution_info: None,
            replication_num: 1,
            properties: std::collections::HashMap::new(),
            row_count: 0,
            data_size: 0,
            stats: None,
            view_definition: None,
        };
        catalog.create_table("testdb", table).unwrap();

        let result = handle_information_schema_tables(&catalog, "testdb");
        assert!(result.rows.iter().any(|r| r[2].as_deref() == Some("users")));
    }

    #[test]
    fn test_handle_information_schema_columns() {
        let catalog = CatalogManager::new();
        catalog.create_database("testdb").unwrap();
        let table = fe_catalog::Table {
            id: 1,
            tablet_id: 0,
            name: "users".to_string(),
            database: "testdb".to_string(),
            columns: vec![
                fe_catalog::table::TableColumn {
                    name: "id".to_string(),
                    data_type: types::DataType::Int32,
                    nullable: false,
                    default_value: None,
                    agg_type: None,
                    comment: String::new(),
                },
                fe_catalog::table::TableColumn {
                    name: "name".to_string(),
                    data_type: types::DataType::String,
                    nullable: true,
                    default_value: None,
                    agg_type: None,
                    comment: String::new(),
                },
            ],
            keys_type: fe_catalog::table::KeysType::Duplicate,
            unique_keys: vec![],
            partition_info: None,
            distribution_info: None,
            replication_num: 1,
            properties: std::collections::HashMap::new(),
            row_count: 0,
            data_size: 0,
            stats: None,
            view_definition: None,
        };
        catalog.create_table("testdb", table).unwrap();

        let result = handle_information_schema_columns(&catalog, "testdb");
        assert_eq!(result.rows.len(), 2);
        assert!(result.rows.iter().any(|r| r[3].as_deref() == Some("id")));
        assert!(result.rows.iter().any(|r| r[3].as_deref() == Some("name")));
        // Check nullable mapping
        assert!(result.rows.iter().any(|r| r[5].as_deref() == Some("NO")));
        assert!(result.rows.iter().any(|r| r[5].as_deref() == Some("YES")));
    }

    #[test]
    fn test_handle_information_schema_schemata() {
        let catalog = CatalogManager::new();
        catalog.create_database("mydb").unwrap();
        let result = handle_information_schema_schemata(&catalog);
        assert!(result.rows.iter().any(|r| r[0].as_deref() == Some("mydb")));
    }

    #[test]
    fn test_handle_information_schema_views_no_views() {
        let catalog = CatalogManager::new();
        catalog.create_database("testdb").unwrap();
        let table = fe_catalog::Table {
            id: 1,
            tablet_id: 0,
            name: "regular_table".to_string(),
            database: "testdb".to_string(),
            columns: vec![],
            keys_type: fe_catalog::table::KeysType::Duplicate,
            unique_keys: vec![],
            partition_info: None,
            distribution_info: None,
            replication_num: 1,
            properties: std::collections::HashMap::new(),
            row_count: 0,
            data_size: 0,
            stats: None,
            view_definition: None,
        };
        catalog.create_table("testdb", table).unwrap();

        let result = handle_information_schema_views(&catalog, "testdb");
        assert_eq!(result.columns.len(), 4);
        assert!(
            result.rows.is_empty(),
            "Should be empty when no tables have view_definitions"
        );
    }

    #[test]
    fn test_handle_information_schema_views_with_view() {
        let catalog = CatalogManager::new();
        catalog.create_database("testdb").unwrap();

        // Table with view_definition
        let view_table = fe_catalog::Table {
            id: 2,
            tablet_id: 0,
            name: "user_view".to_string(),
            database: "testdb".to_string(),
            columns: vec![],
            keys_type: fe_catalog::table::KeysType::Duplicate,
            unique_keys: vec![],
            partition_info: None,
            distribution_info: None,
            replication_num: 1,
            properties: std::collections::HashMap::new(),
            row_count: 0,
            data_size: 0,
            stats: None,
            view_definition: Some("SELECT * FROM users WHERE active = true".to_string()),
        };
        catalog.create_table("testdb", view_table).unwrap();

        // Also add a regular table (no view_definition)
        let regular_table = fe_catalog::Table {
            id: 1,
            tablet_id: 0,
            name: "regular_table".to_string(),
            database: "testdb".to_string(),
            columns: vec![],
            keys_type: fe_catalog::table::KeysType::Duplicate,
            unique_keys: vec![],
            partition_info: None,
            distribution_info: None,
            replication_num: 1,
            properties: std::collections::HashMap::new(),
            row_count: 0,
            data_size: 0,
            stats: None,
            view_definition: None,
        };
        catalog.create_table("testdb", regular_table).unwrap();

        let result = handle_information_schema_views(&catalog, "testdb");
        assert_eq!(result.rows.len(), 1, "Should have 1 view");
        assert_eq!(result.rows[0][2].as_deref(), Some("user_view"));
        assert_eq!(
            result.rows[0][3].as_deref(),
            Some("SELECT * FROM users WHERE active = true")
        );
    }

    #[test]
    fn test_handle_hg_stat_activity() {
        let result = handle_hg_stat_activity("admin");
        assert_eq!(result.columns.len(), 11);
    }

    #[test]
    fn test_handle_pg_matviews() {
        let catalog = CatalogManager::new();
        let result = handle_pg_matviews(&catalog, "testdb");
        assert_eq!(result.columns.len(), 7);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn test_pg_catalog_routing_version() {
        let catalog = CatalogManager::new();
        let result = handle_pg_catalog_query(
            "SELECT version()",
            &catalog,
            "testdb",
            "admin",
            "2025-01-01 00:00:00",
        );
        assert!(result.is_some());
        let r = result.unwrap();
        assert!(r.rows[0][0].as_ref().unwrap().contains("PostgreSQL"));
    }

    #[test]
    fn test_pg_catalog_routing_current_database() {
        let catalog = CatalogManager::new();
        let result =
            handle_pg_catalog_query("SELECT current_database()", &catalog, "mydb", "admin", "");
        assert!(result.is_some());
        assert_eq!(result.unwrap().rows[0][0].as_deref(), Some("mydb"));
    }

    #[test]
    fn test_pg_catalog_routing_current_schema() {
        let catalog = CatalogManager::new();
        let result =
            handle_pg_catalog_query("SELECT current_schema()", &catalog, "testdb", "admin", "");
        assert!(result.is_some());
        assert_eq!(result.unwrap().rows[0][0].as_deref(), Some("public"));
    }

    #[test]
    fn test_pg_catalog_routing_current_user() {
        let catalog = CatalogManager::new();
        let result =
            handle_pg_catalog_query("SELECT current_user", &catalog, "testdb", "myuser", "");
        assert!(result.is_some());
        assert_eq!(result.unwrap().rows[0][0].as_deref(), Some("myuser"));
    }

    #[test]
    fn test_pg_catalog_routing_current_user_func() {
        let catalog = CatalogManager::new();
        let result =
            handle_pg_catalog_query("SELECT current_user()", &catalog, "testdb", "myuser", "");
        assert!(result.is_some());
        assert_eq!(result.unwrap().rows[0][0].as_deref(), Some("myuser"));
    }

    #[test]
    fn test_pg_catalog_routing_postmaster_start_time() {
        let catalog = CatalogManager::new();
        let result = handle_pg_catalog_query(
            "SELECT pg_postmaster_start_time()",
            &catalog,
            "testdb",
            "admin",
            "2025-06-01 12:00:00",
        );
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.rows[0][0].as_deref(), Some("2025-06-01 12:00:00"));
    }

    #[test]
    fn test_pg_catalog_routing_set() {
        let catalog = CatalogManager::new();
        let result =
            handle_pg_catalog_query("SET search_path = public", &catalog, "testdb", "admin", "");
        assert!(result.is_some());
        let r = result.unwrap();
        assert!(r.columns.is_empty());
    }

    #[test]
    fn test_pg_catalog_routing_show() {
        let catalog = CatalogManager::new();
        let result = handle_pg_catalog_query("SHOW search_path", &catalog, "testdb", "admin", "");
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.rows[0][0].as_deref(), Some("public"));
    }

    #[test]
    fn test_pg_catalog_routing_not_catalog() {
        let catalog = CatalogManager::new();
        let result =
            handle_pg_catalog_query("SELECT * FROM real_table", &catalog, "testdb", "admin", "");
        assert!(result.is_none());
    }

    #[test]
    fn test_pg_catalog_routing_pg_class() {
        let catalog = CatalogManager::new();
        catalog.create_database("testdb").unwrap();
        let table = fe_catalog::Table {
            id: 1,
            tablet_id: 0,
            name: "orders".to_string(),
            database: "testdb".to_string(),
            columns: vec![],
            keys_type: fe_catalog::table::KeysType::Duplicate,
            unique_keys: vec![],
            partition_info: None,
            distribution_info: None,
            replication_num: 1,
            properties: std::collections::HashMap::new(),
            row_count: 0,
            data_size: 0,
            stats: None,
            view_definition: None,
        };
        catalog.create_table("testdb", table).unwrap();

        let result =
            handle_pg_catalog_query("SELECT * FROM pg_class", &catalog, "testdb", "admin", "");
        assert!(result.is_some());
        let r = result.unwrap();
        assert!(r.rows.iter().any(|row| row[1].as_deref() == Some("orders")));
    }

    #[test]
    fn test_pg_catalog_routing_pg_attribute() {
        let catalog = CatalogManager::new();
        catalog.create_database("testdb").unwrap();
        let table = fe_catalog::Table {
            id: 1,
            tablet_id: 0,
            name: "items".to_string(),
            database: "testdb".to_string(),
            columns: vec![fe_catalog::table::TableColumn {
                name: "id".to_string(),
                data_type: types::DataType::Int32,
                nullable: false,
                default_value: None,
                agg_type: None,
                comment: String::new(),
            }],
            keys_type: fe_catalog::table::KeysType::Duplicate,
            unique_keys: vec![],
            partition_info: None,
            distribution_info: None,
            replication_num: 1,
            properties: std::collections::HashMap::new(),
            row_count: 0,
            data_size: 0,
            stats: None,
            view_definition: None,
        };
        catalog.create_table("testdb", table).unwrap();

        let result = handle_pg_catalog_query(
            "SELECT * FROM pg_attribute",
            &catalog,
            "testdb",
            "admin",
            "",
        );
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1].as_deref(), Some("id"));
    }

    #[test]
    fn test_pg_catalog_routing_pg_namespace() {
        let catalog = CatalogManager::new();
        let result = handle_pg_catalog_query(
            "SELECT * FROM pg_namespace",
            &catalog,
            "testdb",
            "admin",
            "",
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_pg_catalog_routing_pg_tables() {
        let catalog = CatalogManager::new();
        catalog.create_database("testdb").unwrap();
        let result =
            handle_pg_catalog_query("SELECT * FROM pg_tables", &catalog, "testdb", "admin", "");
        assert!(result.is_some());
    }

    #[test]
    fn test_pg_catalog_routing_pg_settings() {
        let catalog = CatalogManager::new();
        let result =
            handle_pg_catalog_query("SELECT * FROM pg_settings", &catalog, "testdb", "admin", "");
        assert!(result.is_some());
    }

    #[test]
    fn test_pg_catalog_routing_information_schema_tables() {
        let catalog = CatalogManager::new();
        let result = handle_pg_catalog_query(
            "SELECT * FROM information_schema.tables",
            &catalog,
            "testdb",
            "admin",
            "",
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_pg_catalog_routing_information_schema_columns() {
        let catalog = CatalogManager::new();
        let result = handle_pg_catalog_query(
            "SELECT * FROM information_schema.columns",
            &catalog,
            "testdb",
            "admin",
            "",
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_pg_catalog_routing_information_schema_schemata() {
        let catalog = CatalogManager::new();
        let result = handle_pg_catalog_query(
            "SELECT * FROM information_schema.schemata",
            &catalog,
            "testdb",
            "admin",
            "",
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_pg_catalog_routing_information_schema_views() {
        let catalog = CatalogManager::new();
        let result = handle_pg_catalog_query(
            "SELECT * FROM information_schema.views",
            &catalog,
            "testdb",
            "admin",
            "",
        );
        assert!(result.is_some());
    }

    #[test]
    fn test_pg_catalog_routing_stub_tables() {
        let catalog = CatalogManager::new();
        // Stub handlers should return QueryResult::ok()
        for query in &[
            "SELECT * FROM pg_index",
            "SELECT * FROM pg_description",
            "SELECT * FROM pg_proc",
            "SELECT * FROM pg_trigger",
            "SELECT * FROM pg_enum",
            "SELECT * FROM pg_range",
            "SELECT * FROM pg_cast",
            "SELECT * FROM pg_opclass",
            "SELECT * FROM pg_views",
            "SELECT * FROM pg_extension",
            "SELECT * FROM pg_timezone_names",
            "SELECT * FROM pg_collation",
            "SELECT * FROM pg_foreign_table",
        ] {
            let result = handle_pg_catalog_query(query, &catalog, "testdb", "admin", "");
            assert!(result.is_some(), "Expected handler for: {}", query);
        }
    }

    #[test]
    fn test_pg_catalog_routing_hg_stat_activity() {
        let catalog = CatalogManager::new();
        let result = handle_pg_catalog_query(
            "SELECT * FROM hg_stat_activity",
            &catalog,
            "testdb",
            "admin",
            "",
        );
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.columns.len(), 11);
    }

    #[test]
    fn test_map_type_oids() {
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Int32), 23);
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Boolean), 16);
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::String), 25);
        assert_eq!(
            map_harness_type_to_pg_oid(&types::DataType::Varchar(255)),
            1043
        );
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Float64), 701);
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Null), 0);
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Int8), 21);
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Int16), 21);
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Int64), 20);
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Float32), 700);
        assert_eq!(
            map_harness_type_to_pg_oid(&types::DataType::Decimal(types::DecimalType {
                precision: 38,
                scale: 10
            })),
            1700
        );
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Date), 1082);
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::DateTime), 1114);
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Char(10)), 1042);
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Binary), 17);
        assert_eq!(map_harness_type_to_pg_oid(&types::DataType::Json), 25);
        assert_eq!(
            map_harness_type_to_pg_oid(&types::DataType::Array(Box::new(types::DataType::String))),
            1009
        );
        assert_eq!(
            map_harness_type_to_pg_oid(&types::DataType::Map(
                Box::new(types::DataType::String),
                Box::new(types::DataType::Int32)
            )),
            25
        );
        assert_eq!(
            map_harness_type_to_pg_oid(&types::DataType::Struct(vec![])),
            25
        );
        assert_eq!(
            map_harness_type_to_pg_oid(&types::DataType::Float32Vector(128)),
            25
        );
    }

    #[test]
    fn test_map_type_to_len() {
        // Fixed-length types
        assert_eq!(map_harness_type_to_len(&types::DataType::Null), 0);
        assert_eq!(map_harness_type_to_len(&types::DataType::Boolean), 1);
        assert_eq!(map_harness_type_to_len(&types::DataType::Int8), 2);
        assert_eq!(map_harness_type_to_len(&types::DataType::Int16), 2);
        assert_eq!(map_harness_type_to_len(&types::DataType::Int32), 4);
        assert_eq!(map_harness_type_to_len(&types::DataType::Int64), 8);
        assert_eq!(map_harness_type_to_len(&types::DataType::Float32), 4);
        assert_eq!(map_harness_type_to_len(&types::DataType::Float64), 8);
        assert_eq!(map_harness_type_to_len(&types::DataType::Date), 4);
        assert_eq!(map_harness_type_to_len(&types::DataType::DateTime), 8);

        // Variable-length types
        assert_eq!(map_harness_type_to_len(&types::DataType::Int128), -1);
        assert_eq!(
            map_harness_type_to_len(&types::DataType::Decimal(types::DecimalType {
                precision: 38,
                scale: 10
            })),
            -1
        );
        assert_eq!(map_harness_type_to_len(&types::DataType::Varchar(255)), -1);
        assert_eq!(map_harness_type_to_len(&types::DataType::Char(10)), -1);
        assert_eq!(map_harness_type_to_len(&types::DataType::String), -1);
        assert_eq!(map_harness_type_to_len(&types::DataType::Binary), -1);
        assert_eq!(map_harness_type_to_len(&types::DataType::Json), -1);
        assert_eq!(
            map_harness_type_to_len(&types::DataType::Array(Box::new(types::DataType::Int32))),
            -1
        );
        assert_eq!(
            map_harness_type_to_len(&types::DataType::Map(
                Box::new(types::DataType::String),
                Box::new(types::DataType::String)
            )),
            -1
        );
        assert_eq!(map_harness_type_to_len(&types::DataType::Struct(vec![])), -1);
        assert_eq!(
            map_harness_type_to_len(&types::DataType::Float32Vector(256)),
            -1
        );
    }

    #[test]
    fn test_map_type_to_sql() {
        assert_eq!(
            map_harness_type_to_sql_type(&types::DataType::Int32),
            "integer"
        );
        assert_eq!(map_harness_type_to_sql_type(&types::DataType::String), "text");
        assert_eq!(
            map_harness_type_to_sql_type(&types::DataType::Varchar(100)),
            "character varying(100)"
        );
        assert_eq!(
            map_harness_type_to_sql_type(&types::DataType::Boolean),
            "boolean"
        );
        assert_eq!(
            map_harness_type_to_sql_type(&types::DataType::Float64),
            "double precision"
        );
        assert_eq!(
            map_harness_type_to_sql_type(&types::DataType::Decimal(types::DecimalType {
                precision: 38,
                scale: 10
            })),
            "numeric(38,10)"
        );
    }
}
