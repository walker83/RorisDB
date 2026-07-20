use crate::ast::*;
use crate::error::ParseError;

/// Case-insensitive search for an ASCII-only needle in a potentially non-ASCII haystack.
/// Returns the byte offset of the first match, or None.
/// Safe for use with Unicode haystacks — never splits multi-byte characters.
fn find_ascii_ci(haystack: &str, needle: &str) -> Option<usize> {
    debug_assert!(needle.is_ascii(), "find_ascii_ci requires ASCII needle");
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    let nlen = n.len();
    if nlen == 0 || h.len() < nlen {
        return None;
    }
    let n_upper: Vec<u8> = n.iter().map(|b| b.to_ascii_uppercase()).collect();
    for i in 0..=(h.len() - nlen) {
        // Ensure we don't start inside a multi-byte character
        if !haystack.is_char_boundary(i) {
            continue;
        }
        let matched = (0..nlen).all(|j| h[i + j].to_ascii_uppercase() == n_upper[j]);
        if matched {
            return Some(i);
        }
    }
    None
}

/// Like `find_ascii_ci` but returns the LAST match.
fn rfind_ascii_ci(haystack: &str, needle: &str) -> Option<usize> {
    debug_assert!(needle.is_ascii(), "rfind_ascii_ci requires ASCII needle");
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    let nlen = n.len();
    if nlen == 0 || h.len() < nlen {
        return None;
    }
    let n_upper: Vec<u8> = n.iter().map(|b| b.to_ascii_uppercase()).collect();
    for i in (0..=(h.len() - nlen)).rev() {
        if !haystack.is_char_boundary(i) {
            continue;
        }
        let matched = (0..nlen).all(|j| h[i + j].to_ascii_uppercase() == n_upper[j]);
        if matched {
            return Some(i);
        }
    }
    None
}

/// Case-insensitive prefix stripping for ASCII keywords.
/// Returns the remainder after stripping `prefix` from the start of `s`,
/// or None if `s` does not start with `prefix` (case-insensitively).
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.len() < prefix.len() {
        return None;
    }
    let (head, tail) = s.split_at(prefix.len());
    if head.eq_ignore_ascii_case(prefix) {
        Some(tail)
    } else {
        None
    }
}

/// Helper for sub-parsers: uppercases the SQL for keyword matching while keeping
/// the original for value extraction. Returns (uppercased, original_remainder) after
/// stripping a keyword prefix of known length.
///
/// Since SQL keywords are pure ASCII, `.to_uppercase()` preserves byte length for the
/// keyword prefix, so byte offsets from the uppercased string are valid for the original.
#[allow(dead_code)]
fn keyword_split<'a>(sql: &'a str, keyword_len: usize) -> (String, &'a str) {
    let upper = sql.to_uppercase();
    let remainder = if keyword_len <= sql.len() {
        sql[keyword_len..].trim_start()
    } else {
        ""
    };
    (upper, remainder)
}

/// Workaround for sqlparser: it doesn't support `INSERT ... SELECT ... ON DUPLICATE KEY`.
/// MySQL requires `FROM DUAL` in this case. This function auto-injects DUAL.
/// E.g.: `INSERT INTO t SELECT ... ON DUPLICATE KEY UPDATE ...`
/// Becomes: `INSERT INTO t SELECT ... FROM DUAL ON DUPLICATE KEY UPDATE ...`
fn fixup_insert_select_on_duplicate(sql: &str) -> String {
    let trimmed = sql.trim();

    // Use case-insensitive search on original string to avoid Unicode offset bugs
    let upper_check = trimmed.to_uppercase();

    // Quick checks using uppercased copy (no slicing, just contains/starts_with)
    if !upper_check.starts_with("INSERT") {
        return sql.to_string();
    }
    if !upper_check.contains("ON DUPLICATE KEY") {
        return sql.to_string();
    }
    if upper_check.contains("FROM DUAL") {
        return sql.to_string();
    }

    // Find positions in the ORIGINAL string using case-insensitive search
    let Some(on_dup_pos) = find_ascii_ci(trimmed, "ON DUPLICATE KEY") else {
        return sql.to_string();
    };
    let before_on_dup = &trimmed[..on_dup_pos];

    // Check for SELECT keyword before ON DUPLICATE KEY (case-insensitive)
    if !find_ascii_ci(before_on_dup, "SELECT").is_some() {
        return sql.to_string();
    }

    // Insert " FROM DUAL" right before ON DUPLICATE KEY in the original string
    let mut result = trimmed[..on_dup_pos].to_string();
    result.push_str(" FROM DUAL ");
    result.push_str(&trimmed[on_dup_pos..]);
    result
}

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let trimmed = sql.trim().to_uppercase();

    // Pre-process Doris-specific CREATE TABLE extensions before sqlparser
    let sql_to_parse;
    let mut doris_distribution: Option<DistributionDef> = None;
    let mut doris_partition: Option<PartitionDef> = None;
    let mut doris_properties: Vec<(String, String)> = vec![];
    let mut doris_keys_type = KeysType::Duplicate;
    let mut doris_unique_keys: Vec<UniqueKeyDef> = vec![];

    if trimmed.starts_with("CREATE TABLE") {
        let preprocessed = preprocess_create_table(sql);
        sql_to_parse = preprocessed.clean_sql;
        doris_distribution = preprocessed.distribution;
        doris_partition = preprocessed.partition;
        doris_properties = preprocessed.properties;
        doris_keys_type = preprocessed.keys_type;
        doris_unique_keys = preprocessed.unique_keys;
    } else {
        sql_to_parse = sql.to_string();
    }

    if trimmed.starts_with("CREATE REPOSITORY") {
        return parse_create_repository(sql);
    }
    if trimmed.starts_with("DROP REPOSITORY") {
        return parse_drop_repository(sql);
    }
    if trimmed.starts_with("SHOW REPOSITORIES") {
        return Ok(vec![Statement::ShowRepositories]);
    }
    if trimmed.starts_with("SHOW CREATE DATABASE") {
        return parse_show_create_database(sql);
    }
    if trimmed.starts_with("SHOW CREATE VIEW") {
        return parse_show_create_view(sql);
    }
    if trimmed.starts_with("SHOW PARTITIONS") {
        return parse_show_partitions(sql);
    }
    if trimmed.starts_with("SHOW TABLE STATUS") {
        return parse_show_table_status(sql);
    }
    if trimmed.starts_with("SHOW VARIABLES") {
        return parse_show_variables(sql);
    }
    if trimmed.starts_with("SHOW PROCESSLIST") || trimmed == "SHOW PROCESSLIST" {
        return parse_show_processlist(sql);
    }
    if trimmed.starts_with("SHOW STATUS") || trimmed == "SHOW STATUS" {
        return parse_show_status(sql);
    }
    if trimmed.starts_with("SHOW ENGINES") || trimmed == "SHOW ENGINES" {
        return Ok(vec![Statement::ShowEngines]);
    }
    if trimmed.starts_with("SHOW CHARSET") || trimmed.starts_with("SHOW CHARACTER SET") {
        return Ok(vec![Statement::ShowCharset]);
    }
    if trimmed.starts_with("SHOW COLLATION") {
        return parse_show_collation(sql);
    }
    if trimmed.starts_with("SHOW INDEX") || trimmed.starts_with("SHOW KEYS") {
        return parse_show_index(sql);
    }
    if trimmed.starts_with("SHOW ALTER TABLE") {
        return parse_show_alter_table(sql);
    }
    if trimmed.starts_with("SHOW BACKENDS") || trimmed == "SHOW BACKENDS" {
        return Ok(vec![Statement::ShowBackends]);
    }
    if trimmed.starts_with("SHOW FRONTENDS") || trimmed == "SHOW FRONTENDS" {
        return Ok(vec![Statement::ShowFrontends]);
    }
    if trimmed.starts_with("SHOW DYNAMIC PARTITION")
        || trimmed.starts_with("SHOW DYNAMIC PARTITION TABLES")
    {
        return Ok(vec![Statement::ShowDynamicPartitionTables]);
    }
    if trimmed.starts_with("SHOW VIEW") {
        return parse_show_view(sql);
    }
    if trimmed.starts_with("SHOW CREATE MATERIALIZED VIEW") {
        return parse_show_create_materialized_view(sql);
    }
    if trimmed.starts_with("SHOW TABLE ID") || trimmed == "SHOW TABLE ID" {
        return Ok(vec![Statement::ShowTableId]);
    }
    if trimmed.starts_with("SHOW PARTITION ID") || trimmed == "SHOW PARTITION ID" {
        return Ok(vec![Statement::ShowPartitionId]);
    }
    if trimmed.starts_with("BACKUP DATABASE") {
        return parse_backup_database(sql);
    }
    if trimmed.starts_with("RESTORE DATABASE") {
        return parse_restore_database(sql);
    }
    if trimmed.starts_with("CREATE MATERIALIZED VIEW") {
        return parse_create_materialized_view(sql);
    }
    if trimmed.starts_with("DROP MATERIALIZED VIEW") {
        return parse_drop_materialized_view(sql);
    }
    if trimmed.starts_with("ALTER MATERIALIZED VIEW") {
        return parse_alter_materialized_view(sql);
    }
    if trimmed.starts_with("REFRESH MATERIALIZED VIEW") {
        return parse_refresh_materialized_view(sql);
    }
    if trimmed.starts_with("CREATE USER") || trimmed.starts_with("CREATE USER IF NOT EXISTS") {
        return parse_create_user(sql);
    }
    if trimmed.starts_with("DROP USER") {
        return parse_drop_user(sql);
    }
    if trimmed.starts_with("SHOW USERS") || trimmed == "SHOW USERS" {
        return Ok(vec![Statement::ShowUsers]);
    }
    if trimmed.starts_with("CREATE CATALOG") {
        return parse_create_catalog(sql);
    }
    if trimmed.starts_with("DROP CATALOG") {
        return parse_drop_catalog(sql);
    }
    if trimmed.starts_with("SHOW CATALOGS") {
        return Ok(vec![Statement::ShowCatalogs]);
    }
    if trimmed.starts_with("REFRESH CATALOG") {
        return parse_refresh_catalog(sql);
    }

    // Batch 2 DDL: CREATE INDEX, DROP INDEX, CANCEL ALTER TABLE, ALTER COLOCATE GROUP, ALTER DATABASE, DROP VIEW, ALTER VIEW
    if trimmed.starts_with("CREATE INDEX") {
        return parse_create_index(sql);
    }
    if trimmed.starts_with("DROP INDEX") {
        return parse_drop_index(sql);
    }
    if trimmed.starts_with("CANCEL ALTER TABLE") {
        return parse_cancel_alter_table(sql);
    }
    if trimmed.starts_with("ALTER COLOCATE GROUP") {
        return parse_alter_colocate_group(sql);
    }
    if trimmed.starts_with("ALTER DATABASE") {
        return parse_alter_database(sql);
    }
    if trimmed.starts_with("DROP VIEW") {
        return parse_drop_view(sql);
    }
    if trimmed.starts_with("ALTER VIEW") {
        return parse_alter_view(sql);
    }

    // Doris-specific ALTER TABLE extensions
    if trimmed.starts_with("ALTER TABLE") {
        let upper = trimmed.clone();
        if upper.contains("RENAME COLUMN")
            || upper.contains("SET PROPERTIES")
            || upper.contains("COMMENT")
            || upper.contains("ADD PARTITION")
            || upper.contains("DROP PARTITION")
            || upper.contains("ADD ROLLUP")
            || upper.contains("DROP ROLLUP")
            || upper.contains("REPLACE WITH")
            || upper.contains("ADD GENERATED COLUMN")
        {
            return parse_alter_table_doris(sql);
        }
    }

    // Batch 3/4 statements
    if trimmed.starts_with("EXPORT TABLE") {
        return parse_export_table(sql);
    }
    if trimmed.starts_with("CANCEL EXPORT") {
        return parse_cancel_export(sql);
    }
    if trimmed == "SHOW EXPORT" || trimmed == "SHOW EXPORT;" {
        return Ok(vec![Statement::ShowExport]);
    }
    if trimmed.starts_with("CREATE FUNCTION") {
        return parse_create_function(sql);
    }
    if trimmed.starts_with("DROP FUNCTION") {
        return parse_drop_function(sql);
    }
    if trimmed.starts_with("SHOW CREATE FUNCTION") {
        return parse_show_create_function(sql);
    }
    if trimmed.starts_with("SHOW FUNCTIONS") {
        return parse_show_functions(sql);
    }
    if trimmed.starts_with("DESC FUNCTION") || trimmed.starts_with("DESCRIBE FUNCTION") {
        return parse_desc_function(sql);
    }
    if trimmed.starts_with("ANALYZE TABLE") {
        return parse_analyze_table(sql);
    }
    if trimmed.starts_with("DROP STATS") {
        return parse_drop_stats(sql);
    }
    if trimmed.starts_with("DROP ANALYZE JOB") {
        let Some(rest) = sql.trim().strip_prefix("DROP ANALYZE JOB") else {
            return Err(ParseError::SyntaxError {
                position: 0,
                message: "Expected DROP ANALYZE JOB".into(),
            });
        };
        let after = rest.trim();
        let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected job ID".to_string(),
        })?;
        return Ok(vec![Statement::DropJob(name.to_string())]);
    }
    if trimmed.starts_with("KILL ANALYZE") {
        return parse_kill_analyze_job(sql);
    }
    if trimmed.starts_with("KILL QUERY") {
        return parse_kill_query(sql);
    }
    if trimmed.starts_with("KILL ") && !trimmed.starts_with("KILL ANALYZE") {
        return parse_kill_connection(sql);
    }
    if trimmed.starts_with("ADMIN CHECK TABLE") {
        return parse_admin_check_table(sql);
    }
    if trimmed.starts_with("ADMIN SHOW REPLICA") || trimmed == "ADMIN SHOW REPLICA" {
        return Ok(vec![Statement::AdminShowReplica]);
    }
    if trimmed.starts_with("ALTER STATS") {
        return parse_alter_stats(sql);
    }
    if trimmed.starts_with("SHOW ANALYZE") {
        return parse_show_analyze(sql);
    }
    if trimmed.starts_with("SHOW STATS ") || trimmed == "SHOW STATS" {
        return parse_show_stats(sql);
    }
    if trimmed.starts_with("SHOW TABLE STATS") || trimmed.starts_with("SHOW TABLE STATISTICS") {
        return parse_show_table_stats(sql);
    }
    if trimmed.starts_with("CREATE JOB") {
        return parse_create_job(sql);
    }
    if trimmed.starts_with("DROP JOB") {
        return parse_drop_job(sql);
    }
    if trimmed.starts_with("PAUSE JOB") {
        return parse_pause_job(sql);
    }
    if trimmed.starts_with("RESUME JOB") {
        return parse_resume_job(sql);
    }
    if trimmed.starts_with("CANCEL TASK") {
        return parse_cancel_task(sql);
    }
    if trimmed.starts_with("INSTALL PLUGIN") {
        return parse_install_plugin(sql);
    }
    if trimmed.starts_with("UNINSTALL PLUGIN") {
        return parse_uninstall_plugin(sql);
    }
    if trimmed == "SHOW PLUGINS" {
        return Ok(vec![Statement::ShowPlugins]);
    }
    if trimmed.starts_with("RECOVER DATABASE") {
        return parse_recover_database(sql);
    }
    if trimmed.starts_with("RECOVER TABLE") {
        return parse_recover_table(sql);
    }
    if trimmed.starts_with("RECOVER PARTITION") {
        return parse_recover_partition(sql);
    }
    if trimmed.starts_with("DROP CATALOG RECYCLE") {
        return parse_drop_catalog_recycle_bin(sql);
    }
    if trimmed.starts_with("SHOW CATALOG RECYCLE") {
        return Ok(vec![Statement::ShowCatalogRecycleBin]);
    }
    if trimmed.starts_with("CREATE SQL_BLOCK_RULE") {
        return parse_create_sql_block_rule(sql);
    }
    if trimmed.starts_with("ALTER SQL_BLOCK_RULE") {
        return parse_alter_sql_block_rule(sql);
    }
    if trimmed.starts_with("DROP SQL_BLOCK_RULE") {
        return parse_drop_sql_block_rule(sql);
    }
    if trimmed.starts_with("SHOW SQL_BLOCK_RULE") {
        return parse_show_sql_block_rule(sql);
    }
    if trimmed.starts_with("CREATE ROW POLICY") {
        return parse_create_row_policy(sql);
    }
    if trimmed.starts_with("DROP ROW POLICY") {
        return parse_drop_row_policy(sql);
    }
    if trimmed.starts_with("SHOW ROW POLICY") {
        return parse_show_row_policy(sql);
    }

    // Transaction statements - before sqlparser since sqlparser may not handle all variants
    let upper = trimmed.trim_end_matches(';').trim();
    if upper == "START TRANSACTION" || upper == "BEGIN" || upper == "BEGIN WORK" {
        return Ok(vec![Statement::StartTransaction]);
    }
    if upper == "COMMIT" || upper == "COMMIT WORK" {
        return Ok(vec![Statement::Commit]);
    }
    if upper == "ROLLBACK" || upper == "ROLLBACK WORK" {
        return Ok(vec![Statement::Rollback]);
    }
    // SAVEPOINT sp_name
    if let Some(rest) = upper.strip_prefix("SAVEPOINT") {
        let sp_name = rest.trim().to_string();
        if !sp_name.is_empty() {
            return Ok(vec![Statement::Savepoint(sp_name)]);
        }
    }
    // ROLLBACK TO sp_name
    if let Some(rest) = upper.strip_prefix("ROLLBACK TO") {
        let sp_name = rest.trim().to_string();
        if !sp_name.is_empty() {
            return Ok(vec![Statement::RollbackTo(sp_name)]);
        }
    }
    // RELEASE SAVEPOINT sp_name
    if let Some(rest) = upper.strip_prefix("RELEASE SAVEPOINT") {
        let sp_name = rest.trim().to_string();
        if !sp_name.is_empty() {
            return Ok(vec![Statement::ReleaseSavepoint(sp_name)]);
        }
    }
    // SET TRANSACTION ISOLATION LEVEL ...
    if upper.starts_with("SET TRANSACTION ISOLATION LEVEL") {
        let level = upper
            .strip_prefix("SET TRANSACTION ISOLATION LEVEL")
            .unwrap_or_default()
            .trim()
            .to_uppercase();
        let isolation_level = match level.as_str() {
            "READ UNCOMMITTED" => "READ UNCOMMITTED",
            "READ COMMITTED" => "READ COMMITTED",
            "REPEATABLE READ" => "REPEATABLE READ",
            "SERIALIZABLE" => "SERIALIZABLE",
            _ => return Err(ParseError::SyntaxError {
                position: 0,
                message: format!("Unknown isolation level: '{}'", level),
            }),
        };
        return Ok(vec![Statement::SetTransactionIsolation(
            isolation_level.to_string(),
        )]);
    }

    // SET GLOBAL / SET SESSION / SET NAMES / SET character set / SET @@global.var / SET @@session.var
    // sqlparser can't handle the GLOBAL/SESSION keyword prefix, so intercept before it.
    if upper.starts_with("SET ") && !upper.starts_with("SET PROPERTIES") {
        if let Some(stmt) = try_parse_set_variable(sql)? {
            return Ok(vec![stmt]);
        }
        // Fall through to sqlparser for SET @user_var = val etc.
    }

    // INSERT ... SET syntax (MySQL compatibility) - convert to INSERT ... VALUES
    // INSERT INTO t SET col1 = val1, col2 = val2 -> INSERT INTO t (col1, col2) VALUES (val1, val2)
    if trimmed.starts_with("INSERT")
        && trimmed.contains(" SET ")
        && !trimmed.contains(" ON DUPLICATE KEY")
    {
        match convert_insert_set_to_values(sql) {
            Ok(converted_sql) => {
                let dialect = sqlparser::dialect::MySqlDialect {};
                let statements = sqlparser::parser::Parser::parse_sql(&dialect, &converted_sql)
                    .map_err(|e| ParseError::SyntaxError {
                        position: 0,
                        message: e.to_string(),
                    })?;
                return statements
                    .into_iter()
                    .map(|s| convert_statement(s))
                    .collect();
            }
            Err(e) => return Err(e),
        }
    }

    // Workaround: sqlparser doesn't support INSERT ... SELECT ... ON DUPLICATE KEY.
    // MySQL requires FROM DUAL in this case. We auto-inject DUAL to make it work.
    let sql_to_parse = fixup_insert_select_on_duplicate(&sql_to_parse);

    let dialect = sqlparser::dialect::MySqlDialect {};
    let statements =
        sqlparser::parser::Parser::parse_sql(&dialect, &sql_to_parse).map_err(|e| {
            ParseError::SyntaxError {
                position: 0,
                message: e.to_string(),
            }
        })?;

    statements
        .into_iter()
        .map(|s| {
            let mut converted = convert_statement(s)?;
            // Attach Doris extensions to CreateTable
            if let Statement::CreateTable(ref mut ct) = converted {
                if doris_distribution.is_some() {
                    ct.distribution = doris_distribution.take();
                }
                if doris_partition.is_some() {
                    ct.partition = doris_partition.take();
                }
                if !doris_properties.is_empty() {
                    ct.properties = doris_properties.clone();
                }
                // Attach table keys from Doris syntax
                ct.keys_type = doris_keys_type;
                ct.unique_keys = doris_unique_keys.clone();
            }
            Ok(converted)
        })
        .collect()
}

/// Split a string on commas, but only when not inside parentheses or quotes
fn split_on_comma(s: &str) -> Vec<String> {
    let mut result = vec![];
    let mut current = String::new();
    let mut paren_depth = 0;
    let mut in_string = false;
    let mut string_char = ' ';
    let mut prev_char = ' ';

    for c in s.chars() {
        if in_string {
            current.push(c);
            if c == string_char && prev_char != '\\' {
                in_string = false;
            }
        } else if c == '\'' || c == '"' {
            current.push(c);
            in_string = true;
            string_char = c;
        } else if c == '(' {
            current.push(c);
            paren_depth += 1;
        } else if c == ')' {
            current.push(c);
            if paren_depth > 0 {
                paren_depth -= 1;
            }
        } else if c == ',' && paren_depth == 0 {
            result.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(c);
        }
        prev_char = c;
    }

    if !current.trim().is_empty() {
        result.push(current.trim().to_string());
    }

    result
}

/// Convert MySQL INSERT ... SET syntax to INSERT ... VALUES syntax
/// INSERT INTO t SET col1 = val1, col2 = val2 -> INSERT INTO t (col1, col2) VALUES (val1, val2)
fn convert_insert_set_to_values(sql: &str) -> Result<String, ParseError> {
    let sql = sql.trim();

    // Extract table name after INSERT INTO
    let after_insert = sql.strip_prefix("INSERT").unwrap_or("").trim();
    let after_into = after_insert
        .strip_prefix("INTO")
        .unwrap_or(after_insert)
        .trim();

    // Find the table name (before SET)
    // Case-insensitive search for " SET " delimiter
    let upper = after_into.to_uppercase();
    let set_pos = upper.find(" SET ").ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "INSERT ... SET syntax requires SET clause".to_string(),
    })?;
    let table_part = &after_into[..set_pos];
    let set_part = &after_into[set_pos + 5..]; // skip " SET "
    let parts = vec![table_part, set_part];

    let table_and_cols = parts[0].trim();
    let set_clause = parts[1].trim();

    // Extract table name (could have columns in parentheses)
    let table_name: &str;
    let columns_part: Option<&str>;

    if table_and_cols.ends_with(')') {
        // Has column list: INSERT INTO t (col1, col2)
        if let Some(paren_start) = table_and_cols.find('(') {
            table_name = table_and_cols[..paren_start].trim();
            columns_part = Some(&table_and_cols[paren_start..]);
        } else {
            return Err(ParseError::SyntaxError {
                position: 0,
                message: "Invalid INSERT syntax".to_string(),
            });
        }
    } else {
        // No column list: INSERT INTO t
        table_name = table_and_cols;
        columns_part = None;
    }

    // Parse the SET clause: col1 = val1, col2 = val2
    // Use split_on_comma to handle commas inside function calls
    let assignments = split_on_comma(set_clause);
    let mut col_names: Vec<&str> = vec![];
    let mut values: Vec<String> = vec![];

    for assignment in &assignments {
        let parts: Vec<&str> = assignment.splitn(2, '=').collect();
        if parts.len() != 2 {
            continue; // Skip malformed assignments
        }
        col_names.push(parts[0].trim());
        values.push(parts[1].trim().to_string());
    }

    // Build the converted SQL
    let cols_str = if let Some(cols) = columns_part {
        cols.to_string()
    } else if col_names.is_empty() {
        "()".to_string()
    } else {
        format!("({})", col_names.join(", "))
    };

    let vals_str = if values.is_empty() {
        "()".to_string()
    } else {
        format!("({})", values.join(", "))
    };

    Ok(format!(
        "INSERT INTO {} {} VALUES {}",
        table_name, cols_str, vals_str
    ))
}

/// Split a potentially dot-qualified name into (database, object_name).
fn split_qualified_name(name: &str) -> (Option<String>, String) {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.len() {
        3 => (Some(parts[1].to_string()), parts[2].to_string()), // catalog.db.table
        2 => (Some(parts[0].to_string()), parts[1].to_string()), // db.table
        _ => (None, name.to_string()),
    }
}

fn parse_create_repository(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_create = strip_prefix_ci(sql, "CREATE REPOSITORY")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CREATE REPOSITORY".to_string(),
        })?
        .trim();

    let (name, rest) = extract_identifier(after_create).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected repository name".to_string(),
    })?;

    let rest = rest.trim();
    let mut repo_type = RepositoryType::Local;
    let mut properties = vec![];

    let rest_upper = rest.to_uppercase();
    if rest_upper.starts_with("WITH") {
        let after_with = strip_prefix_ci(rest, "WITH").unwrap_or_default().trim();
        let after_with_upper = after_with.to_uppercase();
        if after_with_upper.starts_with("S3") {
            repo_type = RepositoryType::S3;
            let after_s3 = strip_prefix_ci(after_with, "S3").unwrap_or("").trim();
            properties = parse_properties(after_s3);
        } else if after_with_upper.starts_with("HDFS") {
            repo_type = RepositoryType::Hdfs;
            let after_hdfs = strip_prefix_ci(after_with, "HDFS").unwrap_or("").trim();
            properties = parse_properties(after_hdfs);
        } else {
            properties = parse_properties(after_with);
        }
    }

    Ok(vec![Statement::CreateRepository(CreateRepositoryStmt {
        name: name.to_string(),
        repo_type,
        properties,
    })])
}

fn parse_show_create_database(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_show = strip_prefix_ci(sql, "SHOW CREATE DATABASE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW CREATE DATABASE".to_string(),
        })?
        .trim();

    let (db_name, _) = extract_identifier(after_show).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected database name".to_string(),
    })?;

    Ok(vec![Statement::ShowCreateDatabase(db_name.to_string())])
}

fn parse_show_create_view(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_show = strip_prefix_ci(sql, "SHOW CREATE VIEW")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW CREATE VIEW".to_string(),
        })?
        .trim();

    let (name, _) = extract_identifier(after_show).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected view name".to_string(),
    })?;

    let name_str = name.to_string();
    let (db, view_name) = split_qualified_name(&name_str);
    let db = db.unwrap_or_default();

    Ok(vec![Statement::ShowCreateView(db, view_name)])
}

fn parse_show_partitions(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_show = strip_prefix_ci(sql, "SHOW PARTITIONS")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW PARTITIONS".to_string(),
        })?
        .trim();

    let from_pos = find_ascii_ci(after_show, " FROM ");
    let table_name = if let Some(pos) = from_pos {
        let after_from = after_show[pos + 6..].trim();
        let (name, _) = extract_identifier(after_from).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected table name".to_string(),
        })?;
        name.to_string()
    } else {
        return Err(ParseError::SyntaxError {
            position: 0,
            message: "Expected FROM clause".to_string(),
        });
    };

    let name_str = table_name.to_string();
    let (db, table) = split_qualified_name(&name_str);
    let db = db.unwrap_or_default();

    Ok(vec![Statement::ShowPartitions(db, table)])
}

fn parse_show_table_status(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_show = strip_prefix_ci(sql, "SHOW TABLE STATUS")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW TABLE STATUS".to_string(),
        })?
        .trim();

    let mut db_name = None;
    if strip_prefix_ci(after_show, "FROM ").is_some() {
        let after_from = after_show[5..].trim();
        let (name, _) = extract_identifier(after_from).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected database name".to_string(),
        })?;
        db_name = Some(name.to_string());
    }

    Ok(vec![Statement::ShowTableStatus(db_name)])
}

/// Try to parse a SET variable statement.
/// Returns Ok(None) if the SQL is not a simple SET variable (e.g. SET @user_var — let sqlparser handle it).
/// Returns Ok(Some(stmt)) if successfully parsed.
/// Returns Err if this is clearly a SET variable but malformed.
fn try_parse_set_variable(sql: &str) -> Result<Option<Statement>, ParseError> {
    let trimmed = sql.trim().trim_end_matches(';');
    let upper = trimmed.to_uppercase();
    let after_set_upper = upper.strip_prefix("SET ").unwrap_or_default().trim();
    // Original-case remainder after "SET "
    let after_set_orig = strip_prefix_ci(trimmed, "SET ").unwrap_or(trimmed).trim();

    // Detect scope: GLOBAL or SESSION — use uppercased for matching, original for values
    let (is_global, rest, rest_orig) = if after_set_upper.starts_with("GLOBAL ") || after_set_upper.starts_with("GLOBAL\t")
    {
        let orig = strip_prefix_ci(after_set_orig, "GLOBAL ").or_else(|| strip_prefix_ci(after_set_orig, "GLOBAL\t")).unwrap_or_default().trim();
        (
            true,
            after_set_upper.strip_prefix("GLOBAL ").or_else(|| after_set_upper.strip_prefix("GLOBAL\t")).unwrap_or_default().trim(),
            orig,
        )
    } else if after_set_upper.starts_with("SESSION ") || after_set_upper.starts_with("SESSION\t") {
        let orig = strip_prefix_ci(after_set_orig, "SESSION ").or_else(|| strip_prefix_ci(after_set_orig, "SESSION\t")).unwrap_or_default().trim();
        (
            false,
            after_set_upper.strip_prefix("SESSION ").or_else(|| after_set_upper.strip_prefix("SESSION\t")).unwrap_or_default().trim(),
            orig,
        )
    } else {
        (false, after_set_upper, after_set_orig)
    };

    // Handle SET NAMES 'charset' / SET NAMES charset
    if !is_global && rest.starts_with("NAMES") {
        let charset = strip_prefix_ci(rest_orig, "NAMES")
            .unwrap_or_default()
            .trim()
            .trim_matches('\'')
            .trim_matches('"');
        return Ok(Some(Statement::SetVariable(SetVariableStmt {
            variable: "character_set_all".to_string(),
            value: Expr::Literal(LiteralValue::String(charset.to_string())),
            is_global: false,
        })));
    }

    // Handle SET CHARACTER SET ... (same as SET NAMES for our purposes)
    if !is_global && rest.starts_with("CHARACTER SET") {
        let charset = strip_prefix_ci(rest_orig, "CHARACTER SET")
            .unwrap_or_default()
            .trim()
            .trim_matches('\'')
            .trim_matches('"');
        return Ok(Some(Statement::SetVariable(SetVariableStmt {
            variable: "character_set_all".to_string(),
            value: Expr::Literal(LiteralValue::String(charset.to_string())),
            is_global: false,
        })));
    }

    // Handle @@global.var = val / @@session.var = val / @@var = val
    let (scope_from_at, rest, rest_orig) = if rest.starts_with("@@GLOBAL.") {
        (
            Some(true),
            rest.strip_prefix("@@GLOBAL.").unwrap_or_default().trim(),
            strip_prefix_ci(rest_orig, "@@GLOBAL.").unwrap_or_default().trim(),
        )
    } else if rest.starts_with("@@SESSION.") {
        (
            Some(false),
            rest.strip_prefix("@@SESSION.").unwrap_or_default().trim(),
            strip_prefix_ci(rest_orig, "@@SESSION.").unwrap_or_default().trim(),
        )
    } else if rest.starts_with("@@") {
        // just @@var — treat as session
        (
            Some(false),
            rest.strip_prefix("@@").unwrap_or_default().trim(),
            strip_prefix_ci(rest_orig, "@@").unwrap_or_default().trim(),
        )
    } else {
        (None, rest, rest_orig)
    };

    let effective_global = scope_from_at.unwrap_or(is_global);

    // Skip user variables (@var) — let sqlparser handle them
    if rest.starts_with('@') {
        return Ok(None);
    }

    // Parse: var_name = value  OR  var_name := value  OR  var_name TO value
    // Use `rest` (uppercased) for keyword detection, `rest_orig` for value extraction
    let (var_name, value_str) = if let Some(eq_pos) = rest.find(":=") {
        let name = rest_orig[..eq_pos].trim().trim_matches('`').trim_matches('"');
        let val = rest_orig[eq_pos + 2..].trim();
        (name, val)
    } else if let Some(eq_pos) = rest.find('=') {
        let name = rest_orig[..eq_pos].trim().trim_matches('`').trim_matches('"');
        let val = rest_orig[eq_pos + 1..].trim();
        (name, val)
    } else if let Some(to_pos) = find_ascii_ci(rest, " TO ") {
        let name = rest_orig[..to_pos].trim().trim_matches('`').trim_matches('"');
        let val = rest_orig[to_pos + 4..].trim();
        (name, val)
    } else {
        // No assignment — might be something sqlparser handles
        return Ok(None);
    };

    if var_name.is_empty() {
        return Ok(None);
    }

    // Parse value into Expr
    let value_expr = parse_set_value(value_str);

    Ok(Some(Statement::SetVariable(SetVariableStmt {
        variable: var_name.to_lowercase(),
        value: value_expr,
        is_global: effective_global,
    })))
}

/// Parse a SET value string into an Expr.
fn parse_set_value(s: &str) -> Expr {
    let s = s.trim();

    // Quoted string
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        let inner = &s[1..s.len() - 1];
        // Handle escaped quotes
        let unescaped = inner
            .replace("\\'", "'")
            .replace("\\\"", "\"")
            .replace("\\\\", "\\");
        return Expr::Literal(LiteralValue::String(unescaped));
    }

    // Boolean-like
    let upper = s.to_uppercase();
    if upper == "TRUE" || upper == "ON" {
        return Expr::Literal(LiteralValue::Boolean(true));
    }
    if upper == "FALSE" || upper == "OFF" {
        return Expr::Literal(LiteralValue::Boolean(false));
    }
    if upper == "NULL" || upper == "DEFAULT" {
        return Expr::Literal(LiteralValue::Null);
    }

    // Integer
    if let Ok(n) = s.parse::<i64>() {
        return Expr::Literal(LiteralValue::Int64(n));
    }

    // Float
    if let Ok(f) = s.parse::<f64>() {
        return Expr::Literal(LiteralValue::Float64(f));
    }

    // Unquoted string (e.g. utf8mb4, STRICT_TRANS_TABLES)
    Expr::Literal(LiteralValue::String(s.to_string()))
}

fn parse_show_variables(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let sql_upper = sql.to_uppercase();
    let after_show_upper = sql_upper
        .strip_prefix("SHOW VARIABLES")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW VARIABLES".to_string(),
        })?
        .trim();
    let after_show = strip_prefix_ci(sql, "SHOW VARIABLES").unwrap_or("").trim();

    let mut global = false;
    let mut pattern = None;

    if after_show_upper.starts_with("GLOBAL ") {
        global = true;
        let rest = strip_prefix_ci(after_show, "GLOBAL ").unwrap_or_default().trim();
        if rest.len() >= 5 && rest[..5].eq_ignore_ascii_case("LIKE ") {
            pattern = Some(rest[5..].trim().trim_matches('\'').to_string());
        } else {
            pattern = Some(rest.to_string());
        }
    } else if after_show_upper.starts_with("SESSION ") {
        let rest = strip_prefix_ci(after_show, "SESSION ").unwrap_or_default().trim();
        if rest.len() >= 5 && rest[..5].eq_ignore_ascii_case("LIKE ") {
            pattern = Some(rest[5..].trim().trim_matches('\'').to_string());
        } else {
            pattern = Some(rest.to_string());
        }
    } else if after_show_upper.starts_with("LIKE ") {
        pattern = Some(after_show[5..].trim().trim_matches('\'').to_string());
    } else if !after_show.is_empty() {
        pattern = Some(after_show.to_string());
    }

    Ok(vec![Statement::ShowVariables { global, pattern }])
}

fn parse_show_processlist(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql_upper = sql.trim().to_uppercase();
    let after_show = sql_upper
        .strip_prefix("SHOW PROCESSLIST")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW PROCESSLIST".to_string(),
        })?
        .trim();

    let full = after_show.contains("FULL");

    Ok(vec![Statement::ShowProcesslist(full)])
}

fn parse_show_status(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let sql_upper = sql.to_uppercase();
    // Use uppercased for keyword matching
    let after_show_upper = sql_upper
        .strip_prefix("SHOW")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW STATUS".to_string(),
        })?
        .trim();

    // Parse: SHOW [GLOBAL|SESSION] STATUS [LIKE pattern]
    // MySQL default: SESSION status (not GLOBAL)
    let mut global = false;
    let mut rest_upper = after_show_upper;

    // Skip "STATUS" keyword
    rest_upper = rest_upper.strip_prefix("STATUS").unwrap_or(rest_upper).trim();

    // Check for GLOBAL/SESSION before STATUS (re-parse from beginning)
    if after_show_upper.starts_with("GLOBAL") {
        global = true;
        rest_upper = after_show_upper.strip_prefix("GLOBAL").unwrap_or("").trim();
        rest_upper = rest_upper.strip_prefix("STATUS").unwrap_or(rest_upper).trim();
    } else if after_show_upper.starts_with("SESSION") {
        global = false;
        rest_upper = after_show_upper.strip_prefix("SESSION").unwrap_or("").trim();
        rest_upper = rest_upper.strip_prefix("STATUS").unwrap_or(rest_upper).trim();
    } else if after_show_upper.starts_with("STATUS") {
        rest_upper = after_show_upper.strip_prefix("STATUS").unwrap_or("").trim();
    }

    // Use original SQL for pattern extraction — compute offset from uppercased parsing
    let after_show_orig = strip_prefix_ci(sql, "SHOW").unwrap_or("").trim();
    let kw_prefix = if after_show_upper.starts_with("GLOBAL") {
        "GLOBAL"
    } else if after_show_upper.starts_with("SESSION") {
        "SESSION"
    } else {
        ""
    };
    let rest = if !kw_prefix.is_empty() {
        let after_kw = strip_prefix_ci(after_show_orig, kw_prefix).unwrap_or("").trim();
        strip_prefix_ci(after_kw, "STATUS").unwrap_or(after_kw).trim()
    } else {
        strip_prefix_ci(after_show_orig, "STATUS").unwrap_or(after_show_orig).trim()
    };

    let mut pattern = None;
    if rest_upper.starts_with("LIKE ") {
        pattern = Some(rest[5..].trim().trim_matches('\'').to_string());
    } else if !rest.is_empty() {
        pattern = Some(rest.to_string());
    }

    Ok(vec![Statement::ShowStatus { global, pattern }])
}

fn parse_show_collation(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after_show = strip_prefix_ci(sql, "SHOW COLLATION")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW COLLATION".to_string(),
        })?
        .trim();

    let pattern = if after_show.is_empty() {
        None
    } else if strip_prefix_ci(after_show, "LIKE").is_some() {
        let rest = strip_prefix_ci(after_show, "LIKE").unwrap().trim();
        Some(rest.trim_matches('\'').to_string())
    } else {
        Some(after_show.to_string())
    };

    Ok(vec![Statement::ShowCollation { pattern }])
}

fn parse_show_index(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let sql_upper = sql.to_uppercase();
    let _after_show_upper = sql_upper
        .strip_prefix("SHOW INDEX")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW INDEX".to_string(),
        })?
        .trim();
    let after_show = strip_prefix_ci(sql, "SHOW INDEX").unwrap_or("").trim();

    let from_pos = find_ascii_ci(after_show, "FROM");
    let (db, table_name) = if let Some(pos) = from_pos {
        let after_from = after_show[pos + 4..].trim();
        let (name, _) = extract_identifier(after_from).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected table name".to_string(),
        })?;
        let name_str = name.to_string().to_lowercase();
        let (db, table_name) = split_qualified_name(&name_str);
        (db.unwrap_or_default(), table_name)
    } else {
        return Err(ParseError::SyntaxError {
            position: 0,
            message: "Expected FROM clause".to_string(),
        });
    };

    Ok(vec![Statement::ShowIndex(db, table_name)])
}

fn parse_show_alter_table(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_show = strip_prefix_ci(sql, "SHOW ALTER TABLE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW ALTER TABLE".to_string(),
        })?
        .trim();

    let mut db_name = None;
    if strip_prefix_ci(after_show, "FROM ").is_some() {
        let after_from = after_show[5..].trim();
        let (name, _) = extract_identifier(after_from).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected database name".to_string(),
        })?;
        db_name = Some(name.to_string());
    }

    Ok(vec![Statement::ShowAlterTable(db_name)])
}

fn parse_show_view(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_show = strip_prefix_ci(sql, "SHOW VIEW")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW VIEW".to_string(),
        })?
        .trim();

    let from_pos = find_ascii_ci(after_show, " FROM ");
    let table_name = if let Some(pos) = from_pos {
        let after_from = after_show[pos + 6..].trim();
        let (name, _) = extract_identifier(after_from).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected table name".to_string(),
        })?;
        name.to_string()
    } else {
        return Err(ParseError::SyntaxError {
            position: 0,
            message: "Expected FROM clause".to_string(),
        });
    };

    let name_str = table_name.to_string();
    let (db, view_name) = split_qualified_name(&name_str);
    let db = db.unwrap_or_default();

    Ok(vec![Statement::ShowView(db, view_name)])
}

fn parse_show_create_materialized_view(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_show = strip_prefix_ci(sql, "SHOW CREATE MATERIALIZED VIEW")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW CREATE MATERIALIZED VIEW".to_string(),
        })?
        .trim();

    let (name, _) = extract_identifier(after_show).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected materialized view name".to_string(),
    })?;

    Ok(vec![Statement::ShowCreateMaterializedView(
        name.to_string(),
    )])
}

fn parse_drop_repository(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_drop = strip_prefix_ci(sql, "DROP REPOSITORY")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP REPOSITORY".to_string(),
        })?
        .trim();

    let if_exists = after_drop.to_uppercase().starts_with("IF EXISTS");
    let name_part = if if_exists {
        strip_prefix_ci(after_drop, "IF EXISTS").unwrap_or_default().trim()
    } else {
        after_drop
    };

    let (name, _) = extract_identifier(name_part).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected repository name".to_string(),
    })?;

    Ok(vec![Statement::DropRepository(DropRepositoryStmt {
        name: name.to_string(),
        if_exists,
    })])
}

fn parse_backup_database(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_backup = strip_prefix_ci(sql, "BACKUP DATABASE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected BACKUP DATABASE".to_string(),
        })?
        .trim();

    let (database, rest) =
        extract_identifier(after_backup).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected database name".to_string(),
        })?;

    let rest = rest.trim();
    let rest = strip_prefix_ci(rest, "TO")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected TO".to_string(),
        })?
        .trim();

    let (repository, rest) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected repository name".to_string(),
    })?;

    let rest = rest.trim();
    let backup_name = if strip_prefix_ci(rest, "BACKUP").is_some() {
        let after_backup = strip_prefix_ci(rest, "BACKUP").unwrap_or_default().trim();
        let (name, _) =
            extract_identifier(after_backup).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected backup name".to_string(),
            })?;
        name.to_string()
    } else {
        format!("{}_{}", database, chrono_lite_timestamp())
    };

    let properties = parse_properties(rest);

    Ok(vec![Statement::BackupDatabase(BackupDatabaseStmt {
        database: database.to_string(),
        repository: repository.to_string(),
        backup_name,
        properties,
    })])
}

fn parse_restore_database(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_restore = strip_prefix_ci(sql, "RESTORE DATABASE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected RESTORE DATABASE".to_string(),
        })?
        .trim();

    let (database, rest) =
        extract_identifier(after_restore).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected database name".to_string(),
        })?;

    let rest = rest.trim();
    let rest = strip_prefix_ci(rest, "FROM")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected FROM".to_string(),
        })?
        .trim();

    let (repository, rest) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected repository name".to_string(),
    })?;

    let rest = rest.trim();
    let rest = strip_prefix_ci(rest, "BACKUP")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected BACKUP".to_string(),
        })?
        .trim();

    let (backup_name, _) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected backup name".to_string(),
    })?;

    Ok(vec![Statement::RestoreDatabase(RestoreDatabaseStmt {
        database: database.to_string(),
        repository: repository.to_string(),
        backup_name: backup_name.to_string(),
        properties: vec![],
    })])
}

fn parse_create_materialized_view(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_create = strip_prefix_ci(sql, "CREATE MATERIALIZED VIEW")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CREATE MATERIALIZED VIEW".to_string(),
        })?
        .trim();

    let if_not_exists = after_create.to_uppercase().starts_with("IF NOT EXISTS");
    let rest = if if_not_exists {
        strip_prefix_ci(after_create, "IF NOT EXISTS").unwrap_or_default().trim()
    } else {
        after_create
    };

    let (name, rest) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected materialized view name".to_string(),
    })?;

    let rest = rest.trim();
    let name_str = name.to_string();
    let (database, view_name) = split_qualified_name(&name_str);

    let mut columns = Vec::new();
    let mut query = String::new();
    let mut refresh = None;

    if rest.starts_with('(') {
        if let Some(end) = rest.find(')') {
            let cols_str = &rest[1..end];
            for col in cols_str.split(',') {
                columns.push(col.trim().to_string());
            }
            query = rest[end + 1..].trim().to_string();
        }
    } else {
        let as_pos = find_ascii_ci(&rest, " AS ");
        if let Some(pos) = as_pos {
            let after_as = &rest[pos + 4..];
            query = after_as.trim().to_string();
        }
    }

    if let Some(refresh_pos) = find_ascii_ci(&query, "REFRESH") {
        let before_refresh = query[..refresh_pos].trim();
        if !before_refresh.is_empty() && before_refresh != "AS" {
            query = before_refresh.to_string();
        }
        let refresh_start = query[refresh_pos..]
            .find("COMPLETE")
            .or_else(|| query[refresh_pos..].find("FAST"))
            .map(|p| p + refresh_pos);

        if let Some(pos) = refresh_start {
            let refresh_str = &query[pos..];
            if refresh_str.to_uppercase().starts_with("COMPLETE") {
                refresh = Some(RefreshClause {
                    r#type: RefreshType::Complete,
                    concurrency: None,
                });
            } else if refresh_str.to_uppercase().starts_with("FAST") {
                refresh = Some(RefreshClause {
                    r#type: RefreshType::Fast,
                    concurrency: None,
                });
            }
        }
    }

    Ok(vec![Statement::CreateMaterializedView(
        CreateMaterializedViewStmt {
            database,
            name: view_name,
            if_not_exists,
            query: {
                let q = query.trim();
                let upper = q.to_uppercase();
                if let Some(pos) = upper.find(" AS ") {
                    q[pos + 4..].trim().to_string()
                } else if upper.starts_with("AS ") {
                    q[3..].trim().to_string()
                } else {
                    q.to_string()
                }
            },
            columns,
            refresh,
        },
    )])
}

fn parse_drop_materialized_view(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_drop = strip_prefix_ci(sql, "DROP MATERIALIZED VIEW")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP MATERIALIZED VIEW".to_string(),
        })?
        .trim();

    let if_exists = after_drop.to_uppercase().starts_with("IF EXISTS");
    let rest = if if_exists {
        strip_prefix_ci(after_drop, "IF EXISTS").unwrap_or_default().trim()
    } else {
        after_drop
    };

    let (name, _) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected materialized view name".to_string(),
    })?;

    let name_str = name.to_string();
    let (database, view_name) = split_qualified_name(&name_str);

    Ok(vec![Statement::DropMaterializedView(
        DropMaterializedViewStmt {
            database,
            name: view_name,
            if_exists,
        },
    )])
}

fn parse_alter_materialized_view(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_alter = strip_prefix_ci(sql, "ALTER MATERIALIZED VIEW")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected ALTER MATERIALIZED VIEW".to_string(),
        })?
        .trim();

    let (name, rest) = extract_identifier(after_alter).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected materialized view name".to_string(),
    })?;

    let name_str = name.to_string();
    let (database, view_name) = split_qualified_name(&name_str);

    let rest = rest.trim();
    let operation = if rest.to_uppercase().starts_with("PAUSE REFRESH") {
        AlterMaterializedViewOperation::PauseRefresh
    } else if rest.to_uppercase().starts_with("RESUME REFRESH") {
        AlterMaterializedViewOperation::ResumeRefresh
    } else if rest.to_uppercase().starts_with("RENAME TO ") {
        let prefix_len = "RENAME TO ".len();
        let new_name = rest[prefix_len..].trim();
        AlterMaterializedViewOperation::Rename(new_name.to_string())
    } else {
        return Err(ParseError::SyntaxError {
            position: 0,
            message: format!("Unknown ALTER MATERIALIZED VIEW operation: {}", rest),
        });
    };

    Ok(vec![Statement::AlterMaterializedView(
        AlterMaterializedViewStmt {
            database,
            name: view_name,
            operation,
        },
    )])
}

fn parse_refresh_materialized_view(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_refresh = strip_prefix_ci(sql, "REFRESH MATERIALIZED VIEW")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected REFRESH MATERIALIZED VIEW".to_string(),
        })?
        .trim();

    let (name, rest) =
        extract_identifier(after_refresh).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected materialized view name".to_string(),
        })?;

    let name_str = name.to_string();
    let (database, view_name) = split_qualified_name(&name_str);

    let refresh_type = if rest.trim().to_uppercase().starts_with("COMPLETE") {
        RefreshType::Complete
    } else {
        RefreshType::Fast
    };

    Ok(vec![Statement::RefreshMaterializedView(
        RefreshMaterializedViewStmt {
            database,
            name: view_name,
            refresh_type,
        },
    )])
}

/// Result of preprocessing a CREATE TABLE statement
struct PreprocessedCreateTable {
    /// SQL with Doris-specific extensions removed
    clean_sql: String,
    /// DISTRIBUTED BY clause info
    distribution: Option<DistributionDef>,
    /// PARTITION BY clause info
    partition: Option<PartitionDef>,
    /// PROPERTIES clause info
    properties: Vec<(String, String)>,
    /// Table keys type (UNIQUE, DUPLICATE, AGGREGATE, PRIMARY)
    keys_type: KeysType,
    /// Unique key definitions
    unique_keys: Vec<UniqueKeyDef>,
}

fn preprocess_create_table(sql: &str) -> PreprocessedCreateTable {
    let mut clean_sql = sql.to_string();
    let mut distribution: Option<DistributionDef> = None;
    let mut partition: Option<PartitionDef> = None;
    let mut properties: Vec<(String, String)> = vec![];
    let mut keys_type = KeysType::Duplicate;
    let mut unique_keys: Vec<UniqueKeyDef> = vec![];

    // Extract table keys: UNIQUE KEY(col), UNIQUE KEY name(col), DUPLICATE KEY(col), AGGREGATE KEY(col)
    // Also handle: UNIQUE(col), UNIQUE name(col) without KEY keyword
    // These can appear before DISTRIBUTED BY
    // Note: Order matters! "UNIQUE KEY" must come before "UNIQUE" to avoid partial matches
    let key_types = [
        "PRIMARY KEY",
        "UNIQUE KEY",
        "DUPLICATE KEY",
        "AGGREGATE KEY",
        "UNIQUE",
    ];
    for key_type in &key_types {
        if *key_type == "UNIQUE" {
            // For standalone "UNIQUE", find it only after a comma or opening paren
            // to avoid matching "UNIQUE" inside table names like "test_unique"
            let mut search_pos = 0;
            while let Some(key_pos) = find_ascii_ci(&sql[search_pos..], "UNIQUE") {
                let abs_pos = search_pos + key_pos;
                let rest = &sql[abs_pos..];
                // Check this isn't actually "UNIQUE KEY" (handled by another iteration)
                if find_ascii_ci(rest, "UNIQUE KEY") == Some(0) {
                    search_pos = abs_pos + "UNIQUE".len();
                    continue;
                }

                // Check valid context: "UNIQUE" should be preceded by ',' or '('
                let bytes = sql.as_bytes();
                let mut valid_context = false;
                if abs_pos >= 1 {
                    let mut scan = abs_pos - 1;
                    // Skip whitespace bytes
                    while scan > 0 && (bytes[scan] == b' ' || bytes[scan] == b'\t') {
                        scan -= 1;
                    }
                    if bytes[scan] == b',' || bytes[scan] == b'(' {
                        valid_context = true;
                    }
                }

                if !valid_context {
                    search_pos = abs_pos + "UNIQUE".len();
                    continue;
                }

                // Process standalone "UNIQUE"
                let after_unique = &sql[abs_pos..];
                if let Some(paren_pos) = after_unique.find('(') {
                    let cols_start = abs_pos + paren_pos + 1;
                    let remaining = &after_unique[paren_pos + 1..];
                    if let Some(end_paren) = find_matching_paren(remaining) {
                        let cols_str = &remaining[..end_paren];
                        let columns: Vec<String> =
                            cols_str.split(',').map(|s| s.trim().to_string()).collect();

                        keys_type = KeysType::Unique;
                        let before_cols = &after_unique[..paren_pos];
                        let name = if before_cols.trim().is_empty() {
                            None
                        } else {
                            Some(before_cols.trim().to_string())
                        };
                        unique_keys.push(UniqueKeyDef { name, columns });

                        // Remove this key clause from clean_sql
                        let key_end = cols_start + end_paren + 1;
                        let mut clean_start = abs_pos;
                        if clean_start > 0 && clean_sql[..clean_start].trim().ends_with(',') {
                            clean_start =
                                clean_sql[..clean_start].trim().trim_end_matches(',').len();
                        }
                        clean_sql = format!(
                            "{}{}",
                            clean_sql[..clean_start].trim(),
                            clean_sql[key_end..].trim()
                        );
                    }
                }
                break; // Only process one standalone "UNIQUE" per iteration
            }
        } else if let Some(key_pos) = find_ascii_ci(&sql, key_type) {
            // Find the opening parenthesis after the key type
            let after_key = &sql[key_pos..];
            if let Some(paren_pos) = after_key.find('(') {
                let cols_start = key_pos + paren_pos + 1;
                // Find matching closing parenthesis
                let remaining = &after_key[paren_pos + 1..];
                if let Some(end_paren) = find_matching_paren(remaining) {
                    let cols_str = &remaining[..end_paren];
                    let columns: Vec<String> =
                        cols_str.split(',').map(|s| s.trim().to_string()).collect();

                    // Determine keys_type
                    match *key_type {
                        "PRIMARY KEY" => {
                            keys_type = KeysType::Primary;
                            let before_cols = &after_key[..paren_pos];
                            let name = if before_cols.trim().ends_with("KEY")
                                || before_cols.trim().is_empty()
                            {
                                None
                            } else {
                                Some(before_cols.trim().to_string())
                            };
                            unique_keys.push(UniqueKeyDef { name, columns });
                        }
                        "UNIQUE KEY" => {
                            keys_type = KeysType::Unique;
                            // Extract optional constraint name before column list
                            // For "UNIQUE KEY name(col)" or "UNIQUE name(col)"
                            let before_cols = &after_key[..paren_pos];
                            let name = if before_cols.trim().ends_with("KEY")
                                || before_cols.trim().is_empty()
                            {
                                None
                            } else {
                                Some(before_cols.trim().to_string())
                            };
                            unique_keys.push(UniqueKeyDef { name, columns });
                        }
                        "DUPLICATE KEY" => {
                            keys_type = KeysType::Duplicate;
                            unique_keys.push(UniqueKeyDef {
                                name: None,
                                columns,
                            });
                        }
                        "AGGREGATE KEY" => {
                            keys_type = KeysType::Aggregate;
                            unique_keys.push(UniqueKeyDef {
                                name: None,
                                columns,
                            });
                        }
                        _ => {}
                    }

                    // Remove this key clause from clean_sql
                    // Also remove trailing comma before the key clause if present
                    let key_end = cols_start + end_paren + 1;
                    let mut clean_start = key_pos;
                    // Check if there's a trailing comma before the key clause
                    if clean_start > 0 && clean_sql[..clean_start].trim().ends_with(',') {
                        clean_start = clean_sql[..clean_start].trim().trim_end_matches(',').len();
                    }
                    clean_sql = format!(
                        "{}{}",
                        clean_sql[..clean_start].trim(),
                        clean_sql[key_end..].trim()
                    );
                }
            }
        }
    }

    // Strip aggregate column type modifiers: INT SUM, DOUBLE MAX, VARCHAR REPLACE, etc.
    // These are Doris aggregate table syntax that sqlparser doesn't understand
    // We remove only the aggregate modifier (SUM, MAX, etc.), not the type itself
    // Pattern: "TYPE AGG" -> "TYPE" where TYPE is INT, BIGINT, DOUBLE, etc.
    let agg_types = ["SUM", "MAX", "MIN", "REPLACE", "HLL", "BITMAP", "QUANTILE"];
    let sql_types = [
        "INT", "BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL", "VARCHAR", "CHAR",
        "DATE", "DATETIME", "TEXT", "BLOB", "LARGEINT",
    ];
    for type_kw in &sql_types {
        for agg in &agg_types {
            let pattern = format!("{} {}", type_kw.to_uppercase(), agg.to_uppercase());
            if let Some(abs_pos) = find_ascii_ci(&clean_sql, &pattern) {
                let type_len = type_kw.len();
                let agg_len = agg.len();
                let remove_start = abs_pos + type_len;
                let remove_end = abs_pos + type_len + 1 + agg_len;
                let mut end_pos = remove_end;
                while end_pos < clean_sql.len() && clean_sql.as_bytes()[end_pos] == b' ' {
                    end_pos += 1;
                }
                clean_sql = format!("{}{}", &clean_sql[..remove_start], &clean_sql[end_pos..]);
                break; // re-evaluate with updated clean_sql
            }
        }
    }

    // Extract DISTRIBUTED BY HASH(col1, col2) BUCKETS N
    if let Some(dist_pos) = find_ascii_ci(&clean_sql, "DISTRIBUTED BY") {
        let dist_clause = &clean_sql[dist_pos..];

        if let Some(hash_pos) = find_ascii_ci(dist_clause, "HASH") {
            let after_hash = dist_clause[hash_pos + 4..].trim_start();
            if after_hash.starts_with('(') {
                if let Some(end_paren) = after_hash.find(')') {
                    let cols_str = &after_hash[1..end_paren];
                    let columns: Vec<String> =
                        cols_str.split(',').map(|s| s.trim().to_string()).collect();
                    let remaining = after_hash[end_paren + 1..].trim();
                    let buckets = if find_ascii_ci(remaining, "BUCKETS") == Some(0) {
                        remaining[7..]
                            .trim()
                            .split_whitespace()
                            .next()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(1)
                    } else {
                        1
                    };
                    distribution = Some(DistributionDef {
                        dist_type: "HASH".to_string(),
                        columns,
                        buckets,
                    });
                }
            }
        }

        clean_sql = clean_sql[..dist_pos].trim().to_string();
    }

    // Extract PROPERTIES (...) from clean_sql
    if let Some(prop_pos) = rfind_ascii_ci(&clean_sql, "PROPERTIES") {
        let props_part = &clean_sql[prop_pos..];
        properties = parse_properties(props_part);
        clean_sql = clean_sql[..prop_pos].trim().to_string();
    }

    // Extract PARTITION BY RANGE/LIST/HASH (...) from clean_sql
    if let Some(part_pos) = find_ascii_ci(&clean_sql, "PARTITION BY") {
        clean_sql = clean_sql[..part_pos].trim().to_string();
        // Partition parsing can be enhanced later
        partition = None;
    }

    PreprocessedCreateTable {
        clean_sql,
        distribution,
        partition,
        properties,
        keys_type,
        unique_keys,
    }
}

/// Find the index of the matching closing parenthesis
fn find_matching_paren(s: &str) -> Option<usize> {
    let mut depth = 1;
    let bytes = s.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let c = bytes[i];
        // Skip string literals (single-quoted and double-quoted)
        if c == b'\'' || c == b'"' {
            let quote = c;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i += 2; // Skip escaped character
                    continue;
                }
                if bytes[i] == quote {
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if c == b'(' {
            depth += 1;
        } else if c == b')' {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn extract_identifier(s: &str) -> Option<(&str, &str)> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    if s.starts_with('"') || s.starts_with('\'') {
        let quote = s.chars().next().unwrap_or('"');
        let rest = &s[1..];
        let end = rest.find(quote).unwrap_or(0);
        let identifier = &rest[..end];
        let remaining = &rest[end + 1..];
        Some((identifier, remaining))
    } else {
        let end = s
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(s.len());
        if end == 0 {
            return None;
        }
        let identifier = &s[..end];
        let remaining = &s[end..];
        Some((identifier, remaining))
    }
}

fn parse_properties(s: &str) -> Vec<(String, String)> {
    let s = s.trim();
    if !s.starts_with("PROPERTIES") {
        return vec![];
    }

    let props_str = s.strip_prefix("PROPERTIES").unwrap_or_default().trim();
    if !props_str.starts_with('(') || !props_str.ends_with(')') {
        return vec![];
    }

    let content = &props_str[1..props_str.len() - 1];
    let mut props = vec![];

    for pair in content.split(',') {
        let pair = pair.trim();
        if let Some((key, value)) = pair.split_once('=') {
            let key = key.trim().trim_matches('"').trim_matches('\'');
            let value = value.trim().trim_matches('"').trim_matches('\'');
            props.push((key.to_string(), value.to_string()));
        }
    }

    props
}

fn chrono_lite_timestamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}", duration.as_secs())
}

fn convert_statement(stmt: sqlparser::ast::Statement) -> Result<Statement, ParseError> {
    match stmt {
        sqlparser::ast::Statement::Query(query) => {
            // Handle UPDATE/INSERT with top-level CTE (e.g., WITH cte AS (...) UPDATE/INSERT)
            // sqlparser parses this as Statement::Query with body SetExpr::Update or SetExpr::Insert
            match *query.body {
                sqlparser::ast::SetExpr::Update(sqlparser::ast::Statement::Update {
                    table,
                    assignments,
                    from: _,
                    selection,
                    returning: _,
                    or: _,
                }) => {
                    let table_name = table.to_string();
                    let set_clauses: Vec<SetClause> = assignments
                        .iter()
                        .map(|s| {
                            let column = match &s.target {
                                sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                                    name.to_string()
                                }
                                sqlparser::ast::AssignmentTarget::Tuple(_) => String::new(),
                            };
                            let value = convert_expr(s.value.clone())?;
                            Ok(SetClause { column, value })
                        })
                        .collect::<Result<Vec<_>, ParseError>>()?;
                    let selection = selection.map(|e| convert_expr(e)).transpose()?;
                    Ok(Statement::Update(UpdateStmt {
                        table: table_name,
                        set_clauses,
                        selection,
                    }))
                }
                sqlparser::ast::SetExpr::Insert(inner_stmt) => {
                    // For INSERT with CTE, recursively convert the inner statement
                    // The CTE is in the source query's WITH clause
                    convert_statement(inner_stmt)
                }
                _ => {
                    let query_stmt = convert_query(*query)?;
                    Ok(Statement::Query(query_stmt))
                }
            }
        }
        sqlparser::ast::Statement::Insert(stmt) => {
            let table_name = stmt.table_name.to_string();
            let cols: Vec<String> = stmt.columns.iter().map(|c| c.value.clone()).collect();
            // Handle VALUES via source query
            let query_opt: Option<QueryStmt> = stmt.source.as_ref().and_then(|q| {
                if let sqlparser::ast::SetExpr::Values(_) = &*q.body {
                    None
                } else {
                    convert_query(*q.clone()).ok()
                }
            });
            let values_list: Vec<Vec<Expr>> = stmt
                .source
                .as_ref()
                .and_then(|q| {
                    if let sqlparser::ast::SetExpr::Values(values) = &*q.body {
                        Some(
                            values
                                .rows
                                .iter()
                                .map(|row| {
                                    row.iter()
                                        .map(|e| convert_expr(e.clone()))
                                        .collect::<Result<Vec<_>, _>>()
                                })
                                .collect::<Result<Vec<_>, _>>(),
                        )
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| Ok(vec![]))?;
            Ok(Statement::Insert(InsertStmt {
                table: table_name,
                columns: cols,
                values: values_list,
                query: query_opt,
                is_overwrite: stmt.overwrite,
                on_duplicate_key_update: stmt.on.as_ref().map_or_else(
                    || Ok(Vec::new()),
                    |on_insert| {
                        match on_insert {
                            sqlparser::ast::OnInsert::DuplicateKeyUpdate(assignments) => {
                                assignments
                                    .iter()
                                    .map(|assign| {
                                        let value = convert_expr(assign.value.clone())?;
                                        Ok(OnDuplicateKeyUpdate {
                                            column: assign.target.to_string(),
                                            value,
                                        })
                                    })
                                    .collect::<Result<Vec<_>, ParseError>>()
                            }
                            _ => Ok(Vec::new()), // Other ON INSERT variants not yet supported
                        }
                    },
                )?,
            }))
        }
        sqlparser::ast::Statement::CreateTable(stmt) => {
            let name_str = stmt.name.to_string();
            let (database, table_name) = split_qualified_name(&name_str);
            let col_defs: Vec<ColumnDef> = stmt
                .columns
                .iter()
                .map(|c| ColumnDef {
                    name: c.name.value.clone(),
                    data_type: c.data_type.to_string(),
                    nullable: true,
                    default_value: None,
                    agg_type: None,
                    comment: None,
                })
                .collect();
            // Extract unique keys from constraints
            let mut unique_keys: Vec<UniqueKeyDef> = vec![];
            let mut keys_type = KeysType::Duplicate;
            for constraint in &stmt.constraints {
                match constraint {
                    sqlparser::ast::TableConstraint::Unique { name, columns, .. } => {
                        let col_names: Vec<String> =
                            columns.iter().map(|c| c.value.clone()).collect();
                        unique_keys.push(UniqueKeyDef {
                            name: name.as_ref().map(|n| n.value.clone()),
                            columns: col_names,
                        });
                        keys_type = KeysType::Unique;
                    }
                    sqlparser::ast::TableConstraint::PrimaryKey { .. } => {
                        keys_type = KeysType::Primary;
                    }
                    _ => {}
                }
            }
            Ok(Statement::CreateTable(CreateTableStmt {
                database,
                name: table_name,
                if_not_exists: stmt.if_not_exists,
                columns: col_defs,
                keys_type,
                unique_keys,
                partition: None,
                distribution: None,
                properties: vec![],
            }))
        }
        sqlparser::ast::Statement::Drop {
            object_type,
            names,
            if_exists,
            ..
        } => {
            let name = names.first().map(|n| n.to_string()).unwrap_or_default();
            match object_type {
                sqlparser::ast::ObjectType::Database => {
                    Ok(Statement::DropDatabase(DropDatabaseStmt {
                        name,
                        if_exists,
                    }))
                }
                _ => {
                    let (database, table_name) = split_qualified_name(&name);
                    Ok(Statement::DropTable(DropTableStmt {
                        database,
                        name: table_name,
                        if_exists,
                    }))
                }
            }
        }
        sqlparser::ast::Statement::ShowDatabases { .. } => Ok(Statement::ShowDatabases),
        sqlparser::ast::Statement::CreateDatabase {
            db_name,
            if_not_exists,
            ..
        } => Ok(Statement::CreateDatabase(CreateDatabaseStmt {
            name: db_name.to_string(),
            if_not_exists,
            properties: vec![],
        })),
        sqlparser::ast::Statement::ShowTables {
            show_options, full, ..
        } => {
            let db_name = show_options
                .show_in
                .and_then(|si| si.parent_name)
                .map(|n| n.to_string());
            let like_pattern = show_options.filter_position.and_then(|fp| match fp {
                sqlparser::ast::ShowStatementFilterPosition::Suffix(filter)
                | sqlparser::ast::ShowStatementFilterPosition::Infix(filter) => {
                    if let sqlparser::ast::ShowStatementFilter::Like(pattern) = filter {
                        Some(pattern)
                    } else {
                        None
                    }
                }
            });
            Ok(Statement::ShowTables(db_name, like_pattern, full))
        }
        sqlparser::ast::Statement::Use(use_expr) => {
            let db_name = match use_expr {
                sqlparser::ast::Use::Database(name) => name.to_string(),
                sqlparser::ast::Use::Object(name) => name.to_string(),
                sqlparser::ast::Use::Schema(name) => name.to_string(),
                other => other.to_string(),
            };
            Ok(Statement::UseDatabase(db_name))
        }
        sqlparser::ast::Statement::ExplainTable { table_name, .. } => {
            let name_str = table_name.to_string();
            let (db, tbl) = split_qualified_name(&name_str);
            let db = db.unwrap_or_default();
            Ok(Statement::Describe(db, tbl))
        }
        sqlparser::ast::Statement::ShowCreate { obj_name, .. } => {
            let name_str = obj_name.to_string();
            let (db, tbl) = split_qualified_name(&name_str);
            let db = db.unwrap_or_default();
            Ok(Statement::ShowCreateTable(db, tbl))
        }
        sqlparser::ast::Statement::SetVariable {
            variables, value, ..
        } => {
            let var_name = match variables {
                sqlparser::ast::OneOrManyWithParens::One(o) => o.to_string(),
                sqlparser::ast::OneOrManyWithParens::Many(v) => v
                    .first()
                    .map(|s: &sqlparser::ast::ObjectName| s.to_string())
                    .unwrap_or_default(),
            };
            let value_expr = value
                .first()
                .map(|e: &sqlparser::ast::Expr| convert_expr(e.clone()))
                .transpose()?
                .unwrap_or(Expr::Literal(LiteralValue::Null));
            Ok(Statement::SetVariable(SetVariableStmt {
                variable: var_name,
                value: value_expr,
                is_global: false,
            }))
        }
        sqlparser::ast::Statement::Explain {
            statement, verbose, ..
        } => {
            let inner = convert_statement(*statement)?;
            Ok(Statement::Explain(ExplainStmt {
                statement: Box::new(inner),
                verbose,
            }))
        }
        sqlparser::ast::Statement::Truncate { table_names, .. } => {
            let first_table = table_names.first();
            if let Some(table) = first_table {
                let name_str = table.name.to_string();
                let (database, table) = split_qualified_name(&name_str);
                Ok(Statement::TruncateTable {
                    database,
                    table,
                    if_exists: false,
                })
            } else {
                Err(ParseError::SyntaxError {
                    position: 0,
                    message: "TRUNCATE requires at least one table".to_string(),
                })
            }
        }
        sqlparser::ast::Statement::CreateView {
            or_replace: _,
            materialized: _,
            name,
            columns,
            query,
            if_not_exists,
            ..
        } => {
            let name_str = name.to_string();
            let (database, view_name) = split_qualified_name(&name_str);
            let col_names: Vec<String> = columns
                .iter()
                .map(|c: &sqlparser::ast::ViewColumnDef| c.name.value.clone())
                .collect();
            Ok(Statement::CreateView {
                database,
                name: view_name,
                if_not_exists,
                query: query.to_string(),
                columns: col_names,
            })
        }
        sqlparser::ast::Statement::Update {
            table,
            assignments,
            from: _,
            selection,
            returning: _,
            or: _,
        } => {
            let table_name = table.to_string();
            let set_clauses: Vec<SetClause> = assignments
                .iter()
                .map(|s| -> Result<SetClause, ParseError> {
                    let column = match &s.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                        sqlparser::ast::AssignmentTarget::Tuple(_) => String::new(),
                    };
                    let value = convert_expr(s.value.clone())?;
                    Ok(SetClause { column, value })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let selection = selection.map(|e| convert_expr(e)).transpose()?;
            Ok(Statement::Update(UpdateStmt {
                table: table_name,
                set_clauses,
                selection,
            }))
        }
        sqlparser::ast::Statement::Delete(delete) => {
            // Multi-table DELETE support:
            // Form 1: DELETE t1, t2 FROM table1 t1 INNER JOIN table2 t2 ON ...
            //   - delete.tables = [t1, t2], delete.from = tables with joins
            // Form 2: DELETE FROM t1 USING table1 t1 INNER JOIN table2 t2 ON ...
            //   - delete.tables = [t1], delete.using = tables with joins
            let tables: Vec<String> = delete.tables.iter().map(|t| t.to_string()).collect();

            let from = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(from) if !from.is_empty() => {
                    Some(convert_table_ref(from[0].clone()))
                }
                sqlparser::ast::FromTable::WithoutKeyword(from) if !from.is_empty() => {
                    Some(convert_table_ref(from[0].clone()))
                }
                _ => None,
            };

            let using = match &delete.using {
                Some(using_vec) if !using_vec.is_empty() => {
                    Some(convert_table_ref(using_vec[0].clone()))
                }
                _ => None,
            };

            let selection = delete.selection.map(|e| convert_expr(e)).transpose()?;

            // Convert order_by from sqlparser OrderByExpr to our OrderByItem
            let order_by: Vec<OrderByItem> = delete
                .order_by
                .into_iter()
                .map(|o| -> Result<OrderByItem, ParseError> {
                    Ok(OrderByItem {
                        expr: convert_expr(o.expr)?,
                        ascending: o.asc.unwrap_or(true),
                        nulls_first: o.nulls_first.unwrap_or(true),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            // Convert limit - sqlparser uses Expr, we need Option<usize>
            let limit = delete.limit.and_then(|l| match l {
                sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => n.parse().ok(),
                _ => None,
            });

            Ok(Statement::Delete(DeleteStmt {
                tables,
                from,
                using,
                selection,
                order_by,
                limit,
            }))
        }
        sqlparser::ast::Statement::AlterTable {
            name,
            if_exists: _,
            only: _,
            operations,
            ..
        } => {
            let name_str = name.to_string();
            let (database, table_name) = split_qualified_name(&name_str);
            let alter_ops: Vec<AlterOperation> = operations
                .into_iter()
                .filter_map(|op| match op {
                    sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
                        Some(AlterOperation::AddColumn(ColumnDef {
                            name: column_def.name.value.clone(),
                            data_type: column_def.data_type.to_string(),
                            nullable: true,
                            default_value: None,
                            agg_type: None,
                            comment: None,
                        }))
                    }
                    sqlparser::ast::AlterTableOperation::DropColumn { column_name, .. } => {
                        Some(AlterOperation::DropColumn(column_name.value.clone()))
                    }
                    sqlparser::ast::AlterTableOperation::RenameTable { table_name } => {
                        Some(AlterOperation::RenameTable(table_name.to_string()))
                    }
                    sqlparser::ast::AlterTableOperation::ModifyColumn {
                        col_name,
                        data_type,
                        ..
                    } => Some(AlterOperation::ModifyColumn(ColumnDef {
                        name: col_name.value.clone(),
                        data_type: data_type.to_string(),
                        nullable: true,
                        default_value: None,
                        agg_type: None,
                        comment: None,
                    })),
                    _ => None,
                })
                .collect();
            Ok(Statement::AlterTable(AlterTableStmt {
                database,
                table: table_name,
                operations: alter_ops,
            }))
        }
        _ => Err(ParseError::Unsupported(format!(
            "statement type: {:?}",
            stmt
        ))),
    }
}

fn convert_query(query: sqlparser::ast::Query) -> Result<QueryStmt, ParseError> {
    if let Some(ref w) = query.with {
        if w.cte_tables.len() > 1 {
            tracing::warn!(
                "Multiple CTEs ({} defined) not fully supported; only first CTE used.",
                w.cte_tables.len()
            );
        }
    }
    let cte = query.with.as_ref().and_then(|w| {
        w.cte_tables.first().map(|c| Cte {
            name: c.alias.name.value.clone(),
            columns: vec![],
            query: Box::new(
                convert_query(*c.query.clone()).unwrap_or_else(|_| QueryStmt {
                    select_list: vec![],
                    from: None,
                    r#where: None,
                    group_by: vec![],
                    having: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    with: None,
                    set_op: None,
                }),
            ),
        })
    });

    match *query.body {
        sqlparser::ast::SetExpr::Select(select) => {
            let order_by = query.order_by.map(|ob| ob.exprs).unwrap_or_default();
            let limit = query.limit;
            let offset = query.offset;
            convert_select(*select, order_by, limit, offset, cte)
        }
        sqlparser::ast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let left_query = convert_set_expr(*left)?;
            let right_query = convert_set_expr(*right)?;
            let union_op = match op {
                sqlparser::ast::SetOperator::Union => UnionOperator::Union,
                sqlparser::ast::SetOperator::Except => UnionOperator::Except,
                sqlparser::ast::SetOperator::Intersect => UnionOperator::Intersect,
            };
            let all = matches!(set_quantifier, sqlparser::ast::SetQuantifier::All);
            let order_by = query.order_by.map(|ob| ob.exprs).unwrap_or_default();
            Ok(QueryStmt {
                select_list: left_query.select_list,
                from: left_query.from,
                r#where: left_query.r#where,
                group_by: left_query.group_by,
                having: left_query.having,
                order_by: order_by
                    .into_iter()
                    .map(|o| -> Result<OrderByItem, ParseError> {
                        Ok(OrderByItem {
                            expr: convert_expr(o.expr)?,
                            ascending: o.asc.unwrap_or(true),
                            nulls_first: o.nulls_first.unwrap_or(true),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                limit: query.limit.and_then(|l| match l {
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                        n.parse().ok()
                    }
                    _ => None,
                }),
                offset: query.offset.and_then(|o| match o.value {
                    sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => {
                        n.parse().ok()
                    }
                    _ => None,
                }),
                with: cte,
                set_op: Some(SetOperation {
                    op: union_op,
                    all,
                    right: Box::new(right_query),
                }),
            })
        }
        _ => Err(ParseError::Unsupported("non-SELECT query body".to_string())),
    }
}

fn convert_set_expr(expr: sqlparser::ast::SetExpr) -> Result<QueryStmt, ParseError> {
    match expr {
        sqlparser::ast::SetExpr::Select(select) => {
            convert_select(*select, vec![], None, None, None)
        }
        sqlparser::ast::SetExpr::Query(query) => convert_query(*query),
        sqlparser::ast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let left_query = convert_set_expr(*left)?;
            let right_query = convert_set_expr(*right)?;
            let union_op = match op {
                sqlparser::ast::SetOperator::Union => UnionOperator::Union,
                sqlparser::ast::SetOperator::Except => UnionOperator::Except,
                sqlparser::ast::SetOperator::Intersect => UnionOperator::Intersect,
            };
            let all = matches!(set_quantifier, sqlparser::ast::SetQuantifier::All);
            let order_by: Vec<OrderByItem> = vec![];
            Ok(QueryStmt {
                select_list: left_query.select_list,
                from: left_query.from,
                r#where: left_query.r#where,
                group_by: left_query.group_by,
                having: left_query.having,
                order_by,
                limit: None,
                offset: None,
                with: None,
                set_op: Some(SetOperation {
                    op: union_op,
                    all,
                    right: Box::new(right_query),
                }),
            })
        }
        _ => Err(ParseError::Unsupported(
            "set operation not supported".to_string(),
        )),
    }
}

fn convert_select(
    select: sqlparser::ast::Select,
    order_by: Vec<sqlparser::ast::OrderByExpr>,
    limit: Option<sqlparser::ast::Expr>,
    offset: Option<sqlparser::ast::Offset>,
    cte: Option<Cte>,
) -> Result<QueryStmt, ParseError> {
    let select_list = select
        .projection
        .into_iter()
        .map(|item| -> Result<SelectItem, ParseError> {
            Ok(match item {
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => SelectItem {
                    expr: convert_expr(expr)?,
                    alias: None,
                },
                sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => SelectItem {
                    expr: convert_expr(expr)?,
                    alias: Some(alias.value),
                },
                sqlparser::ast::SelectItem::Wildcard(_) => SelectItem {
                    expr: Expr::Wildcard,
                    alias: None,
                },
                _ => SelectItem {
                    expr: Expr::Wildcard,
                    alias: None,
                },
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let from = {
        if select.from.len() > 1 {
            tracing::warn!(
                "Multiple FROM tables ({} tables) not fully supported; only first table used. Use explicit JOIN.",
                select.from.len()
            );
        }
        select.from.into_iter().next().map(convert_table_ref)
    };

    let group_by = match select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs, _) => {
            exprs.into_iter().map(convert_expr).collect::<Result<_, _>>()?
        }
        _ => vec![],
    };

    let order_by: Vec<OrderByItem> = order_by
        .into_iter()
        .map(|o| -> Result<OrderByItem, ParseError> {
            Ok(OrderByItem {
                expr: convert_expr(o.expr)?,
                ascending: o.asc.unwrap_or(true),
                nulls_first: o.nulls_first.unwrap_or(true),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let limit = limit.and_then(|l| match l {
        sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => n.parse().ok(),
        _ => None,
    });

    let offset = offset.and_then(|o| match o.value {
        sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => n.parse().ok(),
        _ => None,
    });

    Ok(QueryStmt {
        select_list,
        from,
        r#where: select.selection.map(|e| convert_expr(e)).transpose()?,
        group_by,
        having: select.having.map(|e| convert_expr(e)).transpose()?,
        order_by,
        limit,
        offset,
        with: cte,
        set_op: None,
    })
}

fn extract_join_condition(op: &sqlparser::ast::JoinOperator) -> Option<sqlparser::ast::Expr> {
    use sqlparser::ast::JoinOperator;
    match op {
        JoinOperator::Inner(constraint) => extract_constraint_expr(constraint),
        JoinOperator::LeftOuter(constraint) => extract_constraint_expr(constraint),
        JoinOperator::RightOuter(constraint) => extract_constraint_expr(constraint),
        JoinOperator::FullOuter(constraint) => extract_constraint_expr(constraint),
        _ => None,
    }
}

fn extract_constraint_expr(
    constraint: &sqlparser::ast::JoinConstraint,
) -> Option<sqlparser::ast::Expr> {
    match constraint {
        sqlparser::ast::JoinConstraint::On(expr) => Some(expr.clone()),
        _ => None,
    }
}

fn convert_table_ref(t: sqlparser::ast::TableWithJoins) -> TableRef {
    let name = match &t.relation {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => {
            let table_name = name.to_string();
            TableRef::Table {
                name: table_name,
                alias: alias.as_ref().map(|a| a.name.value.clone()),
            }
        }
        sqlparser::ast::TableFactor::Derived {
            subquery, alias, ..
        } => match convert_query(*subquery.clone()) {
            Ok(query) => {
                return TableRef::Subquery {
                    query: Box::new(query),
                    alias: alias
                        .as_ref()
                        .map(|a| a.name.value.clone())
                        .unwrap_or_default(),
                };
            }
            Err(_) => {
                return TableRef::Table {
                    name: "unknown".into(),
                    alias: None,
                };
            }
        },
        _ => TableRef::Table {
            name: "unknown".into(),
            alias: None,
        },
    };

    t.joins.into_iter().fold(name, |left, join| {
        let right = convert_table_ref_simple(join.relation);
        let condition = extract_join_condition(&join.join_operator);
        TableRef::Join {
            left: Box::new(left),
            right: Box::new(right),
            r#type: match join.join_operator {
                sqlparser::ast::JoinOperator::Inner(_) => JoinType::Inner,
                sqlparser::ast::JoinOperator::LeftOuter(_) => JoinType::LeftOuter,
                sqlparser::ast::JoinOperator::RightOuter(_) => JoinType::RightOuter,
                sqlparser::ast::JoinOperator::FullOuter(_) => JoinType::FullOuter,
                sqlparser::ast::JoinOperator::CrossJoin => JoinType::Cross,
                _ => JoinType::Inner,
            },
            condition: condition.map(|e| convert_expr(e).ok()).flatten(),
        }
    })
}

fn convert_table_ref_simple(factor: sqlparser::ast::TableFactor) -> TableRef {
    match factor {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => TableRef::Table {
            name: name.to_string(),
            alias: alias.map(|a| a.name.value),
        },
        _ => TableRef::Table {
            name: "unknown".into(),
            alias: None,
        },
    }
}

fn convert_function_args(args: sqlparser::ast::FunctionArguments) -> Result<Vec<Expr>, ParseError> {
    match args {
        sqlparser::ast::FunctionArguments::None => Ok(vec![]),
        sqlparser::ast::FunctionArguments::Subquery(_) => Ok(vec![]),
        sqlparser::ast::FunctionArguments::List(list) => list
            .args
            .into_iter()
            .map(|arg| match arg {
                sqlparser::ast::FunctionArg::Unnamed(arg_expr)
                | sqlparser::ast::FunctionArg::Named { arg: arg_expr, .. } => match arg_expr {
                    sqlparser::ast::FunctionArgExpr::Expr(expr) => convert_expr(expr),
                    sqlparser::ast::FunctionArgExpr::QualifiedWildcard(name) => Ok(Expr::ColumnRef {
                        table: None,
                        column: name.to_string(),
                    }),
                    sqlparser::ast::FunctionArgExpr::Wildcard => Ok(Expr::ColumnRef {
                        table: None,
                        column: "*".to_string(),
                    }),
                },
                other => Err(ParseError::SyntaxError {
                    position: 0,
                    message: format!("Unsupported function argument: {:?}", other),
                }),
            })
            .collect(),
    }
}

/// Process MySQL escape sequences in a string literal.
/// Converts sequences like \n, \t, \\, \', \", etc. to their actual characters.
fn process_escape_sequences(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(&next) = chars.peek() {
                match next {
                    'n' => {
                        result.push('\n');
                        chars.next();
                    }
                    't' => {
                        result.push('\t');
                        chars.next();
                    }
                    'r' => {
                        result.push('\r');
                        chars.next();
                    }
                    '\\' => {
                        result.push('\\');
                        chars.next();
                    }
                    '\'' => {
                        result.push('\'');
                        chars.next();
                    }
                    '"' => {
                        result.push('"');
                        chars.next();
                    }
                    '0' => {
                        result.push('\0');
                        chars.next();
                    }
                    // Handle \x followed by hex digits
                    'x' | 'X' => {
                        chars.next(); // consume 'x'
                        let hex: String = chars.by_ref().take(2).collect();
                        if hex.len() == 2 {
                            if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                                result.push(byte as char);
                            } else {
                                // Invalid hex, keep as-is
                                result.push('\\');
                                result.push('x');
                                result.push_str(&hex);
                            }
                        } else {
                            // Not enough hex digits, keep as-is
                            result.push('\\');
                            result.push('x');
                            result.push_str(&hex);
                        }
                    }
                    // Handle \b (backspace)
                    'b' => {
                        result.push('\x08');
                        chars.next();
                    }
                    // Handle \f (form feed)
                    'f' => {
                        result.push('\x0C');
                        chars.next();
                    }
                    // Handle \v (vertical tab)
                    'v' => {
                        result.push('\x0B');
                        chars.next();
                    }
                    // Unknown escape sequence - keep backslash
                    _ => {
                        result.push('\\');
                    }
                }
            } else {
                // Trailing backslash
                result.push('\\');
            }
        } else {
            result.push(c);
        }
    }

    result
}

fn convert_expr(expr: sqlparser::ast::Expr) -> Result<Expr, ParseError> {
    match expr {
        sqlparser::ast::Expr::Value(v) => Ok(Expr::Literal(match v {
            sqlparser::ast::Value::Number(n, _) => {
                if n.contains('.') || n.contains('e') || n.contains('E') {
                    LiteralValue::Float64(n.parse().unwrap_or(0.0))
                } else {
                    LiteralValue::Int64(n.parse().unwrap_or_else(|_| {
                        // Try to parse as float and truncate, or default to 0
                        n.parse::<f64>().map(|f| f as i64).unwrap_or(0)
                    }))
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => {
                LiteralValue::String(process_escape_sequences(&s))
            }
            sqlparser::ast::Value::DoubleQuotedString(s) => {
                LiteralValue::String(process_escape_sequences(&s))
            }
            sqlparser::ast::Value::Boolean(b) => LiteralValue::Boolean(b),
            sqlparser::ast::Value::Null => LiteralValue::Null,
            _ => LiteralValue::Null,
        })),
        sqlparser::ast::Expr::Identifier(id) => Ok(Expr::ColumnRef {
            table: None,
            column: id.value,
        }),
        sqlparser::ast::Expr::CompoundIdentifier(ids) => {
            let len = ids.len();
            Ok(if len == 2 {
                Expr::ColumnRef {
                    table: Some(ids[0].value.clone()),
                    column: ids[1].value.clone(),
                }
            } else {
                Expr::ColumnRef {
                    table: None,
                    column: ids.last().map(|i| i.value.clone()).unwrap_or_default(),
                }
            })
        }
        sqlparser::ast::Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(convert_expr(*left)?),
            op: convert_binary_op(op)?,
            right: Box::new(convert_expr(*right)?),
        }),
        sqlparser::ast::Expr::UnaryOp { op, expr } => {
            let converted_op = match op {
                sqlparser::ast::UnaryOperator::Not => UnaryOp::Not,
                sqlparser::ast::UnaryOperator::Minus => UnaryOp::Negate,
                sqlparser::ast::UnaryOperator::Plus => {
                    // Unary plus is a no-op, just convert the inner expression
                    return convert_expr(*expr);
                }
                other => {
                    return Err(ParseError::SyntaxError {
                        position: 0,
                        message: format!("Unsupported unary operator: {:?}", other),
                    });
                }
            };
            Ok(Expr::UnaryOp {
                op: converted_op,
                expr: Box::new(convert_expr(*expr)?),
            })
        },
        sqlparser::ast::Expr::Function(fun) => {
            let name = fun.name.to_string();
            let args = convert_function_args(fun.args.clone())?;
            let distinct = matches!(&fun.args, sqlparser::ast::FunctionArguments::List(list) if matches!(list.duplicate_treatment, Some(sqlparser::ast::DuplicateTreatment::Distinct)));
            Ok(Expr::FunctionCall {
                name,
                args,
                distinct,
            })
        }
        sqlparser::ast::Expr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: Box::new(convert_expr(*expr)?),
            list: list
                .into_iter()
                .map(|e| convert_expr(e))
                .collect::<Result<Vec<_>, _>>()?,
            negated,
        }),
        sqlparser::ast::Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(Expr::InSubquery {
            expr: Box::new(convert_expr(*expr)?),
            query: Box::new(convert_query(*subquery).unwrap_or_else(|_| QueryStmt {
                select_list: vec![],
                from: None,
                r#where: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
                with: None,
                set_op: None,
            })),
            negated,
        }),
        sqlparser::ast::Expr::Exists { subquery, negated } => {
            let query = convert_query(*subquery).unwrap_or_else(|_| QueryStmt {
                select_list: vec![],
                from: None,
                r#where: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
                with: None,
                set_op: None,
            });
            if negated {
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(Expr::Exists(Box::new(query))),
                })
            } else {
                Ok(Expr::Exists(Box::new(query)))
            }
        }
        sqlparser::ast::Expr::Subquery(subquery) => Ok(Expr::Subquery(Box::new(
            convert_query(*subquery).unwrap_or_else(|_| QueryStmt {
                select_list: vec![],
                from: None,
                r#where: None,
                group_by: vec![],
                having: None,
                order_by: vec![],
                limit: None,
                offset: None,
                with: None,
                set_op: None,
            }),
        ))),
        sqlparser::ast::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(convert_expr(*expr)?),
            low: Box::new(convert_expr(*low)?),
            high: Box::new(convert_expr(*high)?),
            negated,
        }),
        sqlparser::ast::Expr::IsNull(expr) => Ok(Expr::IsNull {
            expr: Box::new(convert_expr(*expr)?),
            negated: false,
        }),
        sqlparser::ast::Expr::IsNotNull(expr) => Ok(Expr::IsNull {
            expr: Box::new(convert_expr(*expr)?),
            negated: true,
        }),
        sqlparser::ast::Expr::Nested(expr) => convert_expr(*expr),
        sqlparser::ast::Expr::Like {
            expr,
            pattern,
            negated,
            ..
        } => Ok(Expr::Like {
            expr: Box::new(convert_expr(*expr)?),
            pattern: Box::new(convert_expr(*pattern)?),
            negated,
        }),
        sqlparser::ast::Expr::Case {
            operand: _,
            conditions,
            results,
            else_result,
        } => {
            let cases: Vec<WhenThen> = conditions
                .into_iter()
                .zip(results.into_iter())
                .map(|(when_cond, then_val)| {
                    Ok(WhenThen {
                        when: convert_expr(when_cond)?,
                        then: convert_expr(then_val)?,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?;
            Ok(Expr::CaseWhen {
                cases,
                else_expr: else_result
                    .map(|e| Ok::<_, ParseError>(Box::new(convert_expr(*e)?)))
                    .transpose()?,
            })
        }
        sqlparser::ast::Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut args = vec![convert_expr(*expr)?];
            if let Some(from_expr) = substring_from {
                args.push(convert_expr(*from_expr)?);
            }
            if let Some(for_expr) = substring_for {
                args.push(convert_expr(*for_expr)?);
            }
            Ok(Expr::FunctionCall {
                name: "substring".to_string(),
                args,
                distinct: false,
            })
        }
        // Convert special expression forms to function calls
        sqlparser::ast::Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            let func_name = match trim_where {
                Some(sqlparser::ast::TrimWhereField::Leading) => "LTRIM",
                Some(sqlparser::ast::TrimWhereField::Trailing) => "RTRIM",
                Some(sqlparser::ast::TrimWhereField::Both) | None => "TRIM",
            };
            let mut args = vec![convert_expr(*expr)?];
            if let Some(what) = trim_what {
                args.push(convert_expr(*what)?);
            }
            if let Some(chars) = trim_characters {
                for c in chars {
                    args.push(convert_expr(c)?);
                }
            }
            Ok(Expr::FunctionCall {
                name: func_name.to_string(),
                args,
                distinct: false,
            })
        }
        sqlparser::ast::Expr::Interval(interval) => {
            let unit = interval
                .leading_field
                .map(|f| format!("{}", f))
                .unwrap_or_else(|| "DAY".to_string());
            Ok(Expr::FunctionCall {
                name: "INTERVAL".to_string(),
                args: vec![
                    convert_expr(*interval.value)?,
                    Expr::Literal(LiteralValue::String(unit)),
                ],
                distinct: false,
            })
        }
        sqlparser::ast::Expr::Ceil { expr, .. } => Ok(Expr::FunctionCall {
            name: "CEIL".to_string(),
            args: vec![convert_expr(*expr)?],
            distinct: false,
        }),
        sqlparser::ast::Expr::Floor { expr, .. } => Ok(Expr::FunctionCall {
            name: "FLOOR".to_string(),
            args: vec![convert_expr(*expr)?],
            distinct: false,
        }),
        sqlparser::ast::Expr::Position { expr, r#in } => Ok(Expr::FunctionCall {
            name: "POSITION".to_string(),
            args: vec![convert_expr(*expr)?, convert_expr(*r#in)?],
            distinct: false,
        }),
        _ => Err(ParseError::SyntaxError {
            position: 0,
            message: format!("Unsupported expression type: {:?}", expr),
        }),
    }
}

fn convert_binary_op(op: sqlparser::ast::BinaryOperator) -> Result<BinaryOp, ParseError> {
    use sqlparser::ast::BinaryOperator;
    match op {
        BinaryOperator::Eq => Ok(BinaryOp::Eq),
        BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
        BinaryOperator::Lt => Ok(BinaryOp::Lt),
        BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
        BinaryOperator::Gt => Ok(BinaryOp::Gt),
        BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
        BinaryOperator::And => Ok(BinaryOp::And),
        BinaryOperator::Or => Ok(BinaryOp::Or),
        BinaryOperator::Plus => Ok(BinaryOp::Plus),
        BinaryOperator::Minus => Ok(BinaryOp::Minus),
        BinaryOperator::Multiply => Ok(BinaryOp::Multiply),
        BinaryOperator::Divide => Ok(BinaryOp::Divide),
        BinaryOperator::Modulo => Ok(BinaryOp::Modulo),
        BinaryOperator::BitwiseAnd => Ok(BinaryOp::BitwiseAnd),
        BinaryOperator::BitwiseOr => Ok(BinaryOp::BitwiseOr),
        BinaryOperator::BitwiseXor => Ok(BinaryOp::BitwiseXor),
        _ => Err(ParseError::SyntaxError {
            position: 0,
            message: format!("Unsupported binary operator: {:?}", op),
        }),
    }
}

fn parse_create_user(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();

    let _if_not_exists = sql.to_uppercase().contains("IF NOT EXISTS");

    let after_create = strip_prefix_ci(sql, "CREATE USER")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CREATE USER".to_string(),
        })?
        .trim();

    let (username, rest) = extract_username_hostname(after_create)?;

    let mut auth_plugin = "mysql_native_password".to_string();
    let mut password = None;
    let mut identified_by_password = false;

    // Use case-insensitive search on original string to preserve password casing
    if let Some(id_pos) = find_ascii_ci(&rest, "IDENTIFIED BY") {
        identified_by_password = true;
        // Extract password from the ORIGINAL string (not uppercased)
        let after_id_by = &rest[id_pos + "IDENTIFIED BY".len()..];
        let pwd = after_id_by.trim().trim_matches(|c| c == '\'' || c == '"');
        // Stop at next keyword boundary (WITH, etc.)
        let pwd = if let Some(with_off) = find_ascii_ci(pwd, " WITH ") {
            pwd[..with_off].trim()
        } else {
            pwd
        };
        if !pwd.is_empty() {
            password = Some(pwd.to_string());
        }
        // Extract auth plugin from original string too
        if let Some(with_pos) = find_ascii_ci(&rest, "WITH") {
            if with_pos > id_pos {
                let after_with = &rest[with_pos + "WITH".len()..];
                let plugin = after_with
                    .trim()
                    .split_whitespace()
                    .next()
                    .unwrap_or("mysql_native_password");
                auth_plugin = plugin.to_lowercase();
            }
        }
    } else if let Some(id_with_pos) = find_ascii_ci(&rest, "IDENTIFIED WITH") {
        let after_with = &rest[id_with_pos + "IDENTIFIED WITH".len()..];
        let plugin = after_with
            .trim()
            .split_whitespace()
            .next()
            .unwrap_or("mysql_native_password");
        auth_plugin = plugin.to_lowercase();
    }

    Ok(vec![Statement::CreateUser(CreateUserStmt {
        username,
        hostname: None,
        auth_plugin,
        password,
        identified_by_password,
        roles: vec![],
    })])
}

fn parse_drop_user(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();

    let if_exists = sql.to_uppercase().contains("IF EXISTS");

    let after_drop = strip_prefix_ci(sql, "DROP USER")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP USER".to_string(),
        })?
        .trim();

    let (username, _hostname) = extract_username_hostname(after_drop)?;

    Ok(vec![Statement::DropUser(DropUserStmt {
        username,
        hostname: None,
        if_exists,
    })])
}

fn extract_username_hostname(s: &str) -> Result<(String, String), ParseError> {
    let s = s.trim();

    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.is_empty() {
        return Err(ParseError::SyntaxError {
            position: 0,
            message: "Expected username".to_string(),
        });
    }

    let username_hostname = parts[0];
    let remaining = if parts.len() > 1 {
        parts[1].to_string()
    } else {
        String::new()
    };

    if username_hostname.contains('@') {
        let uparts: Vec<&str> = username_hostname.split('@').collect();
        let username = uparts[0].to_string();
        let hostname = if uparts.len() > 1 {
            Some(uparts[1].to_string())
        } else {
            None
        };
        let hostname_str = hostname.unwrap_or_else(|| "%".to_string());
        Ok((username, hostname_str))
    } else {
        Ok((username_hostname.to_string(), remaining))
    }
}

fn parse_create_catalog(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_create = strip_prefix_ci(sql, "CREATE CATALOG")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CREATE CATALOG".to_string(),
        })?
        .trim();

    let (name, rest) = extract_identifier(after_create).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected catalog name".to_string(),
    })?;

    let rest = rest.trim();
    let mut catalog_type = "iceberg".to_string();
    let mut properties = vec![];

    if strip_prefix_ci(rest, "PROPERTIES").is_some() || strip_prefix_ci(rest, "WITH").is_some() {
        let after_with = strip_prefix_ci(rest, "PROPERTIES")
            .or_else(|| strip_prefix_ci(rest, "WITH"))
            .unwrap_or_default()
            .trim();

        if strip_prefix_ci(after_with, "TYPE").is_some() {
            let type_part = strip_prefix_ci(after_with, "TYPE")
                .unwrap_or("")
                .trim()
                .trim_start_matches('=')
                .trim();
            let first_word = type_part
                .split_whitespace()
                .next()
                .unwrap_or(type_part)
                .trim_matches(|c| c == ',' || c == ';');
            catalog_type = first_word.to_lowercase();
        }

        if rest.contains('(') && rest.contains('=') {
            properties = parse_properties(rest);
        }
    }

    Ok(vec![Statement::CreateCatalog(CreateCatalogStmt {
        name: name.to_string(),
        catalog_type,
        properties,
    })])
}

fn parse_drop_catalog(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let if_exists = sql.to_uppercase().contains("IF EXISTS");

    let after_drop = strip_prefix_ci(sql, "DROP CATALOG")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP CATALOG".to_string(),
        })?
        .trim();

    let (name, _) = extract_identifier(after_drop).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected catalog name".to_string(),
    })?;

    Ok(vec![Statement::DropCatalog(DropCatalogStmt {
        name: name.to_string(),
        if_exists,
    })])
}

fn parse_refresh_catalog(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();
    let after_refresh = upper
        .strip_prefix("REFRESH CATALOG")
        .map(|_| sql["REFRESH CATALOG".len()..].trim())
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected REFRESH CATALOG".to_string(),
        })?;

    let (name, _) = extract_identifier(after_refresh).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected catalog name".to_string(),
    })?;

    Ok(vec![Statement::RefreshCatalog(RefreshCatalogStmt {
        name: name.to_string(),
    })])
}

// ---- Batch 2: DDL parsers ----

fn parse_create_index(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_create = sql
        .strip_prefix("CREATE INDEX")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CREATE INDEX".to_string(),
        })?
        .trim();
    let (index_name, rest) =
        extract_identifier(after_create).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected index name".to_string(),
        })?;
    let rest = rest
        .trim()
        .strip_prefix("ON")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected ON".to_string(),
        })?
        .trim();
    let (table_name, rest) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected table name".to_string(),
    })?;
    let name_str = table_name.to_string();
    let (database, table) = split_qualified_name(&name_str);
    let rest = rest.trim();
    let mut columns = vec![];
    let mut index_type = None;
    let mut properties = vec![];
    if rest.starts_with('(') {
        if let Some(end_paren) = rest.find(')') {
            columns = rest[1..end_paren]
                .split(',')
                .map(|c| c.trim().to_string())
                .collect();
            let after_paren = rest[end_paren + 1..].trim();
            if after_paren.to_uppercase().starts_with("USING") {
                let after_using = after_paren
                    .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
                    .trim();
                if let Some((t, _)) = extract_identifier(after_using) {
                    index_type = Some(t.to_string());
                }
            }
            if let Some(prop_start) = find_ascii_ci(&after_paren, "PROPERTIES") {
                properties = parse_properties(&after_paren[prop_start..]);
            }
        }
    }
    Ok(vec![Statement::CreateIndex(CreateIndexStmt {
        index_name: index_name.to_string(),
        database,
        table,
        columns,
        index_type,
        properties,
    })])
}

fn parse_drop_index(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let sql = sql.trim();
    let after_drop = sql
        .strip_prefix("DROP INDEX")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP INDEX".to_string(),
        })?
        .trim();
    let if_exists = after_drop.to_uppercase().starts_with("IF EXISTS");
    let rest = if if_exists {
        after_drop
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    } else {
        after_drop
    };
    let (index_name, rest) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected index name".to_string(),
    })?;
    let rest = rest
        .trim()
        .strip_prefix("ON")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected ON".to_string(),
        })?
        .trim();
    let (table_name, _) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected table name".to_string(),
    })?;
    let name_str = table_name.to_string();
    let (database, table) = split_qualified_name(&name_str);
    Ok(vec![Statement::DropIndex(DropIndexStmt {
        index_name: index_name.to_string(),
        database,
        table,
        if_exists,
    })])
}

fn parse_cancel_alter_table(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("CANCEL ALTER TABLE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CANCEL ALTER TABLE".to_string(),
        })?
        .trim();
    let (database, rest) = if let Some(from_pos) = find_ascii_ci(&after, "FROM") {
        let after_from = after[from_pos + 4..].trim();
        let (db, remaining) =
            extract_identifier(after_from).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected database name".to_string(),
            })?;
        (Some(db.to_string()), remaining.trim())
    } else {
        (None, after)
    };
    // When database is specified in FROM clause, the next identifier is the table name
    // (not db.table - that would only come from the table name itself containing a dot)
    let (db_from_clause, table_name) = if database.is_some() {
        // Database already extracted from FROM clause - remaining is just the table
        // Skip leading dot if present (FROM sql_test.t1 means db=sql_test, table=t1)
        let rest = rest.trim_start_matches('.');
        let (name, _) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected table name".to_string(),
        })?;
        (database, name.to_string())
    } else {
        // No FROM clause - might be db.table or just table
        let (name, remaining) =
            extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected table name".to_string(),
            })?;
        let name_str = name.to_string();
        let remaining = remaining.trim();
        if remaining.starts_with('.') {
            // Fully qualified db.table
            let after_dot = remaining[1..].trim();
            let (table_part, _) =
                extract_identifier(after_dot).ok_or_else(|| ParseError::SyntaxError {
                    position: 0,
                    message: "Expected table name".to_string(),
                })?;
            (Some(name_str), table_part.to_string())
        } else {
            (None, name_str)
        }
    };
    Ok(vec![Statement::CancelAlterTable(CancelAlterTableStmt {
        database: db_from_clause,
        table: table_name,
    })])
}

fn parse_alter_colocate_group(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("ALTER COLOCATE GROUP")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected ALTER COLOCATE GROUP".to_string(),
        })?
        .trim();
    let (group_name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected group name".to_string(),
    })?;
    let rest = rest.trim();
    let rest_upper = rest.to_uppercase();
    let operation = if rest_upper.starts_with("ADD TABLE") {
        let after_add = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let (table_name, _) =
            extract_identifier(after_add).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected table name".to_string(),
            })?;
        let name_str = table_name.to_string();
        let (database, table) = split_qualified_name(&name_str);
        ColocateGroupOperation::AddTable { database, table }
    } else if rest_upper.starts_with("REMOVE TABLE") {
        let after_rm = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let (table_name, _) =
            extract_identifier(after_rm).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected table name".to_string(),
            })?;
        let name_str = table_name.to_string();
        let (database, table) = split_qualified_name(&name_str);
        ColocateGroupOperation::RemoveTable { database, table }
    } else if rest_upper.starts_with("SET PROPERTIES") {
        let after_set = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        ColocateGroupOperation::SetProperty(parse_properties(&format!("PROPERTIES {}", after_set)))
    } else {
        return Err(ParseError::SyntaxError {
            position: 0,
            message: format!("Unknown ALTER COLOCATE GROUP operation: {}", rest),
        });
    };
    Ok(vec![Statement::AlterColocateGroup(
        AlterColocateGroupStmt {
            group_name: group_name.to_string(),
            operation,
        },
    )])
}

fn parse_alter_database(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("ALTER DATABASE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected ALTER DATABASE".to_string(),
        })?
        .trim();
    let (name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected database name".to_string(),
    })?;
    let rest = rest.trim();
    let properties = if rest.to_uppercase().starts_with("SET PROPERTIES") {
        let after_set = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        parse_properties(&format!("PROPERTIES {}", after_set))
    } else {
        vec![]
    };
    Ok(vec![Statement::AlterDatabase(AlterDatabaseStmt {
        name: name.to_string(),
        properties,
    })])
}

fn parse_drop_view(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("DROP VIEW")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP VIEW".to_string(),
        })?
        .trim();
    let if_exists = after.to_uppercase().starts_with("IF EXISTS");
    let rest = if if_exists {
        after
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    } else {
        after
    };
    let (name, _) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected view name".to_string(),
    })?;
    let name_str = name.to_string();
    let (database, view_name) = split_qualified_name(&name_str);
    Ok(vec![Statement::DropView(DropViewStmt {
        database,
        name: view_name,
        if_exists,
    })])
}

fn parse_alter_view(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("ALTER VIEW")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected ALTER VIEW".to_string(),
        })?
        .trim();
    let (name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected view name".to_string(),
    })?;
    let name_str = name.to_string();
    let (database, view_name) = split_qualified_name(&name_str);
    let rest = rest.trim();
    let query = if rest.to_uppercase().starts_with("AS ") {
        rest[3..].trim().to_string()
    } else {
        rest.to_string()
    };
    Ok(vec![Statement::AlterView(AlterViewStmt {
        database,
        name: view_name,
        query,
    })])
}

fn parse_alter_table_doris(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("ALTER TABLE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected ALTER TABLE".to_string(),
        })?
        .trim();
    let (name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected table name".to_string(),
    })?;
    let name_str = name.to_string();
    let (database, table_name) = split_qualified_name(&name_str);
    let rest = rest.trim();
    let rest_upper = rest.to_uppercase();

    let operation = if rest_upper.starts_with("RENAME COLUMN") {
        let after_rename = rest
            .trim_start_matches(|c: char| !c.is_whitespace())
            .trim()
            .trim_start_matches("COLUMN")
            .trim();
        let (old_name, remaining) =
            extract_identifier(after_rename).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected old column name".to_string(),
            })?;
        let remaining = remaining
            .trim()
            .strip_prefix("TO")
            .or_else(|| remaining.trim().strip_prefix("to"))
            .ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected TO".to_string(),
            })?
            .trim();
        let (new_name, _) =
            extract_identifier(remaining).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected new column name".to_string(),
            })?;
        AlterOperation::RenameColumn {
            old_name: old_name.to_string(),
            new_name: new_name.to_string(),
        }
    } else if rest_upper.starts_with("ADD PARTITION") {
        let after_add = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let (part_name, remaining) =
            extract_identifier(after_add).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected partition name".to_string(),
            })?;
        let remaining = remaining.trim();
        let mut values_less_than = vec![];
        let mut properties = vec![];
        if remaining.to_uppercase().starts_with("VALUES LESS THAN") {
            let after_vals = remaining
                .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
                .trim();
            if after_vals.starts_with('(') {
                if let Some(end) = after_vals.find(')') {
                    values_less_than = after_vals[1..end]
                        .split(',')
                        .map(|v| v.trim().trim_matches('\'').trim_matches('"').to_string())
                        .collect();
                    let after_paren = after_vals[end + 1..].trim();
                    if after_paren.to_uppercase().starts_with("PROPERTIES") {
                        properties = parse_properties(after_paren);
                    }
                }
            }
        }
        AlterOperation::AddPartition {
            partition_name: part_name.to_string(),
            values_less_than,
            properties,
        }
    } else if rest_upper.starts_with("DROP PARTITION") {
        let after_drop = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let if_exists = after_drop.to_uppercase().starts_with("IF EXISTS");
        let name_part = if if_exists {
            after_drop
                .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
                .trim()
        } else {
            after_drop
        };
        let (part_name, remaining) =
            extract_identifier(name_part).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected partition name".to_string(),
            })?;
        let force = remaining.trim().to_uppercase() == "FORCE";
        AlterOperation::DropPartition {
            partition_name: part_name.to_string(),
            if_exists,
            force,
        }
    } else if rest_upper.starts_with("ADD ROLLUP") {
        let after_add = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let (rollup_name, remaining) =
            extract_identifier(after_add).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected rollup name".to_string(),
            })?;
        let remaining = remaining.trim();
        let mut columns = vec![];
        let mut properties = vec![];
        if remaining.starts_with('(') {
            if let Some(end) = remaining.find(')') {
                columns = remaining[1..end]
                    .split(',')
                    .map(|c| c.trim().to_string())
                    .collect();
                let after_paren = remaining[end + 1..].trim();
                if after_paren.to_uppercase().starts_with("PROPERTIES") {
                    properties = parse_properties(after_paren);
                }
            }
        }
        AlterOperation::AddRollup {
            rollup_name: rollup_name.to_string(),
            columns,
            properties,
        }
    } else if rest_upper.starts_with("DROP ROLLUP") {
        let after_drop = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let if_exists = after_drop.to_uppercase().starts_with("IF EXISTS");
        let name_part = if if_exists {
            after_drop
                .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
                .trim()
        } else {
            after_drop
        };
        let (rollup_name, _) =
            extract_identifier(name_part).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected rollup name".to_string(),
            })?;
        AlterOperation::DropRollup {
            rollup_name: rollup_name.to_string(),
            if_exists,
        }
    } else if rest_upper.starts_with("REPLACE WITH") {
        let after_replace = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let after_table = if after_replace.to_uppercase().starts_with("TABLE") {
            after_replace
                .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
                .trim()
        } else {
            after_replace
        };
        let (old_table, remaining) =
            extract_identifier(after_table).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected table name".to_string(),
            })?;
        let swap = remaining.trim().to_uppercase().starts_with("SWAP");
        let properties = if let Some(s) = find_ascii_ci(&remaining, "PROPERTIES") {
            parse_properties(&remaining[s..])
        } else {
            vec![]
        };
        AlterOperation::Replace {
            old_table: old_table.to_string(),
            swap,
            properties,
        }
    } else if rest_upper.starts_with("ADD GENERATED COLUMN") {
        let after_add = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let (col_name, remaining) =
            extract_identifier(after_add).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected column name".to_string(),
            })?;
        let remaining = remaining.trim();
        let (data_type, remaining) = extract_identifier(remaining)
            .map(|(dt, r)| (dt.to_string(), r))
            .unwrap_or_else(|| ("STRING".to_string(), ""));
        let comment = if remaining.trim().to_uppercase().starts_with("COMMENT") {
            remaining
                .trim()
                .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string()
        } else {
            String::new()
        };
        AlterOperation::AddGeneratedColumn(ColumnDef {
            name: col_name.to_string(),
            data_type,
            nullable: true,
            default_value: None,
            agg_type: None,
            comment: if comment.is_empty() {
                None
            } else {
                Some(comment)
            },
        })
    } else if rest_upper.starts_with("COMMENT") {
        let comment_part = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        AlterOperation::SetComment(
            comment_part
                .trim_matches('\'')
                .trim_matches('"')
                .to_string(),
        )
    } else if rest_upper.starts_with("SET PROPERTIES") {
        let after_set = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        AlterOperation::SetProperty(parse_properties(&format!("PROPERTIES {}", after_set)))
    } else {
        return Err(ParseError::SyntaxError {
            position: 0,
            message: format!("Unknown ALTER TABLE operation: {}", rest),
        });
    };
    Ok(vec![Statement::AlterTable(AlterTableStmt {
        database,
        table: table_name,
        operations: vec![operation],
    })])
}

// ---- Batch 3/4 parsers ----

fn parse_export_table(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("EXPORT TABLE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected EXPORT TABLE".to_string(),
        })?
        .trim();
    // Extract qualified table name (db.table or just table)
    let to_pos = find_ascii_ci(&after, " TO ").ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected TO".to_string(),
    })?;
    let table_part = after[..to_pos].trim();
    let rest = after[to_pos + 4..].trim();
    let parts: Vec<&str> = table_part.split('.').collect();
    let (database, table) = if parts.len() == 2 {
        (Some(parts[0].to_string()), parts[1].to_string())
    } else {
        (None, table_part.to_string())
    };
    let path = rest
        .trim_start_matches(|c: char| c != ' ')
        .trim_start()
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();
    let mut properties = vec![];
    if let Some(idx) = find_ascii_ci(&rest, "PROPERTIES") {
        properties = parse_properties(&rest[idx..]);
    }
    Ok(vec![Statement::ExportTable(ExportTableStmt {
        database,
        table,
        path,
        properties,
    })])
}

fn parse_cancel_export(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("CANCEL EXPORT")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CANCEL EXPORT".to_string(),
        })?
        .trim();
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected export ID".to_string(),
    })?;
    Ok(vec![Statement::CancelExport(name.to_string())])
}

fn parse_create_function(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("CREATE FUNCTION")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CREATE FUNCTION".to_string(),
        })?
        .trim();
    let if_not_exists = after.to_uppercase().starts_with("IF NOT EXISTS");
    let rest = if if_not_exists {
        after
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    } else {
        after
    };
    let (name, rest) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected function name".to_string(),
    })?;
    let rest = rest.trim();
    let mut args = vec![];
    let mut returns = None;
    let mut properties = vec![];
    if rest.starts_with('(') {
        if let Some(end) = rest.find(')') {
            args = rest[1..end]
                .split(',')
                .map(|a| a.trim().to_string())
                .collect();
            let after_paren = rest[end + 1..].trim();
            let after_upper = after_paren.to_uppercase();
            if after_upper.starts_with("RETURNS") {
                let after_returns = after_paren
                    .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
                    .trim();
                if let Some((t, _)) = extract_identifier(after_returns) {
                    returns = Some(t.to_string());
                }
            }
            if let Some(idx) = find_ascii_ci(&after_paren, "PROPERTIES") {
                properties = parse_properties(&after_paren[idx..]);
            }
        }
    }
    Ok(vec![Statement::CreateFunction(CreateFunctionStmt {
        name: name.to_string(),
        args,
        returns,
        properties,
        if_not_exists,
    })])
}

fn parse_drop_function(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("DROP FUNCTION")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP FUNCTION".to_string(),
        })?
        .trim();
    let if_exists = after.to_uppercase().starts_with("IF EXISTS");
    let rest = if if_exists {
        after
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    } else {
        after
    };
    let (name, rest) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected function name".to_string(),
    })?;
    let mut args = vec![];
    let rest = rest.trim();
    if rest.starts_with('(') {
        if let Some(end) = rest.find(')') {
            args = rest[1..end]
                .split(',')
                .map(|a| a.trim().to_string())
                .collect();
        }
    }
    Ok(vec![Statement::DropFunction(DropFunctionStmt {
        name: name.to_string(),
        args,
        if_exists,
    })])
}

fn parse_show_create_function(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("SHOW CREATE FUNCTION")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW CREATE FUNCTION".to_string(),
        })?
        .trim();
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected function name".to_string(),
    })?;
    Ok(vec![Statement::ShowCreateFunction(name.to_string())])
}

fn parse_desc_function(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql.trim();
    let after = if after.to_uppercase().starts_with("DESCRIBE FUNCTION") {
        after
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    } else {
        after
            .strip_prefix("DESC FUNCTION")
            .unwrap_or_default()
            .trim()
    };
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected function name".to_string(),
    })?;
    Ok(vec![Statement::DescribeFunction(name.to_string())])
}

fn parse_analyze_table(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("ANALYZE TABLE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected ANALYZE TABLE".to_string(),
        })?
        .trim();
    let (table_name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected table name".to_string(),
    })?;
    let name_str = table_name.to_string();
    let (database, table) = split_qualified_name(&name_str);
    let rest = rest.trim();
    let mut columns = vec![];
    let mut sample_rate = None;
    if let Some(idx) = find_ascii_ci(&rest, "UPDATE COLUMNS") {
        let after_cols = rest[idx + 14..].trim();
        if after_cols.starts_with('(') {
            if let Some(end) = after_cols.find(')') {
                columns = after_cols[1..end]
                    .split(',')
                    .map(|c| c.trim().to_string())
                    .collect();
            }
        }
    }
    if let Some(idx) = find_ascii_ci(&rest, "SAMPLE RATE") {
        let rate_str = rest[idx + 11..]
            .trim()
            .split_whitespace()
            .next()
            .unwrap_or("0.1");
        sample_rate = rate_str.parse().ok();
    }
    Ok(vec![Statement::AnalyzeTable(AnalyzeTableStmt {
        database,
        table,
        columns,
        sample_rate,
    })])
}

fn parse_drop_stats(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("DROP STATS")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP STATS".to_string(),
        })?
        .trim();
    let (table_name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected table name".to_string(),
    })?;
    let name_str = table_name.to_string();
    let (database, table) = split_qualified_name(&name_str);
    let mut columns = vec![];
    if rest.trim().to_uppercase().starts_with("COLUMNS") {
        let after_cols = rest
            .trim()
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        if after_cols.starts_with('(') {
            if let Some(end) = after_cols.find(')') {
                columns = after_cols[1..end]
                    .split(',')
                    .map(|c| c.trim().to_string())
                    .collect();
            }
        }
    }
    Ok(vec![Statement::DropStats(DropStatsStmt {
        database,
        table,
        columns,
    })])
}

fn parse_kill_analyze_job(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("KILL ANALYZE JOB")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected KILL ANALYZE JOB".to_string(),
        })?
        .trim();
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected job ID".to_string(),
    })?;
    Ok(vec![Statement::KillAnalyzeJob(name.to_string())])
}

fn parse_kill_query(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("KILL QUERY")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected KILL QUERY".to_string(),
        })?
        .trim();
    let id_str = after.split_whitespace().next().unwrap_or("");
    let id: u64 = id_str.parse().map_err(|_| ParseError::SyntaxError {
        position: 0,
        message: format!("Invalid connection ID: {}", id_str),
    })?;
    Ok(vec![Statement::KillQuery(id)])
}

fn parse_kill_connection(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("KILL")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected KILL".to_string(),
        })?
        .trim();
    // Skip optional "CONNECTION" keyword
    let after = if after.to_uppercase().starts_with("CONNECTION") {
        after["CONNECTION".len()..].trim()
    } else {
        after
    };
    let id_str = after.split_whitespace().next().unwrap_or("");
    let id: u64 = id_str.parse().map_err(|_| ParseError::SyntaxError {
        position: 0,
        message: format!("Invalid connection ID: {}", id_str),
    })?;
    Ok(vec![Statement::KillConnection(id)])
}

fn parse_admin_check_table(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("ADMIN CHECK TABLE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected ADMIN CHECK TABLE".to_string(),
        })?
        .trim();
    let (table_name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected table name".to_string(),
    })?;
    Ok(vec![Statement::AdminCheckTable(table_name.to_string())])
}

fn parse_alter_stats(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("ALTER STATS")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected ALTER STATS".to_string(),
        })?
        .trim();
    let (table_name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected table name".to_string(),
    })?;
    let rest = rest.trim();
    let props = if rest.to_uppercase().starts_with("PROPERTIES") {
        parse_properties(rest)
    } else {
        vec![]
    };
    Ok(vec![Statement::AlterStats(table_name.to_string(), props)])
}

fn parse_show_analyze(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("SHOW ANALYZE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW ANALYZE".to_string(),
        })?
        .trim();
    if after.is_empty() || after == ";" {
        return Ok(vec![Statement::ShowAnalyze(None)]);
    }
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected job ID".to_string(),
    })?;
    let job_id = if name.to_uppercase() == "JOB" {
        let remaining = after
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let (id, _) = extract_identifier(remaining).unwrap_or((&after, ""));
        Some(id.to_string())
    } else {
        Some(name.to_string())
    };
    Ok(vec![Statement::ShowAnalyze(job_id)])
}

fn parse_show_stats(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("SHOW STATS")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW STATS".to_string(),
        })?
        .trim();
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected table name".to_string(),
    })?;
    Ok(vec![Statement::ShowStats(name.to_string())])
}

fn parse_show_table_stats(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql.trim();
    let after = if after.to_uppercase().starts_with("SHOW TABLE STATISTICS") {
        after
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    } else {
        after
            .strip_prefix("SHOW TABLE STATS")
            .unwrap_or_default()
            .trim()
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    };
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected table name".to_string(),
    })?;
    Ok(vec![Statement::ShowTableStats(name.to_string())])
}

fn parse_create_job(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("CREATE JOB")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CREATE JOB".to_string(),
        })?
        .trim();
    let (name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected job name".to_string(),
    })?;
    let rest = rest.trim();
    let schedule = if rest.to_uppercase().starts_with("ON SCHEDULE") {
        let after_schedule = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let cron_start = after_schedule.find("CRON").map(|i| i + 4).unwrap_or(0);
        if cron_start > 0 {
            let after_cron = after_schedule[cron_start..].trim();
            let inner = if after_cron.starts_with('(') {
                if let Some(end) = after_cron.find(')') {
                    after_cron[1..end].to_string()
                } else {
                    after_cron.to_string()
                }
            } else {
                after_cron
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .to_string()
            };
            inner.trim_matches('\'').trim_matches('"').to_string()
        } else {
            after_schedule.to_string()
        }
    } else {
        String::new()
    };
    let execute = if let Some(idx) = find_ascii_ci(&rest, "EXECUTE") {
        let after_exec = rest[idx + 7..].trim();
        after_exec.trim_matches('\'').trim_matches('"').to_string()
    } else {
        String::new()
    };
    Ok(vec![Statement::CreateJob(CreateJobStmt {
        name: name.to_string(),
        schedule,
        execute,
    })])
}

fn parse_drop_job(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("DROP JOB")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP JOB".to_string(),
        })?
        .trim();
    let if_exists = after.to_uppercase().starts_with("IF EXISTS");
    let rest = if if_exists {
        after
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    } else {
        after
    };
    let (name, _) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected job name".to_string(),
    })?;
    let _ = if_exists;
    Ok(vec![Statement::DropJob(name.to_string())])
}

fn parse_pause_job(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("PAUSE JOB")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected PAUSE JOB".to_string(),
        })?
        .trim();
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected job name".to_string(),
    })?;
    Ok(vec![Statement::PauseJob(name.to_string())])
}

fn parse_resume_job(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("RESUME JOB")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected RESUME JOB".to_string(),
        })?
        .trim();
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected job name".to_string(),
    })?;
    Ok(vec![Statement::ResumeJob(name.to_string())])
}

fn parse_cancel_task(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("CANCEL TASK")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CANCEL TASK".to_string(),
        })?
        .trim();
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected task ID".to_string(),
    })?;
    Ok(vec![Statement::CancelTask(name.to_string())])
}

fn parse_install_plugin(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("INSTALL PLUGIN")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected INSTALL PLUGIN".to_string(),
        })?
        .trim();
    let (name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected plugin name".to_string(),
    })?;
    let rest = rest
        .trim()
        .strip_prefix("FROM")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected FROM".to_string(),
        })?
        .trim();
    let source = rest.trim_matches('\'').trim_matches('"').to_string();
    Ok(vec![Statement::InstallPlugin(InstallPluginStmt {
        name: name.to_string(),
        source,
    })])
}

fn parse_uninstall_plugin(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("UNINSTALL PLUGIN")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected UNINSTALL PLUGIN".to_string(),
        })?
        .trim();
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected plugin name".to_string(),
    })?;
    Ok(vec![Statement::UninstallPlugin(name.to_string())])
}

fn parse_recover_database(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("RECOVER DATABASE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected RECOVER DATABASE".to_string(),
        })?
        .trim();
    let (name, _) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected database name".to_string(),
    })?;
    Ok(vec![Statement::RecoverDatabase(name.to_string())])
}

fn parse_recover_table(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("RECOVER TABLE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected RECOVER TABLE".to_string(),
        })?
        .trim();
    let (db_or_table, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected name".to_string(),
    })?;
    let rest = rest.trim();
    let (database, table) = if let Some((t2, _)) = extract_identifier(rest) {
        (db_or_table.to_string(), t2.to_string())
    } else {
        (String::new(), db_or_table.to_string())
    };
    Ok(vec![Statement::RecoverTable { database, table }])
}

fn parse_recover_partition(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("RECOVER PARTITION")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected RECOVER PARTITION".to_string(),
        })?
        .trim();
    let (n1, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected name".to_string(),
    })?;
    let rest = rest.trim();
    let (n2, rest) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected name".to_string(),
    })?;
    let rest = rest.trim();
    let (n3, _) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected name".to_string(),
    })?;
    Ok(vec![Statement::RecoverPartition {
        database: n1.to_string(),
        table: n2.to_string(),
        partition: n3.to_string(),
    }])
}

fn parse_drop_catalog_recycle_bin(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("DROP CATALOG RECYCLE BIN")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP CATALOG RECYCLE BIN".to_string(),
        })?
        .trim();
    let filter = if after.to_uppercase().starts_with("WHERE") {
        Some(
            after
                .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
                .trim()
                .to_string(),
        )
    } else {
        None
    };
    Ok(vec![Statement::DropCatalogRecycleBin(filter)])
}

fn parse_create_sql_block_rule(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("CREATE SQL_BLOCK_RULE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CREATE SQL_BLOCK_RULE".to_string(),
        })?
        .trim();
    let (name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected rule name".to_string(),
    })?;
    let props = if rest.trim().to_uppercase().starts_with("PROPERTIES") {
        parse_properties(rest.trim())
    } else {
        vec![]
    };
    Ok(vec![Statement::CreateSqlBlockRule(
        CreateSqlBlockRuleStmt {
            name: name.to_string(),
            properties: props,
        },
    )])
}

fn parse_alter_sql_block_rule(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("ALTER SQL_BLOCK_RULE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected ALTER SQL_BLOCK_RULE".to_string(),
        })?
        .trim();
    let (name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected rule name".to_string(),
    })?;
    let props = if rest.trim().to_uppercase().starts_with("PROPERTIES") {
        parse_properties(rest.trim())
    } else {
        vec![]
    };
    Ok(vec![Statement::AlterSqlBlockRule(name.to_string(), props)])
}

fn parse_drop_sql_block_rule(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("DROP SQL_BLOCK_RULE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP SQL_BLOCK_RULE".to_string(),
        })?
        .trim();
    let if_exists = after.to_uppercase().starts_with("IF EXISTS");
    let rest = if if_exists {
        after
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    } else {
        after
    };
    let (name, _) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected rule name".to_string(),
    })?;
    let _ = if_exists;
    Ok(vec![Statement::DropSqlBlockRule(name.to_string())])
}

fn parse_show_sql_block_rule(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("SHOW SQL_BLOCK_RULE")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW SQL_BLOCK_RULE".to_string(),
        })?
        .trim();
    let filter = if after.is_empty() || after == ";" {
        None
    } else if after.to_uppercase().starts_with("FOR") {
        let after_for = after
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let (name, _) = extract_identifier(after_for).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected rule name".to_string(),
        })?;
        Some(name.to_string())
    } else {
        Some(after.to_string())
    };
    Ok(vec![Statement::ShowSqlBlockRule(filter)])
}

fn parse_create_row_policy(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("CREATE ROW POLICY")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected CREATE ROW POLICY".to_string(),
        })?
        .trim();
    let (name, rest) = extract_identifier(after).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected policy name".to_string(),
    })?;
    let rest = rest.trim();
    let rest_upper = rest.to_uppercase();
    let after_on = if rest_upper.starts_with("ON") {
        rest.trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    } else {
        rest
    };
    let (table_name, rest) =
        extract_identifier(after_on).ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected table name".to_string(),
        })?;
    let name_str = table_name.to_string();
    let (database, table) = split_qualified_name(&name_str);
    let rest = rest.trim();
    let rest_upper = rest.to_uppercase();
    let after_as = if rest_upper.starts_with("AS") {
        rest.trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    } else {
        rest
    };
    let (policy_type, rest) = extract_identifier(after_as).unwrap_or(("PERMIT", ""));
    let rest = if rest.is_empty() { "" } else { rest.trim() };
    let using_expr = if rest.to_uppercase().starts_with("USING") {
        rest.trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
            .trim_matches('\'')
            .trim_matches('"')
            .to_string()
    } else {
        String::new()
    };
    Ok(vec![Statement::CreateRowPolicy(CreateRowPolicyStmt {
        name: name.to_string(),
        database,
        table,
        policy_type: policy_type.to_string(),
        using_expr,
    })])
}

fn parse_drop_row_policy(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("DROP ROW POLICY")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected DROP ROW POLICY".to_string(),
        })?
        .trim();
    let if_exists = after.to_uppercase().starts_with("IF EXISTS");
    let rest = if if_exists {
        after
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim()
    } else {
        after
    };
    let (name, rest) = extract_identifier(rest).ok_or_else(|| ParseError::SyntaxError {
        position: 0,
        message: "Expected policy name".to_string(),
    })?;
    let rest = rest.trim();
    let (database, table) = if rest.to_uppercase().starts_with("ON") {
        let after_on = rest
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        let (table_name, _) =
            extract_identifier(after_on).ok_or_else(|| ParseError::SyntaxError {
                position: 0,
                message: "Expected table name".to_string(),
            })?;
        let name_str = table_name.to_string();
        let (db_part, tbl_part) = split_qualified_name(&name_str);
        (db_part, tbl_part)
    } else {
        (None, String::new())
    };
    let _ = if_exists;
    Ok(vec![Statement::DropRowPolicy {
        name: name.to_string(),
        database,
        table,
    }])
}

fn parse_show_row_policy(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("SHOW ROW POLICY")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW ROW POLICY".to_string(),
        })?
        .trim();
    let filter = if after.is_empty() || after == ";" {
        None
    } else {
        Some(after.to_string())
    };
    Ok(vec![Statement::ShowRowPolicy(filter)])
}

fn parse_show_functions(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let after = sql
        .trim()
        .strip_prefix("SHOW FUNCTIONS")
        .ok_or_else(|| ParseError::SyntaxError {
            position: 0,
            message: "Expected SHOW FUNCTIONS".to_string(),
        })?
        .trim();
    let pattern = if after.is_empty() || after == ";" {
        None
    } else if after.to_uppercase().starts_with("LIKE") {
        let after_like = after
            .trim_start_matches(|c: char| c.is_ascii_alphabetic() || c == ' ')
            .trim();
        Some(after_like.trim_matches('\'').trim_matches('"').to_string())
    } else {
        Some(after.to_string())
    };
    Ok(vec![Statement::ShowFunctions(pattern)])
}
