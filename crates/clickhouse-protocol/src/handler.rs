//! ClickHouse HTTP command handler

use crate::storage::ClickHouseStorage;
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for handling ClickHouse commands
pub trait ClickHouseCommandHandler: Send + Sync {
    fn handle_query(&self, database: &str, query: &str) -> String;
}

/// Default ClickHouse command handler
pub struct DefaultClickHouseHandler {
    storage: Arc<ClickHouseStorage>,
}

/// Parse a possibly database-qualified table name like "ch_test.users" or "users".
/// Returns (database_option, table_name).
fn parse_qualified_name(name: &str) -> (Option<String>, String) {
    let name = name.trim();
    if let Some(dot_pos) = name.find('.') {
        let db = &name[..dot_pos];
        let table = &name[dot_pos + 1..];
        (Some(db.to_string()), table.to_string())
    } else {
        (None, name.to_string())
    }
}

/// Resolve the effective database name: prefer the one from the qualified name, fall back to default.
fn resolve_database<'a>(qualified_db: &'a Option<String>, default_db: &'a str) -> String {
    match qualified_db {
        Some(db) => db.clone(),
        None => default_db.to_string(),
    }
}

/// Strip surrounding quotes from a value string (single or double quotes).
fn unquote(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Tokenize a SQL string, respecting single-quoted string literals.
fn tokenize_sql(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Skip whitespace
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }

        // Handle string literals
        if chars[i] == '\'' {
            let mut token = String::new();
            token.push('\'');
            i += 1;
            while i < len {
                if chars[i] == '\'' {
                    // Check for escaped quote ''
                    if i + 1 < len && chars[i + 1] == '\'' {
                        token.push('\'');
                        token.push('\'');
                        i += 2;
                    } else {
                        token.push('\'');
                        i += 1;
                        break;
                    }
                } else {
                    token.push(chars[i]);
                    i += 1;
                }
            }
            tokens.push(token);
            continue;
        }

        // Handle operators: >=, <=, !=, <>, =, <, >
        if chars[i] == '(' || chars[i] == ')' || chars[i] == ',' {
            tokens.push(chars[i].to_string());
            i += 1;
            continue;
        }

        // Handle multi-char operators
        if i + 1 < len {
            let two: String = chars[i..i + 2].iter().collect();
            if two == ">=" || two == "<=" || two == "!=" || two == "<>" {
                tokens.push(two);
                i += 2;
                continue;
            }
        }

        if chars[i] == '=' || chars[i] == '<' || chars[i] == '>' {
            tokens.push(chars[i].to_string());
            i += 1;
            continue;
        }

        if chars[i] == '*' {
            tokens.push("*".to_string());
            i += 1;
            continue;
        }

        // Regular token (word, number, etc.)
        let mut token = String::new();
        while i < len && !chars[i].is_whitespace()
            && chars[i] != '(' && chars[i] != ')' && chars[i] != ','
            && chars[i] != '\''
        {
            // Check for operators
            if chars[i] == '=' || chars[i] == '<' || chars[i] == '>' {
                break;
            }
            if i + 1 < len {
                let two: String = chars[i..i + 2].iter().collect();
                if two == ">=" || two == "<=" || two == "!=" || two == "<>" {
                    break;
                }
            }
            token.push(chars[i]);
            i += 1;
        }
        if !token.is_empty() {
            tokens.push(token);
        }
    }

    tokens
}

/// Parse a parenthesized column list: "(col1, col2, col3)"
/// Returns the list of column names.
#[allow(dead_code)]
fn parse_column_list(s: &str) -> Vec<String> {
    let s = s.trim();
    let s = if s.starts_with('(') && s.ends_with(')') {
        &s[1..s.len() - 1]
    } else {
        s
    };
    s.split(',')
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .collect()
}

/// Evaluate a simple WHERE clause against a row.
/// Supports: col = val, col > val, col < val, col >= val, col <= val, col != val, col LIKE pattern
/// Supports AND / OR (simple left-to-right, no precedence).
fn evaluate_where(row: &HashMap<String, String>, where_clause: &str) -> bool {
    let where_clause = where_clause.trim();
    if where_clause.is_empty() {
        return true;
    }

    // Split by OR first (lower precedence)
    let or_parts = split_by_keyword(where_clause, "OR");
    if or_parts.len() > 1 {
        return or_parts.iter().any(|part| evaluate_where(row, part));
    }

    // Split by AND
    let and_parts = split_by_keyword(where_clause, "AND");
    if and_parts.len() > 1 {
        return and_parts.iter().all(|part| evaluate_where(row, part));
    }

    // Single condition
    evaluate_single_condition(row, where_clause)
}

/// Split a string by a keyword (case-insensitive), respecting quoted strings.
fn split_by_keyword(s: &str, keyword: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let chars: Vec<char> = s.chars().collect();
    let kw_upper = keyword.to_uppercase();
    let kw_len = kw_upper.len();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '\'' {
            in_quote = !in_quote;
            current.push(chars[i]);
            i += 1;
            continue;
        }

        if !in_quote && i + kw_len + 2 <= chars.len() {
            // Check for " KEYWORD " pattern
            let before_ok = i == 0 || chars[i - 1].is_whitespace();
            if before_ok {
                let candidate: String = chars[i..i + kw_len].iter().collect();
                if candidate.to_uppercase() == kw_upper {
                    let after_ok = i + kw_len >= chars.len() || chars[i + kw_len].is_whitespace();
                    if after_ok {
                        parts.push(current.trim().to_string());
                        current = String::new();
                        i += kw_len;
                        continue;
                    }
                }
            }
        }

        current.push(chars[i]);
        i += 1;
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        parts.push(trimmed);
    }

    if parts.is_empty() {
        parts.push(s.to_string());
    }

    parts
}

fn evaluate_single_condition(row: &HashMap<String, String>, condition: &str) -> bool {
    let condition = condition.trim();

    // Check for LIKE
    let upper = condition.to_uppercase();
    if let Some(like_pos) = upper.find(" LIKE ") {
        let col = condition[..like_pos].trim().to_string();
        let pattern = condition[like_pos + 6..].trim();
        let pattern = unquote(pattern);

        let val = match row.get(&col) {
            Some(v) => v,
            None => return false,
        };

        return match_like(val, &pattern);
    }

    // Parse comparison: col OP val
    let operators = [">=", "<=", "!=", "<>", "=", ">", "<"];
    for op in &operators {
        if let Some(pos) = condition.rfind(op) {
            // Make sure we're not matching part of a different operator
            let col = condition[..pos].trim().to_string();
            let val_str = condition[pos + op.len()..].trim();
            let val_str = unquote(val_str);

            let row_val = match row.get(&col) {
                Some(v) => v,
                None => return false,
            };

            // Try numeric comparison
            let row_num = row_val.parse::<f64>().ok();
            let val_num = val_str.parse::<f64>().ok();

            return match *op {
                "=" => row_val == &val_str,
                "!=" | "<>" => row_val != &val_str,
                ">" => {
                    if let (Some(a), Some(b)) = (row_num, val_num) {
                        a > b
                    } else {
                        row_val > &val_str
                    }
                }
                "<" => {
                    if let (Some(a), Some(b)) = (row_num, val_num) {
                        a < b
                    } else {
                        row_val < &val_str
                    }
                }
                ">=" => {
                    if let (Some(a), Some(b)) = (row_num, val_num) {
                        a >= b
                    } else {
                        row_val >= &val_str
                    }
                }
                "<=" => {
                    if let (Some(a), Some(b)) = (row_num, val_num) {
                        a <= b
                    } else {
                        row_val <= &val_str
                    }
                }
                _ => false,
            };
        }
    }

    false
}

/// Simple LIKE matching: % matches any sequence, _ matches any single char.
fn match_like(value: &str, pattern: &str) -> bool {
    let value_chars: Vec<char> = value.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    like_match_recursive(&value_chars, 0, &pattern_chars, 0)
}

fn like_match_recursive(value: &[char], vi: usize, pattern: &[char], pi: usize) -> bool {
    if pi == pattern.len() {
        return vi == value.len();
    }

    if pattern[pi] == '%' {
        // % can match zero or more characters
        for i in vi..=value.len() {
            if like_match_recursive(value, i, pattern, pi + 1) {
                return true;
            }
        }
        return false;
    }

    if vi >= value.len() {
        return false;
    }

    if pattern[pi] == '_' || pattern[pi] == value[vi] {
        return like_match_recursive(value, vi + 1, pattern, pi + 1);
    }

    false
}

/// Find the position of a top-level keyword in a token list (case-insensitive).
fn find_keyword(tokens: &[String], keyword: &str) -> Option<usize> {
    let kw = keyword.to_uppercase();
    tokens.iter().position(|t| t.to_uppercase() == kw)
}

/// Find the end of a clause (position of the next major keyword).
#[allow(dead_code)]
fn find_clause_end(tokens: &[String], start: usize, keywords: &[&str]) -> usize {
    let kws: Vec<String> = keywords.iter().map(|k| k.to_uppercase()).collect();
    for i in start..tokens.len() {
        if kws.contains(&tokens[i].to_uppercase()) {
            return i;
        }
    }
    tokens.len()
}

/// Parse value tuples from tokens starting after VALUES keyword.
/// Returns Vec of Vec of string values.
fn parse_value_tuples(tokens: &[String], start: usize) -> Vec<Vec<String>> {
    let mut tuples = Vec::new();
    let mut i = start;

    while i < tokens.len() {
        // Expect '('
        if tokens[i] == "(" {
            i += 1;
            let mut values = Vec::new();
            while i < tokens.len() && tokens[i] != ")" {
                if tokens[i] == "," {
                    i += 1;
                    continue;
                }
                values.push(unquote(&tokens[i]));
                i += 1;
            }
            if i < tokens.len() && tokens[i] == ")" {
                i += 1;
            }
            tuples.push(values);
        } else if tokens[i] == "," {
            i += 1;
        } else {
            i += 1;
        }
    }

    tuples
}

/// Parse column definitions from between parentheses in CREATE TABLE.
/// Input: "id UInt32, name String, age UInt32, email String"
/// Returns Vec of (name, type) pairs.
fn parse_column_defs(s: &str) -> Vec<(String, String)> {
    let s = s.trim();
    let s = if s.starts_with('(') && s.ends_with(')') {
        &s[1..s.len() - 1]
    } else {
        s
    };

    let mut defs = Vec::new();
    // Split by comma, but be careful with nested parens (e.g., Nullable(String))
    let mut depth = 0;
    let mut current = String::new();
    for ch in s.chars() {
        if ch == '(' {
            depth += 1;
            current.push(ch);
        } else if ch == ')' {
            depth -= 1;
            current.push(ch);
        } else if ch == ',' && depth == 0 {
            let trimmed = current.trim().to_string();
            if !trimmed.is_empty() {
                defs.push(trimmed);
            }
            current = String::new();
        } else {
            current.push(ch);
        }
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() {
        defs.push(trimmed);
    }

    defs.iter()
        .filter_map(|def| {
            let parts: Vec<&str> = def.split_whitespace().collect();
            if parts.len() >= 2 {
                Some((parts[0].to_string(), parts[1].to_string()))
            } else {
                None
            }
        })
        .collect()
}

impl DefaultClickHouseHandler {
    pub fn new(storage: Arc<ClickHouseStorage>) -> Self {
        Self { storage }
    }

    fn execute_query(&self, database: &str, query: &str) -> String {
        let query = query.trim().trim_end_matches(';');
        let upper = query.to_uppercase();

        if upper.starts_with("SELECT") {
            self.handle_select(database, query)
        } else if upper.starts_with("INSERT") {
            self.handle_insert(database, query)
        } else if upper.starts_with("CREATE") {
            self.handle_create(database, query)
        } else if upper.starts_with("DROP") {
            self.handle_drop(database, query)
        } else if upper.starts_with("SHOW") {
            self.handle_show(database, query)
        } else if upper.starts_with("DESCRIBE") || upper.starts_with("DESC") {
            self.handle_describe(database, query)
        } else if upper.starts_with("ALTER") {
            self.handle_alter(database, query)
        } else {
            "Error: Unsupported query".to_string()
        }
    }

    fn handle_select(&self, database: &str, query: &str) -> String {
        let upper = query.to_uppercase();

        // Simple system queries
        if upper.contains("SELECT 1") && !upper.contains("FROM") {
            return "1\n".to_string();
        }
        if upper.contains("VERSION()") && !upper.contains("FROM") {
            return "23.8.1.1\n".to_string();
        }

        // Tokenize
        let tokens = tokenize_sql(query);

        // Find FROM
        let from_idx = match find_keyword(&tokens, "FROM") {
            Some(idx) => idx,
            None => return "Error: Missing FROM clause".to_string(),
        };

        // Parse select columns (between SELECT and FROM)
        let select_tokens = &tokens[1..from_idx]; // skip SELECT

        // Get table name
        if from_idx + 1 >= tokens.len() {
            return "Error: Missing table name after FROM".to_string();
        }
        let raw_table = &tokens[from_idx + 1];
        let (qual_db, table_name) = parse_qualified_name(raw_table);
        let db_name = resolve_database(&qual_db, database);
        let db = match self.storage.get_database(&db_name) { Some(d) => d, None => return "Error: Database not found".to_string() };

        let table = match db.get_table(&table_name) {
            Some(t) => t,
            None => return format!("Error: Table {} not found", table_name),
        };

        let all_rows = table.select_all();
        let column_order = &table.column_order;

        // Find WHERE, GROUP BY, ORDER BY, LIMIT positions in tokens
        let where_idx = find_keyword(&tokens, "WHERE");
        let group_idx = find_keyword(&tokens, "GROUP");
        let order_idx = find_keyword(&tokens, "ORDER");
        let limit_idx = find_keyword(&tokens, "LIMIT");

        // Parse WHERE clause
        let filtered_rows: Vec<HashMap<String, String>> = if let Some(wi) = where_idx {
            let where_end = [group_idx, order_idx, limit_idx]
                .iter()
                .filter_map(|&x| x)
                .min()
                .unwrap_or(tokens.len());
            let where_tokens = &tokens[wi + 1..where_end];
            let where_str = where_tokens.join(" ");
            all_rows
                .into_iter()
                .filter(|row| evaluate_where(row, &where_str))
                .collect()
        } else {
            all_rows
        };

        // Check for GROUP BY
        if let Some(gi) = group_idx {
            let _group_end = [order_idx, limit_idx]
                .iter()
                .filter_map(|&x| x)
                .min()
                .unwrap_or(tokens.len());
            // GROUP BY <col>
            let group_col = if gi + 2 < tokens.len() {
                tokens[gi + 2].clone()
            } else {
                return "Error: Missing GROUP BY column".to_string();
            };

            // Group rows
            let mut groups: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
            let mut group_order: Vec<String> = Vec::new();
            for row in &filtered_rows {
                let key = row.get(&group_col).cloned().unwrap_or_default();
                if !groups.contains_key(&key) {
                    group_order.push(key.clone());
                }
                groups.entry(key).or_default().push(row.clone());
            }

            // Build result: for each group, evaluate select expressions
            let mut result_rows: Vec<Vec<String>> = Vec::new();
            let mut result_headers: Vec<String> = Vec::new();
            let mut headers_set = false;

            for key in &group_order {
                let group_rows = &groups[key];
                let mut result_row = Vec::new();

                for sel in select_tokens {
                    if sel == "," {
                        continue;
                    }
                    let sel_upper = sel.to_uppercase();
                    if sel_upper == "COUNT(*)" || sel_upper == "COUNT" {
                        if !headers_set {
                            result_headers.push("count()".to_string());
                        }
                        result_row.push(group_rows.len().to_string());
                    } else if sel == "*" {
                        // Not typical with GROUP BY but handle it
                        for col in column_order {
                            if !headers_set {
                                result_headers.push(col.clone());
                            }
                            result_row.push(
                                group_rows
                                    .first()
                                    .and_then(|r| r.get(col))
                                    .cloned()
                                    .unwrap_or_default(),
                            );
                        }
                    } else {
                        if !headers_set {
                            result_headers.push(sel.clone());
                        }
                        result_row.push(
                            group_rows
                                .first()
                                .and_then(|r| r.get(sel))
                                .cloned()
                                .unwrap_or_default(),
                        );
                    }
                }
                headers_set = true;
                result_rows.push(result_row);
            }

            // Handle "as" aliases in select tokens
            let final_headers = resolve_aliases(select_tokens, &result_headers);

            return format_tsv(&final_headers, &result_rows);
        }

        // Check for COUNT(*) in select
        let is_count = select_tokens.iter().any(|t| {
            let u = t.to_uppercase();
            u == "COUNT(*)" || u == "COUNT"
        });

        if is_count {
            return format!("{}\n", filtered_rows.len());
        }

        // Determine which columns to output
        let select_columns: Vec<String> = if select_tokens.len() == 1 && select_tokens[0] == "*" {
            column_order.clone()
        } else {
            select_tokens
                .iter()
                .filter(|t| *t != ",")
                .cloned()
                .collect()
        };

        // Apply ORDER BY
        let mut ordered_rows = filtered_rows;
        if let Some(oi) = order_idx {
            let order_end = limit_idx.unwrap_or(tokens.len());
            let order_col = if oi + 2 < tokens.len() {
                tokens[oi + 2].clone()
            } else {
                return "Error: Missing ORDER BY column".to_string();
            };

            let desc = if oi + 3 < tokens.len() && oi + 3 < order_end {
                tokens[oi + 3].to_uppercase() == "DESC"
            } else {
                false
            };

            ordered_rows.sort_by(|a, b| {
                let va = a.get(&order_col).cloned().unwrap_or_default();
                let vb = b.get(&order_col).cloned().unwrap_or_default();
                // Try numeric comparison
                let cmp = if let (Ok(na), Ok(nb)) = (va.parse::<f64>(), vb.parse::<f64>()) {
                    na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal)
                } else {
                    va.cmp(&vb)
                };
                if desc {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }

        // Apply LIMIT
        let limited_rows = if let Some(li) = limit_idx {
            let limit_val = if li + 1 < tokens.len() {
                tokens[li + 1].parse::<usize>().unwrap_or(usize::MAX)
            } else {
                usize::MAX
            };
            ordered_rows.into_iter().take(limit_val).collect()
        } else {
            ordered_rows
        };

        // Format output with column headers
        let mut result = String::new();
        result.push_str(&select_columns.join("\t"));
        result.push('\n');
        for row in &limited_rows {
            let values: Vec<String> = select_columns
                .iter()
                .map(|col| row.get(col).cloned().unwrap_or_default())
                .collect();
            result.push_str(&values.join("\t"));
            result.push('\n');
        }

        result
    }

    fn handle_insert(&self, database: &str, query: &str) -> String {
        let tokens = tokenize_sql(query);
        let _upper_query = query.to_uppercase();

        // Find INTO
        let into_idx = match tokens.iter().position(|t| t.to_uppercase() == "INTO") {
            Some(idx) => idx,
            None => return "Error: Missing INTO keyword".to_string(),
        };

        // Table name is after INTO
        if into_idx + 1 >= tokens.len() {
            return "Error: Missing table name after INTO".to_string();
        }
        let raw_table = &tokens[into_idx + 1];
        let (qual_db, table_name) = parse_qualified_name(raw_table);
        let db_name = resolve_database(&qual_db, database);
        let db = match self.storage.get_database(&db_name) { Some(d) => d, None => return "Error: Database not found".to_string() };

        if db.get_table(&table_name).is_none() {
            return format!("Error: Table {} not found", table_name);
        }

        // Find VALUES keyword
        let values_idx = match tokens.iter().position(|t| t.to_uppercase() == "VALUES") {
            Some(idx) => idx,
            None => return "Error: Missing VALUES keyword".to_string(),
        };

        // Parse value tuples
        let value_tuples = parse_value_tuples(&tokens, values_idx + 1);

        if value_tuples.is_empty() {
            return "Error: No values to insert".to_string();
        }

        // Insert each tuple
        for values in value_tuples {
            db.with_table_mut(&table_name, |table| {
                table.insert_row(values);
            });
        }

        "OK\n".to_string()
    }

    fn handle_create(&self, database: &str, query: &str) -> String {
        let upper = query.to_uppercase();

        // CREATE DATABASE
        if upper.contains("DATABASE") {
            return self.handle_create_database(query);
        }

        if !upper.contains("TABLE") {
            return "Error: Only CREATE TABLE and CREATE DATABASE supported".to_string();
        }

        // Extract everything between TABLE [IF NOT EXISTS] and the column defs or ENGINE
        let tokens = tokenize_sql(query);

        // Find TABLE keyword
        let table_idx = tokens.iter().position(|t| t.to_uppercase() == "TABLE").unwrap();

        // Check IF NOT EXISTS
        let mut name_idx = table_idx + 1;
        if name_idx < tokens.len() && tokens[name_idx].to_uppercase() == "IF" {
            // Skip IF NOT EXISTS (3 tokens)
            name_idx += 3;
        }

        if name_idx >= tokens.len() {
            return "Error: Missing table name".to_string();
        }

        let raw_table = &tokens[name_idx];
        let (qual_db, table_name) = parse_qualified_name(raw_table);
        let db_name = resolve_database(&qual_db, database);
        let db = match self.storage.get_database(&db_name) { Some(d) => d, None => return "Error: Database not found".to_string() };

        // If table already exists
        if db.get_table(&table_name).is_some() {
            let has_if_not_exists = query.to_uppercase().contains("IF NOT EXISTS");
            if has_if_not_exists {
                return "OK\n".to_string();
            }
            return format!("Error: Table '{}' already exists", table_name);
        }

        // Find column definitions between ( and )
        // Reconstruct from the original query after table name
        let after_table = &query[query.to_uppercase().find("TABLE").unwrap() + 5..];
        let after_table_upper = after_table.to_uppercase();
        let after_table = if after_table_upper.trim_start().starts_with("IF") {
            // Skip past IF NOT EXISTS
            let pos = after_table_upper.find("EXISTS").unwrap_or(0) + 6;
            &after_table[pos..]
        } else {
            after_table
        };

        // Find the first '(' and matching ')'
        if let Some(paren_start) = after_table.find('(') {
            let mut depth = 0;
            let mut paren_end = paren_start;
            for (i, ch) in after_table[paren_start..].char_indices() {
                if ch == '(' {
                    depth += 1;
                } else if ch == ')' {
                    depth -= 1;
                    if depth == 0 {
                        paren_end = paren_start + i;
                        break;
                    }
                }
            }

            let col_defs_str = &after_table[paren_start..=paren_end];
            let col_defs = parse_column_defs(col_defs_str);

            db.create_table(&table_name);
            db.with_table_mut(&table_name, |table| {
                for (col_name, col_type) in col_defs {
                    table.create_column(col_name, col_type);
                }
            });
        } else {
            // No column definitions, just create empty table
            db.create_table(&table_name);
        }

        "OK\n".to_string()
    }

    fn handle_create_database(&self, query: &str) -> String {
        let tokens = tokenize_sql(query);

        // Find DATABASE keyword
        let db_idx = tokens
            .iter()
            .position(|t| t.to_uppercase() == "DATABASE")
            .unwrap();

        let mut name_idx = db_idx + 1;
        if name_idx < tokens.len() && tokens[name_idx].to_uppercase() == "IF" {
            name_idx += 3; // skip IF NOT EXISTS
        }

        if name_idx >= tokens.len() {
            return "Error: Missing database name".to_string();
        }

        let db_name = &tokens[name_idx];
        self.storage.create_database(db_name);
        "OK\n".to_string()
    }

    fn handle_drop(&self, database: &str, query: &str) -> String {
        let upper = query.to_uppercase();

        // DROP DATABASE
        if upper.contains("DATABASE") {
            let tokens = tokenize_sql(query);
            let db_idx = tokens
                .iter()
                .position(|t| t.to_uppercase() == "DATABASE")
                .unwrap();
            let mut name_idx = db_idx + 1;
            if name_idx < tokens.len() && tokens[name_idx].to_uppercase() == "IF" {
                name_idx += 2; // skip IF EXISTS
            }
            if name_idx >= tokens.len() {
                return "Error: Missing database name".to_string();
            }
            let db_name = &tokens[name_idx];
            if self.storage.drop_database(db_name) {
                "OK\n".to_string()
            } else {
                format!("Error: Database {} not found", db_name)
            }
        } else if upper.contains("TABLE") {
            let tokens = tokenize_sql(query);
            let table_idx = tokens
                .iter()
                .position(|t| t.to_uppercase() == "TABLE")
                .unwrap();
            let mut name_idx = table_idx + 1;
            if name_idx < tokens.len() && tokens[name_idx].to_uppercase() == "IF" {
                name_idx += 3; // skip IF EXISTS
            }
            if name_idx >= tokens.len() {
                return "Error: Missing table name".to_string();
            }
            let raw_table = &tokens[name_idx];
            let (qual_db, table_name) = parse_qualified_name(raw_table);
            let db_name = resolve_database(&qual_db, database);
            let db = match self.storage.get_database(&db_name) { Some(d) => d, None => return "Error: Database not found".to_string() };
            if db.drop_table(&table_name) {
                "OK\n".to_string()
            } else {
                format!("Error: Table {} not found", table_name)
            }
        } else {
            "Error: Only DROP TABLE and DROP DATABASE supported".to_string()
        }
    }

    fn handle_show(&self, database: &str, query: &str) -> String {
        let upper = query.to_uppercase();

        if upper.contains("DATABASES") {
            let dbs = self.storage.list_databases();
            dbs.join("\n") + "\n"
        } else if upper.contains("TABLES") {
            // SHOW TABLES [FROM <database>]
            let tokens = tokenize_sql(query);
            let from_idx = find_keyword(&tokens, "FROM");
            let db_name = if let Some(fi) = from_idx {
                if fi + 1 < tokens.len() {
                    tokens[fi + 1].clone()
                } else {
                    database.to_string()
                }
            } else {
                database.to_string()
            };

            let db = match self.storage.get_database(&db_name) { Some(d) => d, None => return "Error: Database not found".to_string() };
            let tables = db.list_tables();
            tables.join("\n") + "\n"
        } else {
            "Error: Unsupported SHOW command".to_string()
        }
    }

    fn handle_describe(&self, database: &str, query: &str) -> String {
        let tokens = tokenize_sql(query);

        // DESCRIBE TABLE <name> or DESCRIBE <name>
        let table_idx = if tokens.len() > 1 && tokens[1].to_uppercase() == "TABLE" {
            2
        } else {
            1
        };

        if table_idx >= tokens.len() {
            return "Error: Missing table name".to_string();
        }

        let raw_table = &tokens[table_idx];
        let (qual_db, table_name) = parse_qualified_name(raw_table);
        let db_name = resolve_database(&qual_db, database);
        let db = match self.storage.get_database(&db_name) { Some(d) => d, None => return "Error: Database not found".to_string() };

        if let Some(table) = db.get_table(&table_name) {
            let mut result = String::new();
            // Output in column_order so it's deterministic
            for col_name in &table.column_order {
                if let Some(col_type) = table.column_types.get(col_name) {
                    result.push_str(&format!("{}\t{}\n", col_name, col_type));
                }
            }
            result
        } else {
            format!("Error: Table {} not found", table_name)
        }
    }

    fn handle_alter(&self, database: &str, query: &str) -> String {
        // ALTER TABLE <name> UPDATE col = val WHERE ...
        // ALTER TABLE <name> DELETE WHERE ...
        let tokens = tokenize_sql(query);

        // Find TABLE keyword
        let table_idx = match find_keyword(&tokens, "TABLE") {
            Some(idx) => idx,
            None => return "Error: Missing TABLE keyword in ALTER".to_string(),
        };

        if table_idx + 1 >= tokens.len() {
            return "Error: Missing table name".to_string();
        }

        let raw_table = &tokens[table_idx + 1];
        let (qual_db, table_name) = parse_qualified_name(raw_table);
        let db_name = resolve_database(&qual_db, database);
        let db = match self.storage.get_database(&db_name) { Some(d) => d, None => return "Error: Database not found".to_string() };

        // Find UPDATE or DELETE
        let update_idx = find_keyword(&tokens, "UPDATE");
        let delete_idx = find_keyword(&tokens, "DELETE");

        if let Some(ui) = update_idx {
            if ui > table_idx {
                // ALTER TABLE ... UPDATE col = val [, col2 = val2 ...] WHERE ...
                let where_idx = find_keyword(&tokens, "WHERE");

                // Parse assignments between UPDATE and WHERE
                let assign_end = where_idx.unwrap_or(tokens.len());
                let assign_tokens = &tokens[ui + 1..assign_end];

                let mut updates = HashMap::new();
                let mut i = 0;
                while i < assign_tokens.len() {
                    if assign_tokens[i] == "," {
                        i += 1;
                        continue;
                    }
                    let col = &assign_tokens[i];
                    if i + 2 < assign_tokens.len() && assign_tokens[i + 1] == "=" {
                        let val = unquote(&assign_tokens[i + 2]);
                        updates.insert(col.clone(), val);
                        i += 3;
                    } else {
                        i += 1;
                    }
                }

                // Build WHERE predicate
                let where_str = if let Some(wi) = where_idx {
                    tokens[wi + 1..].join(" ")
                } else {
                    String::new()
                };

                let count = db.with_table_mut(&table_name, |table| {
                    table.update_where(
                        |row| evaluate_where(row, &where_str),
                        &updates,
                    )
                });

                return format!("OK, {} rows updated\n", count.unwrap_or(0));
            }
        }

        if let Some(di) = delete_idx {
            if di > table_idx {
                // ALTER TABLE ... DELETE WHERE ...
                let where_idx = find_keyword(&tokens, "WHERE");

                let where_str = if let Some(wi) = where_idx {
                    tokens[wi + 1..].join(" ")
                } else {
                    String::new()
                };

                let count = db.with_table_mut(&table_name, |table| {
                    table.delete_where(|row| evaluate_where(row, &where_str))
                });

                return format!("OK, {} rows deleted\n", count.unwrap_or(0));
            }
        }

        "Error: Unsupported ALTER command".to_string()
    }
}

/// Resolve aliases from select tokens. E.g., "COUNT(*) as cnt" -> header should be "cnt".
fn resolve_aliases(select_tokens: &[String], headers: &[String]) -> Vec<String> {
    let mut result = Vec::new();
    let mut hi = 0;
    let mut i = 0;

    while i < select_tokens.len() {
        if select_tokens[i] == "," {
            i += 1;
            continue;
        }
        if hi >= headers.len() {
            break;
        }

        // Check if next token is "as"
        if i + 2 < select_tokens.len() && select_tokens[i + 1].to_uppercase() == "AS" {
            result.push(select_tokens[i + 2].clone());
            i += 3;
        } else {
            result.push(headers[hi].clone());
            i += 1;
        }
        hi += 1;
    }

    // If we ran out of select tokens but still have headers
    while hi < headers.len() {
        result.push(headers[hi].clone());
        hi += 1;
    }

    result
}

/// Format rows as TSV with headers (used for GROUP BY results).
fn format_tsv(_headers: &[String], rows: &[Vec<String>]) -> String {
    let mut result = String::new();
    for row in rows {
        result.push_str(&row.join("\t"));
        result.push('\n');
    }
    result
}

impl ClickHouseCommandHandler for DefaultClickHouseHandler {
    fn handle_query(&self, database: &str, query: &str) -> String {
        self.execute_query(database, query)
    }
}
