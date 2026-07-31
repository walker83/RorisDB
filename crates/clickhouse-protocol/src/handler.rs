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

    // Handle BETWEEN specially - don't split the AND inside "BETWEEN low AND high"
    // We need to protect the AND that's part of BETWEEN syntax
    let upper = where_clause.to_uppercase();
    if upper.contains("BETWEEN") {
        // Use BETWEEN-aware splitting: split by AND only outside of BETWEEN clauses
        let and_parts = split_by_and_respecting_between(where_clause);
        if and_parts.len() > 1 {
            return and_parts.iter().all(|part| evaluate_where(row, part));
        }
        // Single BETWEEN condition or unparseable, evaluate directly
        return evaluate_single_condition(row, where_clause);
    }

    // Split by AND (for non-BETWEEN conditions)
    let and_parts = split_by_keyword(where_clause, "AND");
    if and_parts.len() > 1 {
        return and_parts.iter().all(|part| evaluate_where(row, part));
    }

    // Single condition
    evaluate_single_condition(row, where_clause)
}

/// Split by AND, but protect the AND that appears inside "BETWEEN low AND high".
/// This handles cases like "age BETWEEN 10 AND 20 AND status = 'active'"
/// which should split into ["age BETWEEN 10 AND 20", "status = 'active'"]
fn split_by_and_respecting_between(clause: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut between_active = false;
    let mut between_operand_count = 0;
    let chars: Vec<char> = clause.chars().collect();
    let upper: String = clause.to_uppercase();
    let mut i = 0;
    
    while i < chars.len() {
        // Handle quoted strings
        if chars[i] == '\'' {
            in_quote = !in_quote;
            current.push(chars[i]);
            i += 1;
            continue;
        }
        
        if in_quote {
            current.push(chars[i]);
            i += 1;
            continue;
        }
        
        // Check for BETWEEN keyword (7 chars) with word boundary
        if !between_active && i + 7 <= chars.len() && &upper[i..i+7] == "BETWEEN" {
            let boundary_before = i == 0 || chars[i - 1].is_ascii_whitespace() || chars[i - 1] == '(';
            let boundary_after = i + 7 >= chars.len() || chars[i + 7].is_ascii_whitespace() || chars[i + 7] == '(';
            if boundary_before && boundary_after {
                between_active = true;
                between_operand_count = 0;
            }
        }
        
        // Check for AND keyword with word boundary
        if i + 3 <= chars.len() && &upper[i..i+3] == "AND" {
            let boundary_before = i == 0 || chars[i - 1].is_ascii_whitespace() || chars[i - 1] == '(';
            let boundary_after = i + 3 >= chars.len() || chars[i + 3].is_ascii_whitespace() || chars[i + 3] == '(';
            
            // Check if this AND is part of BETWEEN (after low operand)
            if between_active && between_operand_count == 0 {
                // This is the AND inside BETWEEN - include it and continue
                current.push_str(&chars[i..i+3].iter().collect::<String>());
                between_operand_count += 1; // Now expecting high operand
                i += 3;
                continue;
            } else if boundary_before && boundary_after {
                // This is a condition-separating AND (word boundary check)
                // But check if we're past the BETWEEN high operand
                if between_active && between_operand_count >= 1 {
                    between_active = false;
                }
                parts.push(current.trim().to_string());
                current.clear();
                i += 3;
                continue;
            }
        }
        
        current.push(chars[i]);
        i += 1;
    }
    
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    
    if parts.is_empty() {
        vec![clause.to_string()]
    } else {
        parts
    }
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

        if !in_quote && i + kw_len <= chars.len() {
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

/// Find the byte index of the first top-level (outside single-quoted literals)
/// occurrence of `keyword` in `s`, using ASCII-case-insensitive matching with
/// word-boundary checks. Returns the byte index of the keyword's first character.
/// Safe for non-ASCII UTF-8: operates on raw bytes; ASCII letters/quotes can never
/// appear as UTF-8 continuation bytes.
fn find_keyword_top_level(s: &str, keyword: &str) -> Option<usize> {
    let b = s.as_bytes();
    let kw: Vec<u8> = keyword.bytes().map(|c| c.to_ascii_uppercase()).collect();
    let kw_len = kw.len();
    let mut in_quote = false;
    let mut i = 0usize;
    while i < b.len() {
        if b[i] == b'\'' {
            in_quote = !in_quote;
            i += 1;
            continue;
        }
        if !in_quote && i + kw_len <= b.len() {
            let boundary_before = i == 0 || b[i - 1].is_ascii_whitespace();
            if boundary_before {
                let matches = b[i..i + kw_len]
                    .iter()
                    .zip(&kw)
                    .all(|(&a, &k)| a.to_ascii_uppercase() == k);
                if matches {
                    let after = i + kw_len;
                    let boundary_after = after >= b.len() || b[after].is_ascii_whitespace();
                    if boundary_after {
                        return Some(i);
                    }
                }
            }
        }
        i += 1;
    }
    None
}

/// Split a string on commas that appear outside single-quoted literals.
fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for c in s.chars() {
        if c == '\'' {
            in_quote = !in_quote;
            current.push(c);
        } else if c == ',' && !in_quote {
            parts.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(c);
        }
    }
    let last = current.trim().to_string();
    if !last.is_empty() {
        parts.push(last);
    }
    parts
}

/// Find the rightmost operator outside of quoted strings.
/// Returns (operator, position) if found, None otherwise.
/// Operators are checked in order, so multi-char operators like ">=" are matched before "=".
fn find_operator_outside_quotes(condition: &str, operators: &[&str]) -> Option<(String, usize)> {
    let b = condition.as_bytes();
    let mut in_quote = false;
    let mut best_match: Option<(String, usize)> = None;
    
    let mut i = 0;
    while i < b.len() {
        // Track quote state
        if b[i] == b'\'' {
            in_quote = !in_quote;
            i += 1;
            continue;
        }
        
        // Only match operators outside of quotes
        if !in_quote {
            for op in operators {
                let op_bytes = op.as_bytes();
                if i + op_bytes.len() <= b.len() {
                    let matches = b[i..i + op_bytes.len()]
                        .iter()
                        .zip(op_bytes)
                        .all(|(a, b)| a == b);
                    if matches {
                        // Check that this isn't part of a longer operator
                        // For example, don't match "=" in ">="
                        let is_longer_op = operators.iter().any(|other_op| {
                            other_op.len() > op.len() && other_op.starts_with(op) && 
                            i + other_op.len() <= b.len() &&
                            b[i..i + other_op.len()].iter()
                                .zip(other_op.as_bytes())
                                .all(|(a, b)| a == b)
                        });
                        
                        if !is_longer_op {
                            // Prefer rightmost match
                            best_match = Some((op.to_string(), i));
                        }
                    }
                }
            }
        }
        i += 1;
    }
    
    best_match
}

fn evaluate_single_condition(row: &HashMap<String, String>, condition: &str) -> bool {
    let condition = condition.trim();

    // Check for IS [NOT] NULL
    if let Some(is_pos) = find_keyword_top_level(condition, "IS") {
        let col = condition[..is_pos].trim().to_string();
        let rest = condition[is_pos + 2..].trim();
        let rest_upper = rest.to_ascii_uppercase();
        
        let is_null = if rest_upper.starts_with("NOT NULL") {
            false
        } else if rest_upper.starts_with("NULL") {
            true
        } else {
            return false;
        };

        let val = row.get(&col);
        let val_is_null = val.map(|v| v.is_empty()).unwrap_or(true);
        
        return if is_null {
            val_is_null
        } else {
            !val_is_null
        };
    }

    // Check for [NOT] BETWEEN
    if let Some(between_pos) = find_keyword_top_level(condition, "BETWEEN") {
        let before = condition[..between_pos].trim();
        let rest = &condition[between_pos + 7..]; // "BETWEEN" is 7 chars

        // Detect and strip a trailing NOT with word boundary (e.g. "age NOT BETWEEN ...")
        let before_upper = before.to_ascii_uppercase();
        let (col, negated) = if before_upper.ends_with("NOT")
            && (before.len() == 3 || before.as_bytes()[before.len() - 4].is_ascii_whitespace())
        {
            (before[..before.len() - 3].trim().to_string(), true)
        } else {
            (before.to_string(), false)
        };

        // Parse "low AND high" (top-level AND only, respecting quotes)
        if let Some(and_pos) = find_keyword_top_level(rest, "AND") {
            let low = unquote(rest[..and_pos].trim());
            let high = unquote(rest[and_pos + 3..].trim()); // "AND" is 3 chars

            let val = match row.get(&col) {
                Some(v) => v,
                None => return false,
            };

            // Try numeric comparison
            let val_num = val.parse::<f64>().ok();
            let low_num = low.parse::<f64>().ok();
            let high_num = high.parse::<f64>().ok();

            let in_range = if let (Some(v), Some(l), Some(h)) = (val_num, low_num, high_num) {
                v >= l && v <= h
            } else {
                val >= &low && val <= &high
            };

            return if negated { !in_range } else { in_range };
        }
        return false;
    }

    // Check for [NOT] IN
    if let Some(in_pos) = find_keyword_top_level(condition, "IN") {
        let before = condition[..in_pos].trim();
        let rest = condition[in_pos + 2..].trim(); // "IN" is 2 chars

        // Detect and strip a trailing NOT with word boundary (e.g. "status NOT IN ...")
        let before_upper = before.to_ascii_uppercase();
        let (col, negated) = if before_upper.ends_with("NOT")
            && (before.len() == 3 || before.as_bytes()[before.len() - 4].is_ascii_whitespace())
        {
            (before[..before.len() - 3].trim().to_string(), true)
        } else {
            (before.to_string(), false)
        };

        // Parse "(val1, val2, ...)" with quote-aware comma splitting
        if rest.starts_with('(') && rest.ends_with(')') {
            let list_str = rest[1..rest.len() - 1].trim();
            let values: Vec<String> = if list_str.is_empty() {
                vec![]
            } else {
                split_top_level_commas(list_str)
                    .into_iter()
                    .map(|s| unquote(s.trim()))
                    .collect()
            };

            let val = match row.get(&col) {
                Some(v) => v,
                None => return false,
            };

            let in_list = values.contains(val);
            return if negated { !in_list } else { in_list };
        }
        return false;
    }

    // Check for LIKE
    if let Some(like_pos) = find_keyword_top_level(condition, "LIKE") {
        let col = condition[..like_pos].trim().to_string();
        let rest = &condition[like_pos + 4..]; // "LIKE" is 4 chars

        // Parse pattern and optional ESCAPE clause (top-level, respecting quotes)
        let (pattern_str, escape_char) = if let Some(escape_pos) = find_keyword_top_level(rest, "ESCAPE") {
            let pattern = rest[..escape_pos].trim();
            let escape = rest[escape_pos + 6..].trim(); // "ESCAPE" is 6 chars
            let escape = unquote(escape);
            let escape_char = escape.chars().next();
            (pattern, escape_char)
        } else {
            (rest.trim(), None)
        };

        let pattern = unquote(pattern_str);

        let val = match row.get(&col) {
            Some(v) => v,
            None => return false,
        };

        return match_like_with_escape(val, &pattern, escape_char);
    }

    // Parse comparison: col OP val
    // Find the rightmost operator outside of quoted strings
    let operators = [">=", "<=", "!=", "<>", "=", ">", "<"];
    if let Some((op, pos)) = find_operator_outside_quotes(condition, &operators) {
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

        return match op.as_str() {
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

    false
}

/// Simple LIKE matching: % matches any sequence, _ matches any single char.
/// ClickHouse LIKE is case-sensitive. Test-only convenience wrapper around
/// [`match_like_with_escape`] with no escape character.
#[cfg(test)]
fn match_like(value: &str, pattern: &str) -> bool {
    match_like_with_escape(value, pattern, None)
}

/// LIKE matching with optional ESCAPE character.
/// ClickHouse LIKE is case-sensitive (no lowercasing).
/// Uses iterative DP to avoid exponential backtracking.
fn match_like_with_escape(value: &str, pattern: &str, escape: Option<char>) -> bool {
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();

    // Parse pattern into tokens, resolving escapes
    #[derive(Clone, Copy, PartialEq)]
    enum Tok {
        Many,   // %
        One,    // _
        Lit(char),
    }

    let mut toks: Vec<Tok> = Vec::with_capacity(p.len());
    let mut i = 0usize;
    while i < p.len() {
        let c = p[i];
        if escape == Some(c) && i + 1 < p.len() {
            // Escaped character: next char is literal
            toks.push(Tok::Lit(p[i + 1]));
            i += 2;
            continue;
        }
        match c {
            '%' => toks.push(Tok::Many),
            '_' => toks.push(Tok::One),
            _ => toks.push(Tok::Lit(c)),
        }
        i += 1;
    }

    let n = v.len();
    // Two-row DP: prev[j] = does v[..j] match the token prefix so far?
    let mut prev = vec![false; n + 1];
    let mut curr = vec![false; n + 1];
    prev[0] = true; // empty value matches empty pattern

    for tok in &toks {
        curr[0] = prev[0] && *tok == Tok::Many; // only % matches empty
        for j in 1..=n {
            curr[j] = match tok {
                Tok::Many => prev[j] || curr[j - 1],
                Tok::One => prev[j - 1],
                Tok::Lit(c) => prev[j - 1] && v[j - 1] == *c,
            };
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[n]
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
                    // Check for COUNT (tokenized as COUNT, (, *, ))
                    let is_count_star = sel_upper == "COUNT";
                    if is_count_star || sel_upper == "COUNT(*)" {
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

        // Check for aggregation functions in select (COUNT/SUM/AVG/MIN/MAX)
        // Tokens are split: e.g., COUNT, (, *, ) or SUM, (, col, )
        let upper_tokens: Vec<String> = select_tokens.iter().map(|t| t.to_uppercase()).collect();
        let has_count = upper_tokens.contains(&"COUNT".to_string());
        let has_sum = upper_tokens.contains(&"SUM".to_string());
        let has_avg = upper_tokens.contains(&"AVG".to_string());
        let has_min = upper_tokens.contains(&"MIN".to_string());
        let has_max = upper_tokens.contains(&"MAX".to_string());
        
        // Find the column for aggregation (for SUM/AVG/MIN/MAX)
        let agg_col = if has_sum || has_avg || has_min || has_max {
            // Find token after ( which is the column name
            let paren_idx = select_tokens.iter().position(|t| t == "(");
            paren_idx.and_then(|i| {
                if i + 1 < select_tokens.len() && select_tokens[i + 1] != "*" {
                    Some(select_tokens[i + 1].clone())
                } else {
                    None
                }
            })
        } else {
            None
        };
        
        if has_count {
            // COUNT(*) - return row count
            return format!("{}\n", filtered_rows.len());
        }
        
        if let Some(ref col) = agg_col {
            // Extract numeric values from the column
            let values: Vec<f64> = filtered_rows
                .iter()
                .filter_map(|row| row.get(col).and_then(|v| v.parse::<f64>().ok()))
                .collect();
            
            if has_sum {
                let sum: f64 = values.iter().sum();
                return format!("{:.6}\n", sum);
            }
            if has_avg {
                if !values.is_empty() {
                    let avg = values.iter().sum::<f64>() / values.len() as f64;
                    return format!("{:.6}\n", avg);
                } else {
                    return "0\n".to_string();
                }
            }
            if has_min {
                if let Some(min_val) = values.iter().cloned().min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)) {
                    return format!("{:.6}\n", min_val);
                } else {
                    return "NULL\n".to_string();
                }
            }
            if has_max {
                if let Some(max_val) = values.iter().cloned().max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)) {
                    return format!("{:.6}\n", max_val);
                } else {
                    return "NULL\n".to_string();
                }
            }
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
            let result = db.with_table_mut(&table_name, |table| table.insert_row(values));
            match result {
                Some(Err(e)) => return format!("Error: {}", e),
                None => return "Error: table disappeared during insert".to_string(),
                Some(Ok(())) => {}
            }
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
        let table_idx = match tokens.iter().position(|t| t.to_uppercase() == "TABLE") {
            Some(idx) => idx,
            None => return "Error: TABLE keyword not found in query".to_string(),
        };

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
        let after_table = match query.to_uppercase().find("TABLE") {
            Some(pos) => &query[pos + 5..],
            None => return "Error: TABLE keyword not found".to_string(),
        };
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
        let db_idx = match tokens.iter().position(|t| t.to_uppercase() == "DATABASE") {
            Some(idx) => idx,
            None => return "Error: DATABASE keyword not found in query".to_string(),
        };

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
            let db_idx = match tokens.iter().position(|t| t.to_uppercase() == "DATABASE") {
                Some(idx) => idx,
                None => return "Error: DATABASE keyword not found".to_string(),
            };
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
            let table_idx = match tokens.iter().position(|t| t.to_uppercase() == "TABLE") {
                Some(idx) => idx,
                None => return "Error: TABLE keyword not found".to_string(),
            };
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
fn format_tsv(headers: &[String], rows: &[Vec<String>]) -> String {
    let mut result = String::new();
    if !headers.is_empty() {
        result.push_str(&headers.join("\t"));
        result.push('\n');
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_like_basic_percent() {
        assert!(match_like("hello", "h%"));
        assert!(match_like("hello", "%o"));
        assert!(match_like("hello", "%ell%"));
        assert!(match_like("hello", "%"));
        assert!(!match_like("hello", "x%"));
    }

    #[test]
    fn test_like_basic_underscore() {
        assert!(match_like("hello", "h_llo"));
        assert!(match_like("hello", "_ello"));
        assert!(match_like("hello", "hell_"));
        assert!(!match_like("hello", "h_lo")); // needs exactly 1 char
    }

    #[test]
    fn test_like_no_match() {
        assert!(!match_like("hello", "world"));
        assert!(!match_like("hello", "h%x"));
        assert!(!match_like("hello", "x%o"));
    }

    #[test]
    fn test_like_consecutive_percent() {
        assert!(match_like("hello", "h%%o"));
        assert!(match_like("hello", "%%%"));
        assert!(match_like("hello", "h%%"));
    }

    #[test]
    fn test_like_leading_trailing_percent() {
        assert!(match_like("hello", "%hello%"));
        assert!(match_like("hello", "hell%")); // trailing %: starts with "hell"
        assert!(match_like("hello", "%llo"));  // leading %: ends with "llo"
    }

    #[test]
    fn test_like_escape() {
        // ESCAPE '!' makes ! the escape char
        assert!(match_like_with_escape("a%b", "a!%b", Some('!')));
        assert!(!match_like_with_escape("aXb", "a!%b", Some('!')));
        assert!(match_like_with_escape("a_b", "a!_b", Some('!')));
        assert!(!match_like_with_escape("aXb", "a!_b", Some('!')));
        
        // Unescaped wildcards still work
        assert!(match_like_with_escape("aXb", "a%b", Some('!')));
        assert!(match_like_with_escape("aXb", "a_b", Some('!')));
    }

    #[test]
    fn test_like_case_sensitive() {
        // ClickHouse LIKE is case-sensitive
        assert!(match_like("ABC", "ABC"));
        assert!(!match_like("ABC", "abc"));
        assert!(!match_like("abc", "ABC"));
        assert!(match_like("Hello", "H%"));
        assert!(!match_like("Hello", "h%"));
    }

    #[test]
    fn test_like_dos_regression() {
        // Adversarial pattern should complete quickly with iterative DP
        let long_value = "a".repeat(200);
        let adversarial = "%a%a%a%a%a%a%a%a%a%a%b";
        // Should return false and not hang
        assert!(!match_like(&long_value, adversarial));
        
        // Another adversarial case
        let long_value2 = "x".repeat(100);
        assert!(!match_like(&long_value2, "%x%x%x%x%x%x%x%x%x%y"));
    }

    #[test]
    fn test_evaluate_between() {
        let mut row = HashMap::new();
        row.insert("age".to_string(), "25".to_string());
        row.insert("name".to_string(), "alice".to_string());

        // Numeric BETWEEN
        assert!(evaluate_single_condition(&row, "age BETWEEN 20 AND 30"));
        assert!(evaluate_single_condition(&row, "age BETWEEN 25 AND 25"));
        assert!(!evaluate_single_condition(&row, "age BETWEEN 30 AND 40"));
        
        // NOT BETWEEN
        assert!(!evaluate_single_condition(&row, "age NOT BETWEEN 20 AND 30"));
        assert!(evaluate_single_condition(&row, "age NOT BETWEEN 30 AND 40"));
        
        // String BETWEEN
        assert!(evaluate_single_condition(&row, "name BETWEEN 'a' AND 'z'"));
        assert!(!evaluate_single_condition(&row, "name BETWEEN 'b' AND 'z'"));
    }

    #[test]
    fn test_evaluate_in() {
        let mut row = HashMap::new();
        row.insert("status".to_string(), "active".to_string());
        row.insert("id".to_string(), "42".to_string());

        // IN with match
        assert!(evaluate_single_condition(&row, "status IN ('active', 'pending')"));
        assert!(evaluate_single_condition(&row, "id IN (1, 42, 100)"));
        
        // IN with no match
        assert!(!evaluate_single_condition(&row, "status IN ('inactive', 'pending')"));
        assert!(!evaluate_single_condition(&row, "id IN (1, 2, 3)"));
        
        // NOT IN
        assert!(!evaluate_single_condition(&row, "status NOT IN ('active', 'pending')"));
        assert!(evaluate_single_condition(&row, "status NOT IN ('inactive', 'pending')"));
    }

    #[test]
    fn test_evaluate_is_null() {
        let mut row = HashMap::new();
        row.insert("name".to_string(), "alice".to_string());
        row.insert("empty_col".to_string(), "".to_string());

        // IS NULL on missing column
        assert!(evaluate_single_condition(&row, "missing IS NULL"));
        
        // IS NULL on empty string (treated as NULL)
        assert!(evaluate_single_condition(&row, "empty_col IS NULL"));
        
        // IS NULL on non-empty value
        assert!(!evaluate_single_condition(&row, "name IS NULL"));
        
        // IS NOT NULL
        assert!(evaluate_single_condition(&row, "name IS NOT NULL"));
        assert!(!evaluate_single_condition(&row, "missing IS NOT NULL"));
        assert!(!evaluate_single_condition(&row, "empty_col IS NOT NULL"));
    }

    #[test]
    fn test_evaluate_like_with_escape() {
        let mut row = HashMap::new();
        row.insert("pattern".to_string(), "a%b".to_string());

        // LIKE with ESCAPE clause
        assert!(evaluate_single_condition(&row, "pattern LIKE 'a!%b' ESCAPE '!'"));
        assert!(!evaluate_single_condition(&row, "pattern LIKE 'aXb' ESCAPE '!'"));
        
        // Regular LIKE without ESCAPE
        row.insert("text".to_string(), "hello".to_string());
        assert!(evaluate_single_condition(&row, "text LIKE 'h%'"));
        assert!(!evaluate_single_condition(&row, "text LIKE 'x%'"));
    }

    #[test]
    fn test_evaluate_in_quoted_comma() {
        let mut row = HashMap::new();
        row.insert("name".to_string(), "Smith, John".to_string());

        // Value containing a comma inside quotes must be matched as a whole
        assert!(evaluate_single_condition(&row, "name IN ('Smith, John', 'Doe')"));
        assert!(!evaluate_single_condition(&row, "name IN ('Smith', 'Doe')"));
        assert!(evaluate_single_condition(&row, "name NOT IN ('Doe', 'Roe')"));
        assert!(!evaluate_single_condition(&row, "name NOT IN ('Smith, John')"));
    }

    #[test]
    fn test_evaluate_not_boundary_column() {
        // Column literally named "knot" must NOT be misread as column "k" + NOT
        let mut row = HashMap::new();
        row.insert("knot".to_string(), "x".to_string());

        assert!(evaluate_single_condition(&row, "knot IN ('x')"));
        assert!(evaluate_single_condition(&row, "knot NOT IN ('y')"));
        assert!(!evaluate_single_condition(&row, "knot NOT IN ('x')"));
    }

    #[test]
    fn test_evaluate_non_ascii_no_panic() {
        let mut row = HashMap::new();
        row.insert("x".to_string(), "v".to_string());

        // Non-ASCII content before/around keywords must not panic (char-boundary safe)
        let _ = evaluate_single_condition(&row, "x = '\u{FB01}'");
        let _ = evaluate_single_condition(&row, "x\u{FB01} IS NULL");
        let _ = evaluate_single_condition(&row, "x LIKE '\u{00E9}%'");
        let _ = evaluate_single_condition(&row, "x IN ('\u{00FC}', 'v')");
        // Reached here => no panic
    }

    #[test]
    fn test_evaluate_between_and_in_quotes() {
        let mut row = HashMap::new();
        row.insert("n".to_string(), "m".to_string());

        // The AND inside the quoted low bound must be ignored; split at top-level AND
        assert!(evaluate_single_condition(&row, "n BETWEEN 'a AND b' AND 'z'"));
        assert!(!evaluate_single_condition(&row, "n BETWEEN 'n AND z' AND 'z'"));
    }

    #[test]
    fn test_evaluate_value_contains_keyword() {
        let mut row = HashMap::new();
        row.insert("s".to_string(), "this IS fine".to_string());
        row.insert("t".to_string(), "ACTIVE IN REGION".to_string());

        // A quoted value containing a keyword must not be hijacked by that keyword's branch
        assert!(evaluate_single_condition(&row, "s = 'this IS fine'"));
        assert!(evaluate_single_condition(&row, "t = 'ACTIVE IN REGION'"));
        assert!(!evaluate_single_condition(&row, "s = 'something else'"));
    }

    #[test]
    fn test_evaluate_in_empty() {
        let mut row = HashMap::new();
        row.insert("x".to_string(), "anything".to_string());
        row.insert("e".to_string(), "".to_string());

        // IN () is always false; NOT IN () is always true
        assert!(!evaluate_single_condition(&row, "x IN ()"));
        assert!(evaluate_single_condition(&row, "x NOT IN ()"));
        assert!(!evaluate_single_condition(&row, "e IN ()"));
        assert!(evaluate_single_condition(&row, "e NOT IN ()"));
    }
}

#[test]
fn test_between_debug() {
    let mut row = HashMap::new();
    row.insert("input_tokens".to_string(), "1693".to_string());
    
    // Test the exact condition
    let cond = "input_tokens BETWEEN 0 AND 100000";
    let result = evaluate_single_condition(&row, cond);
    println!("Condition: '{}' on row with input_tokens=1693, result={}", cond, result);
    assert!(result, "BETWEEN 0 AND 100000 should match value 1693");
    
    // Test narrower range
    let cond2 = "input_tokens BETWEEN 1000 AND 5000";
    let result2 = evaluate_single_condition(&row, cond2);
    println!("Condition: '{}' on row with input_tokens=1693, result={}", cond2, result2);
    assert!(result2, "BETWEEN 1000 AND 5000 should match value 1693");
}

#[test]
fn test_evaluate_where_between() {
    let mut row = HashMap::new();
    row.insert("input_tokens".to_string(), "1693".to_string());
    
    // Test evaluate_where with BETWEEN
    let cond = "input_tokens BETWEEN 0 AND 100000";
    let result = evaluate_where(&row, cond);
    println!("evaluate_where: '{}' on row with input_tokens=1693, result={}", cond, result);
    assert!(result, "evaluate_where BETWEEN should work");
    
    // Test with the condition that fails in real query
    let cond2 = "input_tokens BETWEEN 1000 AND 5000";
    let result2 = evaluate_where(&row, cond2);
    println!("evaluate_where: '{}' on row with input_tokens=1693, result={}", cond2, result2);
    assert!(result2, "evaluate_where BETWEEN 1000-5000 should match 1693");
}
