//! Oracle command handler

use crate::storage::OracleStorage;
use crate::tns::{TnsPacket, TnsPacketType};
use std::collections::HashMap;
use std::sync::Arc;

/// Trait for handling Oracle commands
pub trait OracleCommandHandler: Send + Sync {
    fn handle_connect(&self, service: &str) -> Vec<u8>;
    fn handle_query(&self, schema: &str, sql: &str) -> Vec<u8>;
}

/// Default Oracle command handler
pub struct DefaultOracleHandler {
    storage: Arc<OracleStorage>,
}

impl DefaultOracleHandler {
    pub fn new(storage: Arc<OracleStorage>) -> Self {
        Self { storage }
    }

    fn execute_sql(&self, schema: &str, sql: &str) -> String {
        let sql = sql.trim().trim_end_matches(';');
        let upper = sql.to_uppercase();

        if upper.starts_with("SELECT") {
            // Extract the select expression (between SELECT and FROM)
            let expr = if let Some(from_pos) = upper.find(" FROM ") {
                sql[7..from_pos].trim()
            } else {
                sql[7..].trim()
            };
            let expr_upper = expr.to_uppercase();

            // SELECT USER / SELECT USER FROM DUAL
            if expr_upper == "USER" {
                return format!("USER\n-----\n{}\n", schema);
            }

            // SELECT SYSDATE FROM DUAL
            if expr_upper == "SYSDATE" {
                let now = chrono::Utc::now();
                return format!("SYSDATE\n-------\n{}\n", now.format("%Y-%m-%d %H:%M:%S"));
            }

            // SELECT * FROM v$version
            if expr == "*" && upper.contains("V$VERSION") {
                return "BANNER\n----------------------------------------\nHarnessDB Oracle Compatibility 19.0.0.0.0\n".to_string();
            }

            // SELECT LENGTH('xxx') FROM DUAL
            if let Some(rest) = expr_upper.strip_prefix("LENGTH(").and_then(|s| s.strip_suffix(')')) {
                let inner = expr["LENGTH(".len()..expr.len()-1].trim().trim_matches('\'');
                return format!("LENGTH('{}')\n----------\n{}\n", inner, inner.len());
            }

            // SELECT 'literal' FROM DUAL
            if expr.starts_with('\'') && expr.ends_with('\'') && expr.len() >= 2 {
                let literal = &expr[1..expr.len()-1];
                return format!("'{}'\n------\n{}\n", literal, literal);
            }

            // SELECT <number> FROM DUAL (integer)
            if let Ok(n) = expr.parse::<i64>() {
                return format!("{}\n-----\n{}\n", expr, n);
            }

            // Arithmetic: SELECT a + b, SELECT a * b, etc.
            for (op_str, op_char) in &[
                (" + ", '+'), (" - ", '-'), (" * ", '*'), (" / ", '/'),
            ] {
                if let Some(pos) = expr.find(op_str) {
                    let left = expr[..pos].trim();
                    let right = expr[pos+op_str.len()..].trim();
                    if let (Ok(a), Ok(b)) = (left.parse::<f64>(), right.parse::<f64>()) {
                        let result = match op_char {
                            '+' => a + b,
                            '-' => a - b,
                            '*' => a * b,
                            '/' => if b != 0.0 { a / b } else { f64::NAN },
                            _ => 0.0,
                        };
                        let formatted = if result.fract() == 0.0 {
                            format!("{}", result as i64)
                        } else {
                            format!("{}", result)
                        };
                        return format!("{}\n------\n{}\n", expr, formatted);
                    }
                }
            }

            // SELECT ... FROM DUAL (fallback)
            if upper.contains("FROM DUAL") {
                return format!("EXPR\n----\n{}\n", expr);
            }
        }

        if upper == "SHOW USER" || upper == "SHOW USER;" {
            return format!("USER is \"{}\"", schema);
        }

        "Statement processed".to_string()
    }
}

impl OracleCommandHandler for DefaultOracleHandler {
    fn handle_connect(&self, service: &str) -> Vec<u8> {
        // Return ACCEPT packet
        let accept_data = vec![
            0x00, 0x00, // Version
            0x00, 0x00, // Compatible
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Options
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let packet = TnsPacket::new(TnsPacketType::Accept, accept_data);
        let mut buf = bytes::BytesMut::new();
        packet.encode(&mut buf);
        buf.to_vec()
    }

    fn handle_query(&self, schema: &str, sql: &str) -> Vec<u8> {
        let result = self.execute_sql(schema, sql);

        // Return DATA packet with result
        let packet = TnsPacket::new(TnsPacketType::Data, result.into_bytes());
        let mut buf = bytes::BytesMut::new();
        packet.encode(&mut buf);
        buf.to_vec()
    }
}
