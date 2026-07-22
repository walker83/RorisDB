#[cfg(test)]
mod bug_tests {
    use fe_sql_parser::{Statement, parse_sql};

    #[test]
    fn test_insert_set_case_insensitive() {
        // Lowercase "set" should work
        let result = parse_sql("INSERT INTO t set a = 1");
        assert!(result.is_ok(), "INSERT INTO t set a = 1 should parse: {:?}", result);
    }

    #[test]
    fn test_empty_sql() {
        let result = parse_sql("");
        // Should return error or empty vec, not panic
        println!("Empty SQL result: {:?}", result);
    }

    #[test]
    fn test_whitespace_only() {
        let result = parse_sql("   ");
        println!("Whitespace SQL result: {:?}", result);
    }

    #[test]
    fn test_commit_with_semicolon() {
        let result = parse_sql("COMMIT;");
        assert!(result.is_ok(), "COMMIT; should parse: {:?}", result);
    }

    #[test]
    fn test_create_catalog_lowercase() {
        let result = parse_sql("create catalog mycat");
        assert!(result.is_ok(), "create catalog should parse: {:?}", result);
    }

    #[test]
    fn test_refresh_catalog_lowercase() {
        let result = parse_sql("refresh catalog mycat");
        assert!(result.is_ok(), "refresh catalog should parse: {:?}", result);
    }

    #[test]
    fn test_create_temporary_table() {
        let result = parse_sql("CREATE TEMPORARY TABLE t1 (id INT)");
        assert!(result.is_ok(), "CREATE TEMPORARY TABLE should parse: {:?}", result);
    }

    #[test]
    fn test_set_global_lowercase() {
        let result = parse_sql("set @@global.max_connections = 1000");
        assert!(result.is_ok(), "set @@global should parse: {:?}", result);
    }

    #[test]
    fn test_drop_analyze_job_lowercase() {
        // Dispatch is case-insensitive; the strip_prefix must be too, so the
        // lowercase form parses instead of erroring.
        let result = parse_sql("drop analyze job job_123");
        assert!(result.is_ok(), "lowercase drop analyze job should parse: {:?}", result);
    }

    #[test]
    fn test_drop_analyze_job_mixed_case() {
        let result = parse_sql("Drop Analyze Job job_123");
        assert!(result.is_ok(), "mixed-case drop analyze job should parse: {:?}", result);
    }

    #[test]
    fn test_set_value_escaped_backslash_before_quote() {
        // `'foo\\'bar'` → after correct unescaping the value is `foo\'bar`:
        // `\\` is an escaped backslash, then a literal quote. A naive
        // replace-chain would have collapsed `\\'` into `'` incorrectly.
        use fe_sql_parser::ast::{Expr, LiteralValue, Statement};
        let stmts = parse_sql("SET x = 'foo\\\\'bar'").expect("should parse");
        match stmts.first() {
            Some(Statement::SetVariable(sv)) => match &sv.value {
                Expr::Literal(LiteralValue::String(s)) => {
                    assert_eq!(s, "foo\\'bar");
                }
                other => panic!("expected string literal, got {:?}", other),
            },
            other => panic!("expected SetVariable, got {:?}", other),
        }
    }
}
