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
}
