//! AnalyticDB MySQL command handler — implements mysql-protocol's QueryHandler
//! using sqlparser for SQL parsing and AdbMysqlStorage for data.

use crate::storage::{AdbMysqlStorage, ColumnDef as StorageColumnDef, ColumnType as StorageColumnType};
use dashmap::DashMap;
use mysql_protocol::server::{ColumnDef, ColumnType, QueryHandler, QueryResult};
use sqlparser::ast::{
    Delete, Expr, FromTable, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    Insert, OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, Use, Value, Values, AssignmentTarget,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;
use tracing::{debug, warn};

/// Which aggregation function to apply.
///
/// Variants other than `CountStar` carry the index of the column they
/// aggregate over (resolved against `table_columns` at projection time).
enum AggFunc {
    CountStar,
    /// COUNT(col): counts non-NULL (non-empty) values of the column.
    Count(usize),
    Sum(usize),
    Avg(usize),
    Min(usize),
    Max(usize),
}

impl AggFunc {
    /// The column index the aggregate operates on, or `None` for COUNT(*).
    fn col_index(&self) -> Option<usize> {
        match self {
            AggFunc::CountStar => None,
            AggFunc::Count(i) | AggFunc::Sum(i) | AggFunc::Avg(i) | AggFunc::Min(i) | AggFunc::Max(i) => {
                Some(*i)
            }
        }
    }
}

/// Describes what each output column should contain
enum OutputCol {
    /// A column from the table by index
    TableColumn(usize),
    /// An aggregate function
    Aggregate(AggFunc),
    /// A literal value (for SELECT without FROM)
    Literal(String),
}

pub struct AdbMysqlHandler {
    storage: Arc<AdbMysqlStorage>,
    /// Per-connection current database tracking
    current_databases: DashMap<u32, String>,
}

impl AdbMysqlHandler {
    pub fn new(storage: Arc<AdbMysqlStorage>) -> Self {
        Self {
            storage,
            current_databases: DashMap::new(),
        }
    }

    fn get_database(&self, conn_id: u32) -> String {
        self.current_databases
            .get(&conn_id)
            .map(|d| d.value().clone())
            .unwrap_or_else(|| "default".to_string())
    }

    fn parse_sql(&self, sql: &str) -> Result<Vec<Statement>, String> {
        let dialect = MySqlDialect {};
        Parser::parse_sql(&dialect, sql).map_err(|e| format!("Parse error: {}", e))
    }

    fn ok_result() -> QueryResult {
        QueryResult::with_rows(
            vec![ColumnDef {
                name: "status".to_string(),
                col_type: ColumnType::String,
            }],
            vec![vec![Some("OK".to_string())]],
        )
    }

    fn execute_statement(&self, _conn_id: u32, database: &str, stmt: Statement) -> QueryResult {
        match stmt {
            Statement::Query(query) => self.execute_select(database, &query),
            Statement::Insert(insert) => self.execute_insert(database, &insert),
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => self.execute_update(database, table, assignments, selection),
            Statement::Delete(delete) => self.execute_delete(database, &delete),
            Statement::CreateTable(ct) => {
                let table_name = ct.name.to_string();
                let columns: Vec<StorageColumnDef> = ct
                    .columns
                    .iter()
                    .map(|col| {
                        let col_type = StorageColumnType::from_sql_type(&col.data_type.to_string());
                        StorageColumnDef {
                            name: col.name.value.clone(),
                            col_type,
                        }
                    })
                    .collect();

                if self.storage.get_database(database).is_none() {
                    self.storage.create_database(database);
                }
                if let Some(db) = self.storage.get_database(database) {
                    db.create_table(&table_name, columns);
                }
                Self::ok_result()
            }
            Statement::Drop { object_type, names, .. } => {
                let is_table = matches!(object_type, sqlparser::ast::ObjectType::Table);
                for name in &names {
                    let obj_name = name.to_string();
                    if is_table {
                        if let Some(db) = self.storage.get_database(database) {
                            db.drop_table(&obj_name);
                        }
                    } else {
                        // Schema/Database
                        self.storage.drop_database(&obj_name);
                    }
                }
                Self::ok_result()
            }
            Statement::ShowDatabases { .. } => {
                let dbs = self.storage.list_databases();
                let cols = vec![ColumnDef {
                    name: "Database".to_string(),
                    col_type: ColumnType::String,
                }];
                let rows: Vec<Vec<Option<String>>> =
                    dbs.into_iter().map(|db| vec![Some(db)]).collect();
                QueryResult::with_rows(cols, rows)
            }
            Statement::ShowTables { .. } => {
                let tables = if let Some(db) = self.storage.get_database(database) {
                    db.list_tables()
                } else {
                    vec![]
                };
                let col_name = format!("Tables_in_{}", database);
                let cols = vec![ColumnDef {
                    name: col_name,
                    col_type: ColumnType::String,
                }];
                let rows: Vec<Vec<Option<String>>> =
                    tables.into_iter().map(|t| vec![Some(t)]).collect();
                QueryResult::with_rows(cols, rows)
            }
            Statement::Use(use_expr) => {
                let db_name = match &use_expr {
                    Use::Database(name) => name.to_string(),
                    Use::Schema(name) => name.to_string(),
                    Use::Object(name) => name.to_string(),
                    _ => "default".to_string(),
                };
                if self.storage.get_database(&db_name).is_none() {
                    self.storage.create_database(&db_name);
                }
                Self::ok_result()
            }
            Statement::CreateDatabase { db_name, .. } => {
                let name = db_name.to_string();
                self.storage.create_database(&name);
                Self::ok_result()
            }
            Statement::Truncate { table_names, .. } => {
                for target in table_names {
                    let name = target.name.to_string();
                    if let Some(db) = self.storage.get_database(database) {
                        if let Some(tbl) = db.get_table(&name) {
                            tbl.truncate();
                        }
                    }
                }
                Self::ok_result()
            }
            _ => {
                debug!("Unhandled statement: {:?}", stmt);
                QueryResult::ok()
            }
        }
    }

    fn execute_select(&self, database: &str, query: &Query) -> QueryResult {
        let select = match query.body.as_ref() {
            SetExpr::Select(s) => s,
            _ => return QueryResult::ok(),
        };

        // Handle SELECT without FROM (e.g., SELECT 1)
        if select.from.is_empty() {
            return self.execute_select_no_from(select);
        }

        // Get table name
        let table_name = match &select.from[0].relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return QueryResult::ok(),
        };

        let db = match self.storage.get_database(database) {
            Some(db) => db,
            None => return QueryResult::ok(),
        };

        let table = match db.get_table(&table_name) {
            Some(t) => t,
            None => return QueryResult::ok(),
        };

        let all_rows = table.select_all_ordered();
        let table_columns = &table.columns;

        // Resolve output columns
        let output_cols = self.resolve_output_columns(select, table_columns);

        // Check if this is a GROUP BY query
        let is_group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
            GroupByExpr::All(_) => true,
        };

        // Check for aggregate without GROUP BY (e.g., SELECT COUNT(*) FROM users)
        let has_aggregates = output_cols.iter().any(|c| matches!(c, OutputCol::Aggregate(_)));

        // Filter rows by WHERE
        let filtered: Vec<Vec<String>> = if let Some(ref where_expr) = select.selection {
            all_rows
                .into_iter()
                .filter(|(_, row)| self.eval_where(where_expr, row, table_columns))
                .map(|(_, row)| row)
                .collect()
        } else {
            all_rows.into_iter().map(|(_, row)| row).collect()
        };

        if is_group_by {
            return self.execute_group_by(select, &filtered, table_columns, &output_cols);
        }

        if has_aggregates {
            // Aggregate over all filtered rows (no GROUP BY)
            let mut row = vec![];
            for col in &output_cols {
                match col {
                    OutputCol::Aggregate(agg) => {
                        let idx = agg.col_index().unwrap_or(0);
                        row.push(compute_aggregate(agg, filtered.iter(), idx));
                    }
                    _ => row.push(None),
                }
            }
            let cols = self.build_result_column_defs(&output_cols, table_columns, select);
            return QueryResult::with_rows(cols, vec![row]);
        }

        // Project columns
        let projected: Vec<Vec<Option<String>>> = filtered
            .iter()
            .map(|row| {
                output_cols
                    .iter()
                    .map(|col| match col {
                        OutputCol::TableColumn(idx) => row.get(*idx).cloned().map(Some).unwrap_or(None),
                        OutputCol::Aggregate(agg) => {
                            // Aggregates are constant across rows; compute over
                            // the whole filtered set. (Reached only when an
                            // aggregate appears without triggering the
                            // has_aggregates branch above.)
                            let idx = agg.col_index().unwrap_or(0);
                            compute_aggregate(agg, filtered.iter(), idx)
                        }
                        OutputCol::Literal(v) => Some(v.clone()),
                    })
                    .collect()
            })
            .collect();

        // Handle ORDER BY
        let ordered = if let Some(ref order_by) = query.order_by {
            self.apply_order_by(&order_by.exprs, projected, &output_cols, table_columns)
        } else {
            projected
        };

        // Handle LIMIT
        let limited = if let Some(ref limit) = query.limit {
            if let Expr::Value(Value::Number(n, _)) = limit {
                let n: usize = n.parse().unwrap_or(usize::MAX);
                ordered.into_iter().take(n).collect()
            } else {
                ordered
            }
        } else {
            ordered
        };

        let cols = self.build_result_column_defs(&output_cols, table_columns, select);
        QueryResult::with_rows(cols, limited)
    }

    fn execute_select_no_from(&self, select: &Select) -> QueryResult {
        let mut cols = vec![];
        let mut row = vec![];

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (val, name) = self.eval_simple_expr(expr);
                    cols.push(ColumnDef {
                        name,
                        col_type: ColumnType::String,
                    });
                    row.push(Some(val));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let (val, _) = self.eval_simple_expr(expr);
                    cols.push(ColumnDef {
                        name: alias.value.clone(),
                        col_type: ColumnType::String,
                    });
                    row.push(Some(val));
                }
                _ => {}
            }
        }

        QueryResult::with_rows(cols, vec![row])
    }

    fn eval_simple_expr(&self, expr: &Expr) -> (String, String) {
        match expr {
            Expr::Value(Value::Number(n, _)) => (n.clone(), n.clone()),
            Expr::Value(Value::SingleQuotedString(s)) => (s.clone(), s.clone()),
            Expr::Value(Value::DoubleQuotedString(s)) => (s.clone(), format!("\"{}\"", s)),
            Expr::Identifier(ident) => {
                let name = ident.value.clone();
                (name.clone(), name)
            }
            Expr::BinaryOp { left, op, right } => {
                let (lv, _) = self.eval_simple_expr(left);
                let (rv, _) = self.eval_simple_expr(right);
                let result = match op {
                    sqlparser::ast::BinaryOperator::Plus => {
                        if let (Ok(l), Ok(r)) = (lv.parse::<f64>(), rv.parse::<f64>()) {
                            format_f64(l + r)
                        } else {
                            format!("{}+{}", lv, rv)
                        }
                    }
                    sqlparser::ast::BinaryOperator::Minus => {
                        if let (Ok(l), Ok(r)) = (lv.parse::<f64>(), rv.parse::<f64>()) {
                            format_f64(l - r)
                        } else {
                            format!("{}-{}", lv, rv)
                        }
                    }
                    sqlparser::ast::BinaryOperator::Multiply => {
                        if let (Ok(l), Ok(r)) = (lv.parse::<f64>(), rv.parse::<f64>()) {
                            format_f64(l * r)
                        } else {
                            format!("{}*{}", lv, rv)
                        }
                    }
                    _ => "?".to_string(),
                };
                let display = format!("{} {} {}", lv, op, rv);
                (result, display)
            }
            _ => ("?".to_string(), format!("{:?}", expr)),
        }
    }

    fn resolve_output_columns(
        &self,
        select: &Select,
        table_columns: &[StorageColumnDef],
    ) -> Vec<OutputCol> {
        let mut result = vec![];
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.clone();
                    let idx = table_columns.iter().position(|c| c.name == col_name);
                    if let Some(i) = idx {
                        result.push(OutputCol::TableColumn(i));
                    } else {
                        result.push(OutputCol::Literal(String::new()));
                    }
                }
                SelectItem::ExprWithAlias {
                    expr: Expr::Identifier(ident),
                    alias: _,
                } => {
                    let col_name = ident.value.clone();
                    let idx = table_columns.iter().position(|c| c.name == col_name);
                    if let Some(i) = idx {
                        result.push(OutputCol::TableColumn(i));
                    } else {
                        result.push(OutputCol::Literal(String::new()));
                    }
                }
                SelectItem::Wildcard(_) => {
                    for (i, _) in table_columns.iter().enumerate() {
                        result.push(OutputCol::TableColumn(i));
                    }
                }
            SelectItem::UnnamedExpr(Expr::Function(f)) => {
                match resolve_agg_func(f, table_columns) {
                    Some(agg) => result.push(OutputCol::Aggregate(agg)),
                    None => result.push(OutputCol::Literal(String::new())),
                }
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Function(f),
                alias: _,
            } => {
                match resolve_agg_func(f, table_columns) {
                    Some(agg) => result.push(OutputCol::Aggregate(agg)),
                    None => result.push(OutputCol::Literal(String::new())),
                }
            }
                _ => {
                    result.push(OutputCol::Literal(String::new()));
                }
            }
        }
        result
    }

    fn build_result_column_defs(
        &self,
        output_cols: &[OutputCol],
        table_columns: &[StorageColumnDef],
        select: &Select,
    ) -> Vec<ColumnDef> {
        let mut cols = vec![];
        for (i, col) in output_cols.iter().enumerate() {
            // Check if there's an alias in the projection for this position
            let alias = select.projection.get(i).and_then(|item| {
                if let SelectItem::ExprWithAlias { alias, .. } = item {
                    Some(alias.value.clone())
                } else {
                    None
                }
            });

            let (name, col_type) = match col {
                OutputCol::TableColumn(idx) => {
                    let tc = table_columns.get(*idx);
                    let n = tc.map(|c| c.name.clone()).unwrap_or_else(|| "?".to_string());
                    let ct = tc.map(|c| match c.col_type {
                        StorageColumnType::Int => ColumnType::Int,
                        StorageColumnType::Double => ColumnType::Double,
                        _ => ColumnType::String,
                    }).unwrap_or(ColumnType::String);
                    (n, ct)
                }
                OutputCol::Aggregate(agg) => aggregate_column_def(agg, table_columns),
                OutputCol::Literal(_) => {
                    ("?".to_string(), ColumnType::String)
                }
            };

            // Use alias if available, otherwise fall back to the default name
            let final_name = alias.unwrap_or(name);
            cols.push(ColumnDef {
                name: final_name,
                col_type,
            });
        }
        cols
    }

    fn execute_group_by(
        &self,
        select: &Select,
        rows: &[Vec<String>],
        table_columns: &[StorageColumnDef],
        output_cols: &[OutputCol],
    ) -> QueryResult {
        // Get group-by column indices
        let group_col_indices: Vec<usize> = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => exprs
                .iter()
                .filter_map(|e| {
                    if let Expr::Identifier(ident) = e {
                        table_columns.iter().position(|c| c.name == ident.value)
                    } else {
                        None
                    }
                })
                .collect(),
            _ => vec![],
        };

        // Group rows by key
        let mut groups: std::collections::BTreeMap<String, Vec<&Vec<String>>> =
            std::collections::BTreeMap::new();
        for row in rows {
            let key: String = group_col_indices
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or_default())
                .collect::<Vec<_>>()
                .join("|||");
            groups.entry(key).or_default().push(row);
        }

        // Build result rows
        let mut result_rows = vec![];
        for (_key, group_rows) in &groups {
            let mut row = vec![];
            for col in output_cols {
                match col {
                    OutputCol::TableColumn(idx) => {
                        let val = group_rows.first().and_then(|r| r.get(*idx).cloned());
                        row.push(val.map(Some).unwrap_or(None));
                    }
                    OutputCol::Aggregate(agg) => {
                        let idx = agg.col_index().unwrap_or(0);
                        // group_rows: &Vec<&Vec<String>>; deref to &&Vec<String>.
                        row.push(compute_aggregate(agg, group_rows.iter().copied(), idx));
                    }
                    OutputCol::Literal(v) => {
                        row.push(Some(v.clone()));
                    }
                }
            }
            result_rows.push(row);
        }

        let cols = self.build_result_column_defs(output_cols, table_columns, select);
        QueryResult::with_rows(cols, result_rows)
    }

    fn eval_where(&self, expr: &Expr, row: &[String], columns: &[StorageColumnDef]) -> bool {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                sqlparser::ast::BinaryOperator::And => {
                    self.eval_where(left, row, columns) && self.eval_where(right, row, columns)
                }
                sqlparser::ast::BinaryOperator::Or => {
                    self.eval_where(left, row, columns) || self.eval_where(right, row, columns)
                }
                sqlparser::ast::BinaryOperator::Eq => {
                    let lv = self.eval_expr_value(left, row, columns);
                    let rv = self.eval_expr_value(right, row, columns);
                    lv == rv
                }
                sqlparser::ast::BinaryOperator::NotEq => {
                    let lv = self.eval_expr_value(left, row, columns);
                    let rv = self.eval_expr_value(right, row, columns);
                    lv != rv
                }
                sqlparser::ast::BinaryOperator::Gt => {
                    let lv = self.eval_expr_value(left, row, columns);
                    let rv = self.eval_expr_value(right, row, columns);
                    compare_string_values(&lv, &rv) == Some(std::cmp::Ordering::Greater)
                }
                sqlparser::ast::BinaryOperator::GtEq => {
                    let lv = self.eval_expr_value(left, row, columns);
                    let rv = self.eval_expr_value(right, row, columns);
                    compare_string_values(&lv, &rv) != Some(std::cmp::Ordering::Less)
                }
                sqlparser::ast::BinaryOperator::Lt => {
                    let lv = self.eval_expr_value(left, row, columns);
                    let rv = self.eval_expr_value(right, row, columns);
                    compare_string_values(&lv, &rv) == Some(std::cmp::Ordering::Less)
                }
                sqlparser::ast::BinaryOperator::LtEq => {
                    let lv = self.eval_expr_value(left, row, columns);
                    let rv = self.eval_expr_value(right, row, columns);
                    compare_string_values(&lv, &rv) != Some(std::cmp::Ordering::Greater)
                }
                _ => false, // Unknown operator — reject row
            },
            // [NOT] IN (val1, val2, ...) — membership test.
            Expr::InList { expr, list, negated } => {
                let v = self.eval_expr_value(expr, row, columns);
                let matched = list
                    .iter()
                    .any(|e| self.eval_expr_value(e, row, columns) == v);
                matched != *negated
            }
            // <expr> [NOT] BETWEEN <low> AND <high>
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let v = self.eval_expr_value(expr, row, columns);
                let lo = self.eval_expr_value(low, row, columns);
                let hi = self.eval_expr_value(high, row, columns);
                // v >= lo AND v <= hi
                let ge_lo = matches!(
                    compare_string_values(&v, &lo),
                    Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
                );
                let le_hi = matches!(
                    compare_string_values(&v, &hi),
                    Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
                );
                let in_range = ge_lo && le_hi;
                in_range != *negated
            }
            // [NOT] LIKE <pattern> — MySQL default is case-insensitive.
            Expr::Like {
                negated,
                expr,
                pattern,
                ..
            } => {
                let v = self.eval_expr_value(expr, row, columns);
                let p = self.eval_expr_value(pattern, row, columns);
                let matched = like_match(&v, &p);
                matched != *negated
            }
            Expr::ILike {
                negated,
                expr,
                pattern,
                ..
            } => {
                let v = self.eval_expr_value(expr, row, columns);
                let p = self.eval_expr_value(pattern, row, columns);
                let matched = like_match(&v, &p);
                matched != *negated
            }
            _ => false,
        }
    }

    fn eval_expr_value(&self, expr: &Expr, row: &[String], columns: &[StorageColumnDef]) -> String {
        match expr {
            Expr::Identifier(ident) => {
                let col_name = &ident.value;
                let idx = columns.iter().position(|c| &c.name == col_name);
                idx.and_then(|i| row.get(i).cloned()).unwrap_or_default()
            }
            Expr::Value(Value::Number(n, _)) => n.clone(),
            Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
            Expr::Value(Value::DoubleQuotedString(s)) => s.clone(),
            _ => String::new(),
        }
    }

    fn apply_order_by(
        &self,
        order_by: &[OrderByExpr],
        mut rows: Vec<Vec<Option<String>>>,
        output_cols: &[OutputCol],
        table_columns: &[StorageColumnDef],
    ) -> Vec<Vec<Option<String>>> {
        // Resolve order-by column positions in the output
        let order_specs: Vec<(usize, bool)> = order_by
            .iter()
            .filter_map(|ob| {
                if let Expr::Identifier(ident) = &ob.expr {
                    let name = &ident.value;
                    let desc = !ob.asc.unwrap_or(true);
                    // Find position in output_cols
                    let pos = output_cols.iter().position(|c| match c {
                        OutputCol::TableColumn(idx) => {
                            table_columns.get(*idx).map(|tc| &tc.name) == Some(name)
                        }
                        _ => false,
                    });
                    pos.map(|p| (p, desc))
                } else {
                    None
                }
            })
            .collect();

        rows.sort_by(|a, b| {
            for &(col_idx, desc) in &order_specs {
                let va = a.get(col_idx).and_then(|v| v.as_deref()).unwrap_or("");
                let vb = b.get(col_idx).and_then(|v| v.as_deref()).unwrap_or("");
                let cmp = compare_string_values(va, vb).unwrap_or(std::cmp::Ordering::Equal);
                let cmp = if desc { cmp.reverse() } else { cmp };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        rows
    }

    fn execute_insert(&self, database: &str, insert: &Insert) -> QueryResult {
        let table_name = insert.table_name.to_string();

        if self.storage.get_database(database).is_none() {
            self.storage.create_database(database);
        }

        let db = match self.storage.get_database(database) {
            Some(db) => db,
            None => return QueryResult::ok(),
        };

        let table = match db.get_table(&table_name) {
            Some(t) => t,
            None => return QueryResult::ok(),
        };

        // INSERT ... VALUES ... is represented as INSERT ... SELECT * FROM (VALUES ...)
        // In sqlparser, insert.source is Option<Box<Query>>
        if let Some(ref source) = insert.source {
            if let SetExpr::Values(Values { rows, .. }) = source.body.as_ref() {
                for value_row in rows {
                    let values: Vec<String> = value_row
                        .iter()
                        .map(|expr| match expr {
                            Expr::Value(Value::Number(n, _)) => n.clone(),
                            Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
                            Expr::Value(Value::DoubleQuotedString(s)) => s.clone(),
                            Expr::Value(Value::Null) => String::new(),
                            Expr::Value(Value::Boolean(b)) => b.to_string(),
                            Expr::BinaryOp { left, op, right } => {
                                let l = match left.as_ref() {
                                    Expr::Value(Value::Number(n, _)) => n.parse::<f64>().unwrap_or(0.0),
                                    _ => 0.0,
                                };
                                let r = match right.as_ref() {
                                    Expr::Value(Value::Number(n, _)) => n.parse::<f64>().unwrap_or(0.0),
                                    _ => 0.0,
                                };
                                let result = match op {
                                    sqlparser::ast::BinaryOperator::Plus => l + r,
                                    sqlparser::ast::BinaryOperator::Minus => l - r,
                                    sqlparser::ast::BinaryOperator::Multiply => l * r,
                                    sqlparser::ast::BinaryOperator::Divide => if r != 0.0 { l / r } else { 0.0 },
                                    _ => 0.0,
                                };
                                if result == result.floor() && result.is_finite() && result.abs() <= i64::MAX as f64 {
                                    format!("{}", result as i64)
                                } else {
                                    format!("{}", result)
                                }
                            }
                            _ => String::new(),
                        })
                        .collect();
                    table.insert(values);
                }
            }
        }

        Self::ok_result()
    }

    fn execute_update(
        &self,
        database: &str,
        table: TableWithJoins,
        assignments: Vec<sqlparser::ast::Assignment>,
        selection: Option<Expr>,
    ) -> QueryResult {
        let table_name = match &table.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return QueryResult::ok(),
        };

        let db = match self.storage.get_database(database) {
            Some(db) => db,
            None => return QueryResult::ok(),
        };

        let tbl = match db.get_table(&table_name) {
            Some(t) => t,
            None => return QueryResult::ok(),
        };

        // Parse SET assignments: column_name = value
        let set_values: Vec<(usize, String)> = assignments
            .iter()
            .filter_map(|a| {
                let col_name = match &a.target {
                    AssignmentTarget::ColumnName(obj_name) => {
                        obj_name.0.first().map(|i| i.value.clone())
                    }
                    _ => None,
                }?;
                let col_idx = tbl.columns.iter().position(|c| c.name == col_name)?;
                let val = match &a.value {
                    Expr::Value(Value::Number(n, _)) => n.clone(),
                    Expr::Value(Value::SingleQuotedString(s)) => s.clone(),
                    _ => String::new(),
                };
                Some((col_idx, val))
            })
            .collect();

        let all_rows = tbl.select_all_ordered();

        for (row_id, row) in &all_rows {
            let matches = selection
                .as_ref()
                .map(|e| self.eval_where(e, row, &tbl.columns))
                .unwrap_or(true);

            if matches {
                for &(col_idx, ref val) in &set_values {
                    tbl.update_row(*row_id, col_idx, val);
                }
            }
        }

        Self::ok_result()
    }

    fn execute_delete(&self, database: &str, delete: &Delete) -> QueryResult {
        let table_name = match &delete.from {
            FromTable::WithFromKeyword(from) => match &from[0].relation {
                TableFactor::Table { name, .. } => name.to_string(),
                _ => return QueryResult::ok(),
            },
            FromTable::WithoutKeyword(from) => match &from[0].relation {
                TableFactor::Table { name, .. } => name.to_string(),
                _ => return QueryResult::ok(),
            },
        };

        let db = match self.storage.get_database(database) {
            Some(db) => db,
            None => return QueryResult::ok(),
        };

        let tbl = match db.get_table(&table_name) {
            Some(t) => t,
            None => return QueryResult::ok(),
        };

        let all_rows = tbl.select_all_ordered();

        for (row_id, row) in &all_rows {
            let matches = delete
                .selection
                .as_ref()
                .map(|e| self.eval_where(e, row, &tbl.columns))
                .unwrap_or(true);

            if matches {
                tbl.delete_row(*row_id);
            }
        }

        Self::ok_result()
    }
}

impl QueryHandler for AdbMysqlHandler {
    fn handle_query(&self, conn_id: u32, sql: &str) -> QueryResult {
        debug!("ADB query [conn={}]: {}", conn_id, sql);

        let stmts = match self.parse_sql(sql) {
            Ok(s) => s,
            Err(e) => {
                warn!("ADB SQL parse error: {} — SQL: {}", e, sql);
                return QueryResult::with_rows(
                    vec![ColumnDef { name: "error".to_string(), col_type: ColumnType::String }],
                    vec![vec![Some(format!("ERROR: {}", e))]],
                );
            }
        };

        // Pre-scan for USE statements to update connection's database
        for stmt in &stmts {
            if let Statement::Use(use_expr) = stmt {
                let db_name = match use_expr {
                    Use::Database(name) => name.to_string(),
                    Use::Schema(name) => name.to_string(),
                    Use::Object(name) => name.to_string(),
                    _ => "default".to_string(),
                };
                self.current_databases.insert(conn_id, db_name.clone());
                if self.storage.get_database(&db_name).is_none() {
                    self.storage.create_database(&db_name);
                }
            }
        }

        // Re-read database after USE pre-scan
        let database = self.get_database(conn_id);

        let mut result = QueryResult::ok();
        for stmt in stmts {
            result = self.execute_statement(conn_id, &database, stmt);
        }
        result
    }

    fn set_database(&self, conn_id: u32, db: &str) {
        self.current_databases.insert(conn_id, db.to_string());
        if self.storage.get_database(db).is_none() {
            self.storage.create_database(db);
        }
    }

    fn on_connect(&self, conn_id: u32, _user: &str, _host: &str) {
        self.current_databases.insert(conn_id, "default".to_string());
    }

    fn on_disconnect(&self, conn_id: u32) {
        self.current_databases.remove(&conn_id);
    }
}

/// Compare two string values, trying numeric first, then lexicographic
/// Resolve a parsed SQL aggregate function into an [`AggFunc`].
///
/// Returns `None` for unrecognised functions or malformed arguments (e.g. a
/// SUM with no column), so the caller can fall back to a literal column.
fn resolve_agg_func(
    f: &sqlparser::ast::Function,
    table_columns: &[StorageColumnDef],
) -> Option<AggFunc> {
    let name = f.name.to_string().to_uppercase();
    // COUNT(*) is special: it has a bare wildcard argument (or no args).
    let args = match &f.args {
        FunctionArguments::List(list) => &list.args,
        _ => return None,
    };
    if name == "COUNT" {
        if matches!(args.first(), Some(FunctionArg::Unnamed(FunctionArgExpr::Wildcard)))
            || matches!(
                args.first(),
                Some(FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)))
            ) {
            return Some(AggFunc::CountStar);
        }
        return single_col_index(args, table_columns).map(AggFunc::Count);
    }
    let idx = single_col_index(args, table_columns)?;
    match name.as_str() {
        "SUM" => Some(AggFunc::Sum(idx)),
        "AVG" => Some(AggFunc::Avg(idx)),
        "MIN" => Some(AggFunc::Min(idx)),
        "MAX" => Some(AggFunc::Max(idx)),
        _ => None,
    }
}

/// Extract the column index of the first positional `Expr(Identifier)` argument.
fn single_col_index(args: &[FunctionArg], table_columns: &[StorageColumnDef]) -> Option<usize> {
    let first = args.first()?;
    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) = first {
        return table_columns.iter().position(|c| c.name == ident.value);
    }
    None
}

/// Produce the (name, type) column definition for an aggregate output column.
fn aggregate_column_def(agg: &AggFunc, table_columns: &[StorageColumnDef]) -> (String, ColumnType) {
    let col_name = |idx: usize| {
        table_columns
            .get(idx)
            .map(|c| c.name.as_str())
            .unwrap_or("?")
    };
    match agg {
        AggFunc::CountStar => ("COUNT(*)".to_string(), ColumnType::Int),
        AggFunc::Count(i) => (format!("COUNT({})", col_name(*i)), ColumnType::Int),
        AggFunc::Sum(i) => (format!("SUM({})", col_name(*i)), ColumnType::Double),
        AggFunc::Avg(i) => (format!("AVG({})", col_name(*i)), ColumnType::Double),
        // MIN/MAX preserve the underlying column's domain.
        AggFunc::Min(i) | AggFunc::Max(i) => {
            let fname = if matches!(agg, AggFunc::Min(_)) { "MIN" } else { "MAX" };
            let ct = table_columns.get(*i).map(|c| match c.col_type {
                StorageColumnType::Int => ColumnType::Int,
                StorageColumnType::Double => ColumnType::Double,
                _ => ColumnType::String,
            }).unwrap_or(ColumnType::String);
            (format!("{}({})", fname, col_name(*i)), ct)
        }
    }
}

/// Compute an aggregate over an iterator of row references.
///
/// `rows` yields borrowed rows; `col_idx` selects the cell within each row.
/// Non-numeric cells (for Sum/Avg) are skipped. Empty inputs yield `None`
/// except for COUNT, which yields 0.
fn compute_aggregate<'a, I>(agg: &AggFunc, rows: I, col_idx: usize) -> Option<String>
where
    I: IntoIterator<Item = &'a Vec<String>>,
{
    match agg {
        AggFunc::CountStar => Some(rows.into_iter().count().to_string()),
        AggFunc::Count(_) => {
            // Count non-NULL cells. NULL is represented as an empty string here
            // (storage stringifies values; NULL becomes "").
            let n = rows
                .into_iter()
                .filter(|r| r.get(col_idx).map(|s| !s.is_empty()).unwrap_or(false))
                .count();
            Some(n.to_string())
        }
        AggFunc::Sum(_) | AggFunc::Avg(_) => {
            let nums: Vec<f64> = rows
                .into_iter()
                .filter_map(|r| r.get(col_idx)?.parse::<f64>().ok())
                .collect();
            if nums.is_empty() {
                return None;
            }
            match agg {
                AggFunc::Sum(_) => Some(format_f64(nums.iter().sum())),
                AggFunc::Avg(_) => Some(format_f64(nums.iter().sum::<f64>() / nums.len() as f64)),
                _ => unreachable!(),
            }
        }
        AggFunc::Min(_) | AggFunc::Max(_) => {
            let mut best: Option<&String> = None;
            for r in rows {
                let Some(val) = r.get(col_idx) else { continue };
                // Skip SQL NULL (empty string) for MIN/MAX.
                if val.is_empty() {
                    continue;
                }
                best = Some(match best {
                    None => val,
                    Some(cur) => {
                        let ord = compare_string_values(cur, val)
                            .unwrap_or(std::cmp::Ordering::Equal);
                        match agg {
                            AggFunc::Min(_) => {
                                if ord == std::cmp::Ordering::Greater {
                                    val
                                } else {
                                    cur
                                }
                            }
                            AggFunc::Max(_) => {
                                if ord == std::cmp::Ordering::Less {
                                    val
                                } else {
                                    cur
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                });
            }
            best.map(|s| s.clone())
        }
    }
}

/// Match a value against an SQL LIKE pattern (MySQL semantics).
///
/// `%` matches any sequence of characters (including empty), `_` matches any
/// single character, and any other character matches itself. Matching is
/// case-insensitive (MySQL's default collation). There is no escape handling
/// for backslashes here because the parser already resolved literal pattern
/// strings; a literal `%`/`_` in the pattern is always a wildcard.
fn like_match(value: &str, pattern: &str) -> bool {
    let v: Vec<char> = value.chars().flat_map(|c| c.to_lowercase()).collect();
    let p: Vec<char> = pattern.chars().flat_map(|c| c.to_lowercase()).collect();
    like_match_inner(&v, 0, &p, 0)
}

/// Recursive LIKE matcher. Tries to consume the value against the pattern;
/// `%` greedily tries every split point so it matches the longest prefix that
/// lets the rest succeed. Complexity is O(|v|*|p|) worst case, acceptable for
/// typical WHERE clauses.
fn like_match_inner(v: &[char], vi: usize, p: &[char], pi: usize) -> bool {
    if pi == p.len() {
        return vi == v.len();
    }
    match p[pi] {
        // '%' matches zero or more characters.
        '%' => {
            // Skip consecutive % (they are equivalent to a single %).
            let mut next_pi = pi;
            while next_pi < p.len() && p[next_pi] == '%' {
                next_pi += 1;
            }
            // Try every possible consumption length for the run of '%'.
            for skip in vi..=v.len() {
                if like_match_inner(v, skip, p, next_pi) {
                    return true;
                }
            }
            false
        }
        // '_' matches exactly one character.
        '_' => {
            if vi < v.len() {
                like_match_inner(v, vi + 1, p, pi + 1)
            } else {
                false
            }
        }
        c => {
            if vi < v.len() && v[vi] == c {
                like_match_inner(v, vi + 1, p, pi + 1)
            } else {
                false
            }
        }
    }
}

fn compare_string_values(a: &str, b: &str) -> Option<std::cmp::Ordering> {
    if let (Ok(na), Ok(nb)) = (a.parse::<f64>(), b.parse::<f64>()) {
        // Handle NaN explicitly since partial_cmp returns None for NaN
        return if na.is_nan() && nb.is_nan() {
            Some(std::cmp::Ordering::Equal)
        } else if na.is_nan() {
            Some(std::cmp::Ordering::Less)
        } else if nb.is_nan() {
            Some(std::cmp::Ordering::Greater)
        } else {
            na.partial_cmp(&nb)
        };
    }
    Some(a.cmp(b))
}

/// Format f64 without trailing zeros (e.g., 2.0 → "2", 2.5 → "2.5")
fn format_f64(v: f64) -> String {
    if v.is_finite() && v == v.floor() && v.abs() <= i64::MAX as f64 {
        format!("{}", v as i64)
    } else {
        format!("{}", v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows() -> Vec<Vec<String>> {
        vec![
            vec!["10".to_string(), "a".to_string()],
            vec!["20".to_string(), "".to_string()],
            vec!["5".to_string(), "b".to_string()],
        ]
}

    #[test]
    fn test_aggregate_count_star() {
        let r = rows();
        assert_eq!(compute_aggregate(&AggFunc::CountStar, r.iter(), 0), Some("3".to_string()));
    }

    #[test]
    fn test_aggregate_count_col_skips_nulls() {
        // Column 1 has one empty (NULL) cell -> COUNT(col1) = 2.
        let r = rows();
        assert_eq!(compute_aggregate(&AggFunc::Count(1), r.iter(), 1), Some("2".to_string()));
    }

    #[test]
    fn test_aggregate_sum() {
        let r = rows();
        assert_eq!(compute_aggregate(&AggFunc::Sum(0), r.iter(), 0), Some("35".to_string()));
    }

    #[test]
    fn test_aggregate_avg() {
        let r = rows();
        assert_eq!(compute_aggregate(&AggFunc::Avg(0), r.iter(), 0), Some(format_f64(35.0 / 3.0)));
    }

    #[test]
    fn test_aggregate_min_numeric() {
        let r = rows();
        assert_eq!(compute_aggregate(&AggFunc::Min(0), r.iter(), 0), Some("5".to_string()));
    }

    #[test]
    fn test_aggregate_max_numeric() {
        let r = rows();
        assert_eq!(compute_aggregate(&AggFunc::Max(0), r.iter(), 0), Some("20".to_string()));
    }

    #[test]
    fn test_aggregate_min_string() {
        // Column 1 non-null values: "a", "b" -> min is "a".
        let r = rows();
        assert_eq!(compute_aggregate(&AggFunc::Min(1), r.iter(), 1), Some("a".to_string()));
    }

    #[test]
    fn test_aggregate_sum_empty_returns_none() {
        let r: Vec<Vec<String>> = vec![];
        assert_eq!(compute_aggregate(&AggFunc::Sum(0), r.iter(), 0), None);
    }

    #[test]
    fn test_aggregate_count_star_empty_is_zero() {
        let r: Vec<Vec<String>> = vec![];
        assert_eq!(
            compute_aggregate(&AggFunc::CountStar, std::iter::empty(), 0),
            Some("0".to_string())
        );
    }

    #[test]
    fn test_aggregate_min_skips_null() {
        // NULL (empty) cells must not win MIN.
        let r = vec![vec!["".to_string()], vec!["7".to_string()]];
        assert_eq!(compute_aggregate(&AggFunc::Min(0), r.iter(), 0), Some("7".to_string()));
    }

    #[test]
    fn test_aggregate_column_def_naming() {
        let cols = vec![
            StorageColumnDef { name: "amount".to_string(), col_type: StorageColumnType::Double },
            StorageColumnDef { name: "label".to_string(), col_type: StorageColumnType::VarChar },
        ];
        assert_eq!(
            aggregate_column_def(&AggFunc::Sum(0), &cols),
            ("SUM(amount)".to_string(), ColumnType::Double)
        );
        assert_eq!(
            aggregate_column_def(&AggFunc::Avg(0), &cols),
            ("AVG(amount)".to_string(), ColumnType::Double)
        );
        assert_eq!(
            aggregate_column_def(&AggFunc::Min(1), &cols),
            ("MIN(label)".to_string(), ColumnType::String)
        );
        assert_eq!(
            aggregate_column_def(&AggFunc::CountStar, &cols),
            ("COUNT(*)".to_string(), ColumnType::Int)
        );
    }

    #[test]
    fn test_like_match_basic() {
        assert!(like_match("hello", "hello"));
        assert!(like_match("hello", "h_llo"));
        assert!(like_match("hello", "h%o"));
        assert!(like_match("hello", "%"));
        assert!(like_match("hello", "he%"));
        assert!(like_match("hello", "%lo"));
        assert!(like_match("", "%"));
        assert!(like_match("", ""));
    }

    #[test]
    fn test_like_match_case_insensitive() {
        // MySQL default collation: LIKE is case-insensitive.
        assert!(like_match("Hello", "hello"));
        assert!(like_match("HELLO", "h_llo"));
        assert!(like_match("Hello", "H%O"));
    }

    #[test]
    fn test_like_match_no_match() {
        assert!(!like_match("hello", "world"));
        assert!(!like_match("hello", "h_o")); // h_o needs exactly 1 char between
        assert!(!like_match("hello", "h%x"));  // must end with x
        assert!(!like_match("", "h"));
    }

    #[test]
    fn test_like_match_underscore() {
        assert!(like_match("abc", "a_c"));
        assert!(!like_match("ac", "a_c")); // underscore needs exactly one char
        assert!(!like_match("abbc", "a_c"));
    }

    #[test]
    fn test_like_match_multiple_percent() {
        // Consecutive % collapse to one.
        assert!(like_match("hello", "h%%o"));
        assert!(like_match("hello", "%%%"));
    }
}
