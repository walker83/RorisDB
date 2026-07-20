use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow_arith::boolean;
use arrow_array::BooleanArray;
use arrow_array::Float64Array;
use arrow_array::Int32Array;
use arrow_array::Int64Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_ord::cmp;
use arrow_schema::Schema as ArrowSchema;
use arrow_select::filter::filter_record_batch;
use async_trait::async_trait;
use tracing::warn;
use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::expr::BinaryExpr;
use datafusion::logical_expr::{Expr, Operator, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion_datasource::memory::MemorySourceConfig;

use crate::ParquetStorage;

/// DataFusion TableProvider backed by a Parquet file.
///
/// On each `scan()`, reads the Parquet file with projection/limit pushdown,
/// applies simple filter pushdown, and wraps the result in `MemorySourceConfig`.
pub struct ParquetTableProvider {
    schema: Arc<ArrowSchema>,
    storage: Arc<ParquetStorage>,
    db_name: String,
    table_name: String,
}

impl fmt::Debug for ParquetTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetTableProvider")
            .field("db", &self.db_name)
            .field("table", &self.table_name)
            .finish()
    }
}

impl ParquetTableProvider {
    pub fn new(
        storage: Arc<ParquetStorage>,
        db_name: String,
        table_name: String,
        schema: Arc<ArrowSchema>,
    ) -> Self {
        Self {
            schema,
            storage,
            db_name,
            table_name,
        }
    }
}

#[async_trait]
impl TableProvider for ParquetTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::prelude::Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // 1. Read with projection + limit pushdown
        let rb = self
            .storage
            .read_with_options(&self.db_name, &self.table_name, projection, limit)
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to read {}.{}: {}",
                    self.db_name, self.table_name, e
                ))
            })?;

        // 2. Apply filter pushdown (best-effort, only simple col op literal)
        let filtered_rb = if !filters.is_empty() && rb.num_rows() > 0 {
            match apply_filters(&rb, filters) {
                Some(filtered) => filtered,
                None => {
                    warn!(
                        "Filter pushdown failed for {}.{}, falling back to unfiltered scan. Filters: {:?}",
                        self.db_name, self.table_name, filters
                    );
                    rb
                }
            }
        } else {
            rb
        };

        // 3. Compute the projected schema matching the columns actually read
        let projected_schema = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => self.schema.clone(),
        };

        // 4. Return DataSourceExec backed by MemorySourceConfig
        //    (projection already baked into the data, so pass None for projection)
        let exec = MemorySourceConfig::try_new_exec(&[vec![filtered_rb]], projected_schema, None)?;
        Ok(exec)
    }
}

/// Attempt to create a constant Arrow array from a ScalarValue.
///
/// Returns `None` for types that are not (yet) supported in the filter
/// pushdown path.
fn scalar_to_array(value: &ScalarValue, size: usize) -> Option<arrow_array::ArrayRef> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Boolean(Some(v)) => Some(Arc::new(BooleanArray::from(vec![*v; size]))),
        ScalarValue::Int8(Some(v)) => Some(Arc::new(
            std::iter::repeat(*v)
                .take(size)
                .collect::<arrow_array::Int8Array>(),
        )),
        ScalarValue::Int16(Some(v)) => Some(Arc::new(
            std::iter::repeat(*v)
                .take(size)
                .collect::<arrow_array::Int16Array>(),
        )),
        ScalarValue::Int32(Some(v)) => Some(Arc::new(Int32Array::from(vec![*v; size]))),
        ScalarValue::Int64(Some(v)) => Some(Arc::new(Int64Array::from(vec![*v; size]))),
        ScalarValue::UInt8(Some(v)) => Some(Arc::new(
            std::iter::repeat(*v)
                .take(size)
                .collect::<arrow_array::UInt8Array>(),
        )),
        ScalarValue::UInt16(Some(v)) => Some(Arc::new(
            std::iter::repeat(*v)
                .take(size)
                .collect::<arrow_array::UInt16Array>(),
        )),
        ScalarValue::UInt32(Some(v)) => Some(Arc::new(
            std::iter::repeat(*v)
                .take(size)
                .collect::<arrow_array::UInt32Array>(),
        )),
        ScalarValue::UInt64(Some(v)) => Some(Arc::new(
            std::iter::repeat(*v)
                .take(size)
                .collect::<arrow_array::UInt64Array>(),
        )),
        ScalarValue::Float32(Some(v)) => Some(Arc::new(
            std::iter::repeat(*v)
                .take(size)
                .collect::<arrow_array::Float32Array>(),
        )),
        ScalarValue::Float64(Some(v)) => Some(Arc::new(Float64Array::from(vec![*v; size]))),
        ScalarValue::Utf8(Some(v)) => Some(Arc::new(StringArray::from(vec![v.as_str(); size]))),
        ScalarValue::LargeUtf8(Some(v)) => {
            let arr: arrow_array::LargeStringArray = vec![v.as_str(); size].into();
            Some(Arc::new(arr))
        }
        _ => None,
    }
}

/// Try to extract a simple `Column op Literal` filter from an expression.
fn extract_simple_filter(expr: &Expr) -> Option<(String, Operator, ScalarValue)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            // column op literal
            if let Expr::Column(col) = left.as_ref() {
                if let Expr::Literal(val, _) = right.as_ref() {
                    return Some((col.name.clone(), *op, val.clone()));
                }
            }
            // literal op column (reverse — swap op semantics)
            if let Expr::Literal(val, _) = left.as_ref() {
                if let Expr::Column(col) = right.as_ref() {
                    let reversed_op = match op {
                        Operator::Gt => Operator::Lt,
                        Operator::GtEq => Operator::LtEq,
                        Operator::Lt => Operator::Gt,
                        Operator::LtEq => Operator::GtEq,
                        other => *other, // Eq/NotEq are symmetric
                    };
                    return Some((col.name.clone(), reversed_op, val.clone()));
                }
            }
            None
        }
        _ => None,
    }
}

/// Recursively decompose AND expressions into simple `col op literal` filters.
/// Returns false if any sub-expression cannot be decomposed.
fn extract_filters(expr: &Expr, out: &mut Vec<(String, Operator, ScalarValue)>) -> bool {
    if let Some(filter) = extract_simple_filter(expr) {
        out.push(filter);
        return true;
    }
    // Try to decompose AND expressions
    if let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::And,
        right,
    }) = expr
    {
        return extract_filters(left, out) && extract_filters(right, out);
    }
    false
}

/// Best-effort filter pushdown for simple `col op literal` comparisons.
///
/// Supports AND-composed filters by recursively decomposing `a = 1 AND b = 2`
/// into individual column predicates. Returns `Some(filtered_batch)` on success.
/// Returns `None` if any filter cannot be evaluated (caller falls back to unfiltered data).
fn apply_filters(batch: &RecordBatch, filters: &[Expr]) -> Option<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut simple_filters = Vec::new();

    for expr in filters {
        if !extract_filters(expr, &mut simple_filters) {
            return None;
        }
    }

    let mut combined_mask: Option<BooleanArray> = None;

    for (col_name, op, scalar) in simple_filters {
        // Find the column in the (potentially projected) batch
        let col_idx = batch.schema().index_of(&col_name).ok()?;
        let array = batch.column(col_idx);

        // Create a constant array for the scalar value
        let const_array = scalar_to_array(&scalar, num_rows)?;

        // Apply the comparison kernel
        // Note: we create &dyn Array references which implement Datum, then pass
        // references to them as &dyn Datum
        let col_arr: &dyn arrow_array::Array = array.as_ref();
        let const_arr: &dyn arrow_array::Array = const_array.as_ref();
        let mask: BooleanArray = match op {
            Operator::Eq => cmp::eq(&col_arr, &const_arr).ok()?,
            Operator::NotEq => cmp::neq(&col_arr, &const_arr).ok()?,
            Operator::Lt => cmp::lt(&col_arr, &const_arr).ok()?,
            Operator::LtEq => cmp::lt_eq(&col_arr, &const_arr).ok()?,
            Operator::Gt => cmp::gt(&col_arr, &const_arr).ok()?,
            Operator::GtEq => cmp::gt_eq(&col_arr, &const_arr).ok()?,
            _ => return None,
        };

        combined_mask = Some(match combined_mask {
            Some(prev) => boolean::and(&prev, &mask).ok()?,
            None => mask,
        });
    }

    let mask = combined_mask?;
    filter_record_batch(batch, &mask).ok()
}
