//! Doris-compatible UDF implementations.
//!
//! Provides Doris-specific functions:
//! - Time functions: date_trunc, months_add, days_add, hours_add
//! - String functions: concat_ws, substring_index
//! - Aggregate functions: bitmap_count

use arrow_array::{Array, ListArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field};
use chrono::{Datelike, NaiveDate};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::StateFieldsArgs;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    AggregateUDF, ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Time Functions
// ---------------------------------------------------------------------------

/// date_trunc - truncate date to specified precision
/// Usage: date_trunc('month', date_col) -> truncated date
pub fn create_date_trunc_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct DateTruncUdf {
        signature: Signature,
    }

    impl DateTruncUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Utf8, DataType::Date32],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for DateTruncUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "date_trunc"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Date32)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            use arrow_array::Date32Array;

            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let precision = args[0]
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "date_trunc: precision must be StringArray".to_string(),
                    )
                })?;
            let dates = args[1]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("date_trunc: dates must be Date32Array".to_string())
                })?;

            let result: Vec<Option<i32>> = dates
                .iter()
                .zip(precision.iter())
                .map(|(d, p)| match (d, p) {
                    (Some(date), Some(prec)) => truncate_date(date, prec),
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(Date32Array::from(result)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(DateTruncUdf::new())
}

/// Truncate a date to the specified precision using correct calendar arithmetic.
fn truncate_date(days: i32, precision: &str) -> Option<i32> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let date = epoch.checked_add_signed(chrono::TimeDelta::days(days as i64))?;

    let truncated = match precision.to_lowercase().as_str() {
        "year" => NaiveDate::from_ymd_opt(date.year(), 1, 1)?,
        "month" => NaiveDate::from_ymd_opt(date.year(), date.month(), 1)?,
        "day" | "hour" | "minute" | "second" => date,
        _ => date,
    };

    let since_epoch = truncated.signed_duration_since(epoch);
    Some(since_epoch.num_days() as i32)
}

/// days_add - add days to a date
pub fn create_days_add_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct DaysAddUdf {
        signature: Signature,
    }

    impl DaysAddUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Date32, DataType::Int64],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for DaysAddUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "days_add"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Date32)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            use arrow_array::{Date32Array, Int64Array};

            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let dates = args[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("days_add: dates must be Date32Array".to_string())
                })?;
            let days_to_add = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("days_add: days must be Int64Array".to_string())
                })?;

            let result: Vec<Option<i32>> = dates
                .iter()
                .zip(days_to_add.iter())
                .map(|(d, n)| match (d, n) {
                    (Some(date), Some(n)) => {
                        // Clamp to i32 range to avoid truncation of large i64 values
                        let clamped = n.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                        Some(date + clamped)
                    }
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(Date32Array::from(result)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(DaysAddUdf::new())
}

/// months_add - add months to a date
pub fn create_months_add_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct MonthsAddUdf {
        signature: Signature,
    }

    impl MonthsAddUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Date32, DataType::Int64],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for MonthsAddUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "months_add"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Date32)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            use arrow_array::{Date32Array, Int64Array};

            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let dates = args[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("months_add: dates must be Date32Array".to_string())
                })?;
            let months = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("months_add: months must be Int64Array".to_string())
                })?;

            let result: Vec<Option<i32>> = dates
                .iter()
                .zip(months.iter())
                .map(|(d, m)| match (d, m) {
                    (Some(date), Some(m)) => add_months(date, m),
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(Date32Array::from(result)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(MonthsAddUdf::new())
}

/// Add months to a date using correct calendar arithmetic.
/// Clamps the day to the last valid day of the target month (e.g. Jan 31 + 1 month = Feb 28/29).
/// Uses Euclidean division to correctly handle negative month deltas (months must stay 1-12).
fn add_months(days_since_epoch: i32, months: i64) -> Option<i32> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let date = epoch.checked_add_signed(chrono::TimeDelta::days(days_since_epoch as i64))?;
    let total_months = date.year() as i64 * 12 + (date.month() as i64 - 1) + months;
    // Euclidean division ensures new_month is always 0-11 and new_year adjusts correctly
    // even when total_months is negative (prevents month=0).
    let new_year = total_months.div_euclid(12) as i32;
    let new_month = (total_months.rem_euclid(12) + 1) as u32;
    let new_day = date.day().min(max_day_of_month(new_year, new_month));

    let result = NaiveDate::from_ymd_opt(new_year, new_month, new_day)?;
    let since_epoch = result.signed_duration_since(epoch);
    Some(since_epoch.num_days() as i32)
}

/// Return the number of days in a given month.
fn max_day_of_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// hours_add - add hours to a datetime
pub fn create_hours_add_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct HoursAddUdf {
        signature: Signature,
    }

    impl HoursAddUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![
                        DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
                        DataType::Int64,
                    ],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for HoursAddUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "hours_add"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Timestamp(arrow_schema::TimeUnit::Second, None))
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            use arrow_array::{Int64Array, TimestampSecondArray};

            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let timestamps = args[0]
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "hours_add: timestamps must be TimestampSecondArray".to_string(),
                    )
                })?;
            let hours = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("hours_add: hours must be Int64Array".to_string())
                })?;

            let result: Vec<Option<i64>> = timestamps
                .iter()
                .zip(hours.iter())
                .map(|(ts, h)| match (ts, h) {
                    (Some(ts), Some(h)) => Some(ts + h * 3600),
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(TimestampSecondArray::from(result)) as Arc<dyn arrow_array::Array>,
            ))
        }
    }

    ScalarUDF::new_from_impl(HoursAddUdf::new())
}

// ---------------------------------------------------------------------------
// String Functions
// ---------------------------------------------------------------------------

/// concat_ws - concatenate strings with separator
/// Accepts variable arguments: separator, str1, str2, ..., strN
pub fn create_concat_ws_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct ConcatWsUdf {
        signature: Signature,
    }

    impl ConcatWsUdf {
        fn new() -> Self {
            Self {
                signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for ConcatWsUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "concat_ws"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            use arrow_array::StringArray;

            let args = ColumnarValue::values_to_arrays(&args.args)?;

            if args.len() < 2 {
                return Err(DataFusionError::Execution(
                    "concat_ws requires at least 2 arguments (separator, str1)".to_string(),
                ));
            }

            let sep_arr = args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "concat_ws: separator must be StringArray".to_string(),
                    )
                })?;
            let n = sep_arr.len();

            let result: datafusion::error::Result<Vec<Option<String>>> = (0..n)
                .map(|i| {
                    if sep_arr.is_null(i) {
                        return Ok(None);
                    }
                    let sep = sep_arr.value(i);
                    let mut parts = Vec::new();
                    for j in 1..args.len() {
                        let str_arr =
                            args[j]
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .ok_or_else(|| {
                                    DataFusionError::Internal(
                                        "concat_ws: string arguments must be StringArray"
                                            .to_string(),
                                    )
                                })?;
                        if !str_arr.is_null(i) {
                            parts.push(str_arr.value(i));
                        }
                    }
                    Ok(Some(parts.join(sep)))
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(StringArray::from(result?)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(ConcatWsUdf::new())
}

/// substring_index - substring before/after delimiter
pub fn create_substring_index_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct SubstringIndexUdf {
        signature: Signature,
    }

    impl SubstringIndexUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for SubstringIndexUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "substring_index"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            use arrow_array::{Int64Array, StringArray};

            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let str_arr = args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "substring_index: str must be StringArray".to_string(),
                    )
                })?;
            let delim_arr = args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "substring_index: delim must be StringArray".to_string(),
                    )
                })?;
            let count_arr = args[2]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "substring_index: count must be Int64Array".to_string(),
                    )
                })?;

            let result: Vec<Option<String>> = str_arr
                .iter()
                .zip(delim_arr.iter())
                .zip(count_arr.iter())
                .map(|((s, d), c)| match (s, d, c) {
                    (Some(str), Some(delim), Some(count)) => {
                        if count == 0 {
                            Some(String::new())
                        } else if count > 0 {
                            let parts: Vec<&str> = str.split(delim).collect();
                            if parts.len() >= count as usize {
                                Some(parts[..count as usize].join(delim))
                            } else {
                                Some(str.to_string())
                            }
                        } else {
                            let parts: Vec<&str> = str.split(delim).collect();
                            let abs_count = (-count) as usize;
                            if parts.len() >= abs_count {
                                Some(parts[parts.len() - abs_count..].join(delim))
                            } else {
                                Some(str.to_string())
                            }
                        }
                    }
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(StringArray::from(result)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(SubstringIndexUdf::new())
}

// ---------------------------------------------------------------------------
// String Functions (additional)
// ---------------------------------------------------------------------------

/// substring - extract substring from string (MySQL-compatible)
/// Usage: substring(str, pos) or substring(str, pos, len)
/// pos is 1-based. Negative pos counts from end of string.
pub fn create_substring_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct SubstringUdf {
        signature: Signature,
    }

    impl SubstringUdf {
        fn new() -> Self {
            Self {
                signature: Signature::one_of(
                    vec![
                        datafusion::logical_expr::TypeSignature::Exact(vec![
                            DataType::Utf8,
                            DataType::Int64,
                        ]),
                        datafusion::logical_expr::TypeSignature::Exact(vec![
                            DataType::Utf8,
                            DataType::Int64,
                            DataType::Int64,
                        ]),
                    ],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for SubstringUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "substring"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            use arrow_array::{Int64Array, StringArray};

            let raw_args = ColumnarValue::values_to_arrays(&args.args)?;
            let str_arr = raw_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("substring: arg0 must be StringArray".to_string())
                })?;
            let pos_arr = raw_args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("substring: arg1 must be Int64Array".to_string())
                })?;
            let len_arr = if raw_args.len() > 2 {
                Some(
                    raw_args[2]
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "substring: arg2 must be Int64Array".to_string(),
                            )
                        })?,
                )
            } else {
                None
            };

            let result: Vec<Option<String>> = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || pos_arr.is_null(i) {
                        return None;
                    }
                    let s = str_arr.value(i);
                    let pos = pos_arr.value(i);
                    let chars: Vec<char> = s.chars().collect();
                    let slen = chars.len() as i64;

                    // MySQL: pos is 1-based, negative counts from end
                    let start = if pos > 0 {
                        (pos - 1).max(0) as usize
                    } else if pos < 0 {
                        (slen + pos).max(0) as usize
                    } else {
                        0 // pos=0 returns empty in MySQL
                    };

                    if start >= chars.len() {
                        return Some(String::new());
                    }

                    let end = if let Some(la) = len_arr {
                        if la.is_null(i) {
                            return None;
                        }
                        let len = la.value(i);
                        if len < 0 {
                            return Some(String::new());
                        }
                        (start + len as usize).min(chars.len())
                    } else {
                        chars.len()
                    };

                    if start >= end {
                        Some(String::new())
                    } else {
                        Some(chars[start..end].iter().collect())
                    }
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(StringArray::from(result)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(SubstringUdf::new())
}

// ---------------------------------------------------------------------------
// Aggregate Functions (Doris-specific)
// ---------------------------------------------------------------------------

/// bitmap_count - count distinct values using bitmap
pub fn create_bitmap_count_udf() -> AggregateUDF {
    use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, function::AccumulatorArgs};
    use std::collections::HashSet;

    const MAX_BITMAP_DISTINCT: usize = 1_000_000;

    #[derive(Debug)]
    struct BitmapCountAccumulator {
        values: HashSet<ScalarValue>,
        data_type: Option<DataType>,
        max_distinct: usize,
    }

    impl BitmapCountAccumulator {
        fn new() -> Self {
            Self {
                values: HashSet::new(),
                data_type: None,
                max_distinct: MAX_BITMAP_DISTINCT,
            }
        }
    }

    impl Accumulator for BitmapCountAccumulator {
        fn update_batch(
            &mut self,
            values: &[Arc<dyn arrow_array::Array>],
        ) -> datafusion::error::Result<()> {
            if values.is_empty() {
                return Ok(());
            }
            let arr = &values[0];
            self.data_type = Some(arr.data_type().clone());
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let scalar = ScalarValue::try_from_array(arr, i)?;
                    self.values.insert(scalar);
                    if self.values.len() > self.max_distinct {
                        return Err(DataFusionError::Execution(format!(
                            "bitmap_count: distinct count {} exceeds limit of {}. \
                             Consider using a more selective query.",
                            self.values.len(),
                            self.max_distinct
                        )));
                    }
                }
            }
            Ok(())
        }

        fn merge_batch(
            &mut self,
            states: &[Arc<dyn arrow_array::Array>],
        ) -> datafusion::error::Result<()> {
            if states.is_empty() {
                return Ok(());
            }
            // The state is stored as a ListArray where each element is a
            // list of unique values from one partition.
            let list_arr = states[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "bitmap_count merge: expected ListArray state".to_string(),
                    )
                })?;

            for i in 0..list_arr.len() {
                if list_arr.is_null(i) {
                    continue;
                }
                let inner = list_arr.value(i);
                for j in 0..inner.len() {
                    if inner.is_null(j) {
                        continue;
                    }
                    let val = ScalarValue::try_from_array(inner.as_ref(), j)?;
                    self.values.insert(val);
                    if self.values.len() > self.max_distinct {
                        return Err(DataFusionError::Execution(format!(
                            "bitmap_count: distinct count {} exceeds limit of {}.",
                            self.values.len(),
                            self.max_distinct
                        )));
                    }
                }
            }
            Ok(())
        }

        fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
            let values: Vec<ScalarValue> = self.values.iter().cloned().collect();
            let elem_type = self.data_type.clone().unwrap_or(DataType::Null);
            let field: Arc<Field> = Arc::new(Field::new("item", elem_type, true));

            if values.is_empty() {
                let list_array = ListArray::new_null(field, 1);
                return Ok(vec![ScalarValue::List(Arc::new(list_array))]);
            }

            let values_array = ScalarValue::iter_to_array(values.into_iter())?;
            let offsets = OffsetBuffer::from_lengths([values_array.len()]);
            let list_array = ListArray::new(field, offsets, values_array, None);
            Ok(vec![ScalarValue::List(Arc::new(list_array))])
        }

        fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
            Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
        }

        fn size(&self) -> usize {
            // Each ScalarValue ~128 bytes average (enum + heap allocation for strings)
            // Plus HashSet bucket overhead (~48 bytes per entry)
            self.values.len() * 176
        }
    }

    #[derive(Debug)]
    struct BitmapCountUDFImpl {
        signature: Signature,
    }

    impl BitmapCountUDFImpl {
        fn new() -> Self {
            Self {
                signature: Signature::any(1, Volatility::Immutable),
            }
        }
    }

    impl AggregateUDFImpl for BitmapCountUDFImpl {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "bitmap_count"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn accumulator(
            &self,
            _acc_args: AccumulatorArgs,
        ) -> datafusion::error::Result<Box<dyn Accumulator>> {
            Ok(Box::new(BitmapCountAccumulator::new()))
        }

        /// Tell DataFusion that the intermediate state is a list of values matching
        /// the input type, rather than a flat Int64.
        fn state_fields(
            &self,
            args: StateFieldsArgs,
        ) -> datafusion::error::Result<Vec<Arc<Field>>> {
            let input_type = args
                .input_fields
                .first()
                .map(|f| f.data_type().clone())
                .unwrap_or(DataType::Null);
            Ok(vec![Arc::new(Field::new(
                format_state_name(args.name, "value"),
                DataType::List(Arc::new(Field::new("item", input_type, true))),
                true,
            ))])
        }
    }

    AggregateUDF::from(BitmapCountUDFImpl::new())
}

// ---------------------------------------------------------------------------
// UDF Registration
// ---------------------------------------------------------------------------

/// Register all Doris-compatible UDFs with a DataFusion context.
pub fn register_doris_udfs(ctx: &mut datafusion::prelude::SessionContext) {
    // Time functions
    ctx.register_udf(create_date_trunc_udf());
    ctx.register_udf(create_days_add_udf());
    ctx.register_udf(create_months_add_udf());
    ctx.register_udf(create_hours_add_udf());

    // String functions
    ctx.register_udf(create_concat_ws_udf());
    ctx.register_udf(create_substring_index_udf());
    ctx.register_udf(create_substring_udf());

    // Aggregate functions
    ctx.register_udaf(create_bitmap_count_udf());
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_substring_index_count_zero() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_substring_index_udf());

        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a.b.c"])),
                Arc::new(StringArray::from(vec!["."])),
                Arc::new(Int64Array::from(vec![0i64])),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();

        let result = ctx
            .sql("SELECT substring_index(s, d, c) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), ""); // count=0 returns empty string
    }

    #[tokio::test]
    async fn test_substring_index_positive_count() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_substring_index_udf());

        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a.b.c"])),
                Arc::new(StringArray::from(vec!["."])),
                Arc::new(Int64Array::from(vec![2i64])),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();

        let result = ctx
            .sql("SELECT substring_index(s, d, c) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "a.b");
    }

    #[tokio::test]
    async fn test_substring_index_negative_count() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_substring_index_udf());

        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a.b.c"])),
                Arc::new(StringArray::from(vec!["."])),
                Arc::new(Int64Array::from(vec![-1i64])),
            ],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();

        let result = ctx
            .sql("SELECT substring_index(s, d, c) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(arr.value(0), "c");
    }
}
