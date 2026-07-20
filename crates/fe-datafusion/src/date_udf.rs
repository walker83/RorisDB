//! Date/time UDF implementations for MySQL/Doris compatibility.
//!
//! Provides MySQL-compatible date/time functions:
//! - Extraction: year, month, day, dayofmonth, hour, minute, second
//! - Calendar: dayofweek, dayofyear, datediff, date_format, str_to_date
//! - Conversion: from_unixtime, unix_timestamp
//! - Construction: makedate, maketime, last_day
//! - Current time: curdate, curtime (volatile)
//! - Arithmetic: date_add, date_sub

use std::sync::Arc;

use arrow_array::*;
use arrow_schema::DataType;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

/// Parse various date string formats into days since epoch (Date32).
fn parse_date_str_to_days(s: &str) -> Option<i32> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .or_else(|| NaiveDate::parse_from_str(s, "%Y/%m/%d").ok())
        .or_else(|| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|dt| dt.date())
        })
        .or_else(|| {
            NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S")
                .ok()
                .map(|dt| dt.date())
        })
        .or_else(|| NaiveDate::parse_from_str(s, "%Y%m%d").ok())
        .map(|d| (d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
}

/// Convert MySQL format specifiers to chrono format specifiers.
fn mysql_to_chrono_fmt(fmt: &str) -> String {
    let mut out = String::new();
    let mut chars = fmt.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('Y') => out.push_str("%Y"),
            Some('y') => out.push_str("%y"),
            Some('m') => out.push_str("%m"),
            Some('d') => out.push_str("%d"),
            Some('H') => out.push_str("%H"),
            Some('i') => out.push_str("%M"),
            Some('s') => out.push_str("%S"),
            Some('W') => out.push_str("%A"),
            Some('M') => out.push_str("%B"),
            Some('b') => out.push_str("%b"),
            Some('T') => out.push_str("%T"),
            Some('r') => out.push_str("%r"),
            Some('f') => out.push_str("%f"),
            Some('j') => out.push_str("%j"),
            Some('a') => out.push_str("%a"),
            Some('c') => out.push_str("%-m"),
            Some('e') => out.push_str("%-d"),
            Some('k') => out.push_str("%-H"),
            Some('l') => out.push_str("%-I"),
            Some('p') => out.push_str("%p"),
            Some('x') => out.push_str("%G"),
            Some('v') => out.push_str("%V"),
            Some(other) => {
                out.push('%');
                out.push(other);
            }
            None => out.push('%'),
        }
    }
    out
}

/// Convert Date32 days-since-epoch to a NaiveDate.
fn days_to_date(days: i32) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(chrono::TimeDelta::days(days as i64))
}

// ---------------------------------------------------------------------------
// year
// ---------------------------------------------------------------------------

/// Extract year from a Date32 or date string.
pub fn create_year_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct YearUdf {
        signature: Signature,
    }

    impl YearUdf {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![DataType::Date32, DataType::Utf8],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for YearUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "year"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            match args[0].data_type() {
                DataType::Date32 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("year: expected Date32Array".to_string())
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|d| {
                            d.and_then(|days| days_to_date(days))
                                .map(|date| date.year() as i64)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                DataType::Utf8 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("year: expected StringArray".to_string())
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|s| {
                            s.and_then(parse_date_str_to_days)
                                .and_then(days_to_date)
                                .map(|date| date.year() as i64)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "year: unsupported type {:?}",
                    args[0].data_type()
                ))),
            }
        }

        fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
            Ok(arg_types.to_vec())
        }
    }

    ScalarUDF::new_from_impl(YearUdf::new())
}

// ---------------------------------------------------------------------------
// month
// ---------------------------------------------------------------------------

/// Extract month (1-12) from a Date32 or date string.
pub fn create_month_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct MonthUdf {
        signature: Signature,
    }

    impl MonthUdf {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![DataType::Date32, DataType::Utf8],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for MonthUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "month"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            match args[0].data_type() {
                DataType::Date32 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("month: expected Date32Array".to_string())
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|d| d.and_then(days_to_date).map(|date| date.month() as i64))
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                DataType::Utf8 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("month: expected StringArray".to_string())
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|s| {
                            s.and_then(parse_date_str_to_days)
                                .and_then(days_to_date)
                                .map(|date| date.month() as i64)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "month: unsupported type {:?}",
                    args[0].data_type()
                ))),
            }
        }

        fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
            Ok(arg_types.to_vec())
        }
    }

    ScalarUDF::new_from_impl(MonthUdf::new())
}

// ---------------------------------------------------------------------------
// day (and dayofmonth alias)
// ---------------------------------------------------------------------------

/// Extract day of month (1-31) from a Date32 or date string.
#[derive(Debug)]
struct DayUdfInner {
    signature: Signature,
    name: String,
}

impl DayUdfInner {
    fn new(name: &str) -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::Date32, DataType::Utf8],
                Volatility::Immutable,
            ),
            name: name.to_string(),
        }
    }

    fn execute(&self, args: &[ColumnarValue]) -> datafusion::error::Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        match args[0].data_type() {
            DataType::Date32 => {
                let arr = args[0]
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!("{}: expected Date32Array", self.name))
                    })?;
                let result: Vec<Option<i64>> = arr
                    .iter()
                    .map(|d| d.and_then(days_to_date).map(|date| date.day() as i64))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
            }
            DataType::Utf8 => {
                let arr = args[0]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!("{}: expected StringArray", self.name))
                    })?;
                let result: Vec<Option<i64>> = arr
                    .iter()
                    .map(|s| {
                        s.and_then(parse_date_str_to_days)
                            .and_then(days_to_date)
                            .map(|date| date.day() as i64)
                    })
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
            }
            _ => Err(DataFusionError::Internal(format!(
                "{}: unsupported type {:?}",
                self.name,
                args[0].data_type()
            ))),
        }
    }
}

impl ScalarUDFImpl for DayUdfInner {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        self.execute(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }
}

pub fn create_day_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(DayUdfInner::new("day"))
}

pub fn create_dayofmonth_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(DayUdfInner::new("dayofmonth"))
}

// ---------------------------------------------------------------------------
// hour
// ---------------------------------------------------------------------------

/// Extract hour from a Timestamp(Second, None) or datetime string.
pub fn create_hour_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct HourUdf {
        signature: Signature,
    }

    impl HourUdf {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![
                        DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
                        DataType::Utf8,
                    ],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for HourUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "hour"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            match args[0].data_type() {
                DataType::Timestamp(_, _) => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "hour: expected TimestampSecondArray".to_string(),
                            )
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|ts| {
                            ts.and_then(|secs| {
                                chrono::DateTime::from_timestamp(secs, 0).map(|dt| dt.naive_utc())
                            })
                            .map(|dt| dt.hour() as i64)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                DataType::Utf8 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("hour: expected StringArray".to_string())
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|s| {
                            s.and_then(|s| {
                                NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                    .ok()
                                    .or_else(|| {
                                        NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S").ok()
                                    })
                                    .or_else(|| {
                                        NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok()
                                    })
                            })
                            .map(|dt| dt.hour() as i64)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "hour: unsupported type {:?}",
                    args[0].data_type()
                ))),
            }
        }

        fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
            Ok(arg_types.to_vec())
        }
    }

    ScalarUDF::new_from_impl(HourUdf::new())
}

// ---------------------------------------------------------------------------
// minute
// ---------------------------------------------------------------------------

/// Extract minute from a Timestamp(Second, None) or datetime string.
pub fn create_minute_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct MinuteUdf {
        signature: Signature,
    }

    impl MinuteUdf {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![
                        DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
                        DataType::Utf8,
                    ],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for MinuteUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "minute"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            match args[0].data_type() {
                DataType::Timestamp(_, _) => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "minute: expected TimestampSecondArray".to_string(),
                            )
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|ts| {
                            ts.and_then(|secs| {
                                chrono::DateTime::from_timestamp(secs, 0).map(|dt| dt.naive_utc())
                            })
                            .map(|dt| dt.minute() as i64)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                DataType::Utf8 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("minute: expected StringArray".to_string())
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|s| {
                            s.and_then(|s| {
                                NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                    .ok()
                                    .or_else(|| {
                                        NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S").ok()
                                    })
                                    .or_else(|| {
                                        NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok()
                                    })
                            })
                            .map(|dt| dt.minute() as i64)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "minute: unsupported type {:?}",
                    args[0].data_type()
                ))),
            }
        }

        fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
            Ok(arg_types.to_vec())
        }
    }

    ScalarUDF::new_from_impl(MinuteUdf::new())
}

// ---------------------------------------------------------------------------
// second
// ---------------------------------------------------------------------------

/// Extract second from a Timestamp(Second, None) or datetime string.
pub fn create_second_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct SecondUdf {
        signature: Signature,
    }

    impl SecondUdf {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![
                        DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
                        DataType::Utf8,
                    ],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for SecondUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "second"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            match args[0].data_type() {
                DataType::Timestamp(_, _) => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "second: expected TimestampSecondArray".to_string(),
                            )
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|ts| {
                            ts.and_then(|secs| {
                                chrono::DateTime::from_timestamp(secs, 0).map(|dt| dt.naive_utc())
                            })
                            .map(|dt| dt.second() as i64)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                DataType::Utf8 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("second: expected StringArray".to_string())
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|s| {
                            s.and_then(|s| {
                                NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                    .ok()
                                    .or_else(|| {
                                        NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S").ok()
                                    })
                                    .or_else(|| {
                                        NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok()
                                    })
                            })
                            .map(|dt| dt.second() as i64)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "second: unsupported type {:?}",
                    args[0].data_type()
                ))),
            }
        }

        fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
            Ok(arg_types.to_vec())
        }
    }

    ScalarUDF::new_from_impl(SecondUdf::new())
}

// ---------------------------------------------------------------------------
// dayofweek
// ---------------------------------------------------------------------------

/// Extract day of week (1=Sunday, 7=Saturday, MySQL-compatible).
pub fn create_dayofweek_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct DayOfWeekUdf {
        signature: Signature,
    }

    impl DayOfWeekUdf {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![DataType::Date32, DataType::Utf8],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for DayOfWeekUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "dayofweek"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            match args[0].data_type() {
                DataType::Date32 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("dayofweek: expected Date32Array".to_string())
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|d| {
                            d.and_then(days_to_date)
                                .map(|date| date.weekday().num_days_from_sunday() as i64 + 1)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                DataType::Utf8 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("dayofweek: expected StringArray".to_string())
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|s| {
                            s.and_then(parse_date_str_to_days)
                                .and_then(days_to_date)
                                .map(|date| date.weekday().num_days_from_sunday() as i64 + 1)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "dayofweek: unsupported type {:?}",
                    args[0].data_type()
                ))),
            }
        }

        fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
            Ok(arg_types.to_vec())
        }
    }

    ScalarUDF::new_from_impl(DayOfWeekUdf::new())
}

// ---------------------------------------------------------------------------
// dayofyear
// ---------------------------------------------------------------------------

/// Extract day of year (1-366).
pub fn create_dayofyear_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct DayOfYearUdf {
        signature: Signature,
    }

    impl DayOfYearUdf {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![DataType::Date32, DataType::Utf8],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for DayOfYearUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "dayofyear"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            match args[0].data_type() {
                DataType::Date32 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("dayofyear: expected Date32Array".to_string())
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|d| d.and_then(days_to_date).map(|date| date.ordinal() as i64))
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                DataType::Utf8 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("dayofyear: expected StringArray".to_string())
                        })?;
                    let result: Vec<Option<i64>> = arr
                        .iter()
                        .map(|s| {
                            s.and_then(parse_date_str_to_days)
                                .and_then(days_to_date)
                                .map(|date| date.ordinal() as i64)
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "dayofyear: unsupported type {:?}",
                    args[0].data_type()
                ))),
            }
        }

        fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
            Ok(arg_types.to_vec())
        }
    }

    ScalarUDF::new_from_impl(DayOfYearUdf::new())
}

// ---------------------------------------------------------------------------
// datediff
// ---------------------------------------------------------------------------

/// Difference in days between two dates (d1 - d2).
pub fn create_datediff_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct DatediffUdf {
        signature: Signature,
    }

    impl DatediffUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Date32, DataType::Date32],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for DatediffUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "datediff"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let d1 = args[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("datediff: d1 must be Date32Array".to_string())
                })?;
            let d2 = args[1]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("datediff: d2 must be Date32Array".to_string())
                })?;

            let result: Vec<Option<i64>> = d1
                .iter()
                .zip(d2.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(date1), Some(date2)) => Some((date1 - date2) as i64),
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(Arc::new(Int64Array::from(result))))
        }

        fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
            Ok(arg_types.to_vec())
        }
    }

    ScalarUDF::new_from_impl(DatediffUdf::new())
}

// ---------------------------------------------------------------------------
// date_format
// ---------------------------------------------------------------------------

/// Format a date/datetime using MySQL format specifiers.
pub fn create_date_format_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct DateFormatUdf {
        signature: Signature,
    }

    impl DateFormatUdf {
        fn new() -> Self {
            Self {
                signature: Signature::one_of(
                    vec![
                        datafusion::logical_expr::TypeSignature::Exact(vec![
                            DataType::Date32,
                            DataType::Utf8,
                        ]),
                        datafusion::logical_expr::TypeSignature::Exact(vec![
                            DataType::Utf8,
                            DataType::Utf8,
                        ]),
                        datafusion::logical_expr::TypeSignature::Exact(vec![
                            DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
                            DataType::Utf8,
                        ]),
                    ],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for DateFormatUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "date_format"
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
            let raw_args = ColumnarValue::values_to_arrays(&args.args)?;
            let fmt_arr = raw_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("date_format: format must be StringArray".to_string())
                })?;

            match raw_args[0].data_type() {
                DataType::Date32 => {
                    let date_arr = raw_args[0]
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "date_format: expected Date32Array".to_string(),
                            )
                        })?;

                    let result: Vec<Option<String>> = date_arr
                        .iter()
                        .zip(fmt_arr.iter())
                        .map(|(d, f)| match (d, f) {
                            (Some(days), Some(fmt)) => {
                                let date = days_to_date(days)?;
                                let chrono_fmt = mysql_to_chrono_fmt(fmt);
                                Some(date.format(&chrono_fmt).to_string())
                            }
                            _ => None,
                        })
                        .collect();

                    Ok(ColumnarValue::Array(
                        Arc::new(StringArray::from(result)) as Arc<dyn arrow_array::Array>
                    ))
                }
                DataType::Utf8 => {
                    let str_arr = raw_args[0]
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "date_format: expected StringArray".to_string(),
                            )
                        })?;

                    let result: Vec<Option<String>> = str_arr
                        .iter()
                        .zip(fmt_arr.iter())
                        .map(|(s, f)| match (s, f) {
                            (Some(s), Some(fmt)) => {
                                // Try parsing as datetime first, then as date-only
                                if let Ok(dt) =
                                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                {
                                    let chrono_fmt = mysql_to_chrono_fmt(fmt);
                                    Some(dt.format(&chrono_fmt).to_string())
                                } else if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                    let chrono_fmt = mysql_to_chrono_fmt(fmt);
                                    Some(d.format(&chrono_fmt).to_string())
                                } else {
                                    let days = parse_date_str_to_days(s)?;
                                    let date = days_to_date(days)?;
                                    let chrono_fmt = mysql_to_chrono_fmt(fmt);
                                    Some(date.format(&chrono_fmt).to_string())
                                }
                            }
                            _ => None,
                        })
                        .collect();

                    Ok(ColumnarValue::Array(
                        Arc::new(StringArray::from(result)) as Arc<dyn arrow_array::Array>
                    ))
                }
                DataType::Timestamp(_, _) => {
                    let ts_arr = raw_args[0]
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "date_format: expected TimestampSecondArray".to_string(),
                            )
                        })?;

                    let result: Vec<Option<String>> = ts_arr
                        .iter()
                        .zip(fmt_arr.iter())
                        .map(|(ts, f)| match (ts, f) {
                            (Some(secs), Some(fmt)) => {
                                let dt = chrono::DateTime::from_timestamp(secs, 0)?.naive_utc();
                                let chrono_fmt = mysql_to_chrono_fmt(fmt);
                                Some(dt.format(&chrono_fmt).to_string())
                            }
                            _ => None,
                        })
                        .collect();

                    Ok(ColumnarValue::Array(
                        Arc::new(StringArray::from(result)) as Arc<dyn arrow_array::Array>
                    ))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "date_format: unsupported type {:?}",
                    raw_args[0].data_type()
                ))),
            }
        }

        fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
            Ok(arg_types.to_vec())
        }
    }

    ScalarUDF::new_from_impl(DateFormatUdf::new())
}

// ---------------------------------------------------------------------------
// str_to_date
// ---------------------------------------------------------------------------

/// Parse a string to Date32 using MySQL format specifiers.
pub fn create_str_to_date_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct StrToDateUdf {
        signature: Signature,
    }

    impl StrToDateUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Utf8, DataType::Utf8],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for StrToDateUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "str_to_date"
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
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let str_arr = args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("str_to_date: str must be StringArray".to_string())
                })?;
            let fmt_arr = args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("str_to_date: fmt must be StringArray".to_string())
                })?;

            let result: Vec<Option<i32>> = str_arr
                .iter()
                .zip(fmt_arr.iter())
                .map(|(s, f)| match (s, f) {
                    (Some(s), Some(fmt)) => {
                        let chrono_fmt = mysql_to_chrono_fmt(fmt);
                        let date = NaiveDate::parse_from_str(s, &chrono_fmt).ok()?;
                        Some(
                            (date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32,
                        )
                    }
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(Date32Array::from(result)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(StrToDateUdf::new())
}

// ---------------------------------------------------------------------------
// from_unixtime
// ---------------------------------------------------------------------------

/// Convert unix timestamp to datetime string.
pub fn create_from_unixtime_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct FromUnixtimeUdf {
        signature: Signature,
    }

    impl FromUnixtimeUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for FromUnixtimeUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "from_unixtime"
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
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let arr = args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("from_unixtime: expected Int64Array".to_string())
                })?;

            let result: Vec<Option<String>> = arr
                .iter()
                .map(|n| {
                    n.and_then(|secs| {
                        chrono::DateTime::from_timestamp(secs, 0).map(|dt| dt.naive_utc())
                    })
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(StringArray::from(result)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(FromUnixtimeUdf::new())
}

// ---------------------------------------------------------------------------
// unix_timestamp
// ---------------------------------------------------------------------------

/// Current time as epoch seconds (0 args) or parse date string to epoch seconds (1 arg).
pub fn create_unix_timestamp_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct UnixTimestampUdf {
        signature: Signature,
    }

    impl UnixTimestampUdf {
        fn new() -> Self {
            Self {
                signature: Signature::one_of(
                    vec![
                        datafusion::logical_expr::TypeSignature::Exact(vec![]),
                        datafusion::logical_expr::TypeSignature::Exact(vec![DataType::Utf8]),
                    ],
                    Volatility::Volatile,
                ),
            }
        }
    }

    impl ScalarUDFImpl for UnixTimestampUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "unix_timestamp"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int64)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            if args.args.is_empty() {
                // 0 args: current timestamp
                let now = chrono::Utc::now().timestamp();
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(now))))
            } else {
                // 1 Utf8 arg: parse date string
                let raw_args = ColumnarValue::values_to_arrays(&args.args)?;
                let arr = raw_args[0]
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "unix_timestamp: expected StringArray".to_string(),
                        )
                    })?;

                let result: Vec<Option<i64>> = arr
                    .iter()
                    .map(|s| {
                        s.and_then(|s| {
                            // Try datetime formats first, then date-only
                            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                .ok()
                                .or_else(|| {
                                    NaiveDateTime::parse_from_str(s, "%Y/%m/%d %H:%M:%S").ok()
                                })
                                .or_else(|| {
                                    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok()
                                })
                                .or_else(|| {
                                    NaiveDate::parse_from_str(s, "%Y-%m-%d")
                                        .ok()
                                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                                })
                                .or_else(|| {
                                    NaiveDate::parse_from_str(s, "%Y/%m/%d")
                                        .ok()
                                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                                })
                                .map(|dt| dt.and_utc().timestamp())
                        })
                    })
                    .collect();

                Ok(ColumnarValue::Array(
                    Arc::new(Int64Array::from(result)) as Arc<dyn arrow_array::Array>
                ))
            }
        }
    }

    ScalarUDF::new_from_impl(UnixTimestampUdf::new())
}

// ---------------------------------------------------------------------------
// makedate
// ---------------------------------------------------------------------------

/// Create a date from year and day-of-year.
pub fn create_makedate_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct MakedateUdf {
        signature: Signature,
    }

    impl MakedateUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Int64, DataType::Int64],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for MakedateUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "makedate"
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
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let year_arr = args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("makedate: year must be Int64Array".to_string())
                })?;
            let doy_arr = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("makedate: dayofyear must be Int64Array".to_string())
                })?;

            let result: Vec<Option<i32>> = year_arr
                .iter()
                .zip(doy_arr.iter())
                .map(|(y, d)| match (y, d) {
                    (Some(year), Some(doy)) => {
                        let date = NaiveDate::from_yo_opt(year as i32, doy as u32)?;
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
                        Some(date.signed_duration_since(epoch).num_days() as i32)
                    }
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(Date32Array::from(result)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(MakedateUdf::new())
}

// ---------------------------------------------------------------------------
// maketime
// ---------------------------------------------------------------------------

/// Create a time string from hour, minute, second.
pub fn create_maketime_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct MaketimeUdf {
        signature: Signature,
    }

    impl MaketimeUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Int64, DataType::Int64, DataType::Int64],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for MaketimeUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "maketime"
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
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let h_arr = args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("maketime: hour must be Int64Array".to_string())
                })?;
            let m_arr = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("maketime: minute must be Int64Array".to_string())
                })?;
            let s_arr = args[2]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("maketime: second must be Int64Array".to_string())
                })?;

            let result: Vec<Option<String>> = h_arr
                .iter()
                .zip(m_arr.iter())
                .zip(s_arr.iter())
                .map(|((h, m), s)| match (h, m, s) {
                    (Some(h), Some(m), Some(s)) => Some(format!("{:02}:{:02}:{:02}", h, m, s)),
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(StringArray::from(result)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(MaketimeUdf::new())
}

// ---------------------------------------------------------------------------
// last_day
// ---------------------------------------------------------------------------

/// Return the last day of the month for a given date.
pub fn create_last_day_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct LastDayUdf {
        signature: Signature,
    }

    impl LastDayUdf {
        fn new() -> Self {
            Self {
                signature: Signature::uniform(
                    1,
                    vec![DataType::Date32, DataType::Utf8],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for LastDayUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "last_day"
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
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            match args[0].data_type() {
                DataType::Date32 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("last_day: expected Date32Array".to_string())
                        })?;
                    let result: Vec<Option<i32>> = arr
                        .iter()
                        .map(|d| {
                            d.and_then(|days| {
                                let date = days_to_date(days)?;
                                // First day of next month, then subtract one day
                                let (y, m) = if date.month() == 12 {
                                    (date.year() + 1, 1u32)
                                } else {
                                    (date.year(), date.month() + 1)
                                };
                                let first_of_next = NaiveDate::from_ymd_opt(y, m, 1)?;
                                let last_day = first_of_next.pred_opt()?;
                                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
                                Some(last_day.signed_duration_since(epoch).num_days() as i32)
                            })
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Date32Array::from(result))))
                }
                DataType::Utf8 => {
                    let arr = args[0]
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("last_day: expected StringArray".to_string())
                        })?;
                    let result: Vec<Option<i32>> = arr
                        .iter()
                        .map(|s| {
                            s.and_then(parse_date_str_to_days)
                                .and_then(days_to_date)
                                .and_then(|date| {
                                    let (y, m) = if date.month() == 12 {
                                        (date.year() + 1, 1u32)
                                    } else {
                                        (date.year(), date.month() + 1)
                                    };
                                    let first_of_next = NaiveDate::from_ymd_opt(y, m, 1)?;
                                    let last_day = first_of_next.pred_opt()?;
                                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
                                    Some(last_day.signed_duration_since(epoch).num_days() as i32)
                                })
                        })
                        .collect();
                    Ok(ColumnarValue::Array(Arc::new(Date32Array::from(result))))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "last_day: unsupported type {:?}",
                    args[0].data_type()
                ))),
            }
        }

        fn coerce_types(&self, arg_types: &[DataType]) -> datafusion::error::Result<Vec<DataType>> {
            Ok(arg_types.to_vec())
        }
    }

    ScalarUDF::new_from_impl(LastDayUdf::new())
}

// ---------------------------------------------------------------------------
// curdate
// ---------------------------------------------------------------------------

/// Current date (volatile).
pub fn create_curdate_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct CurdateUdf {
        signature: Signature,
    }

    impl CurdateUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![], Volatility::Volatile),
            }
        }
    }

    impl ScalarUDFImpl for CurdateUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "curdate"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Date32)
        }

        fn invoke_with_args(
            &self,
            _args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let now = chrono::Utc::now().date_naive();
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = now.signed_duration_since(epoch).num_days() as i32;
            Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(days))))
        }
    }

    ScalarUDF::new_from_impl(CurdateUdf::new())
}

// ---------------------------------------------------------------------------
// curtime
// ---------------------------------------------------------------------------

/// Current time as string (volatile).
pub fn create_curtime_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct CurtimeUdf {
        signature: Signature,
    }

    impl CurtimeUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![], Volatility::Volatile),
            }
        }
    }

    impl ScalarUDFImpl for CurtimeUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "curtime"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(
            &self,
            _args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let now = chrono::Utc::now();
            let time_str = now.format("%H:%M:%S").to_string();
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(time_str))))
        }
    }

    ScalarUDF::new_from_impl(CurtimeUdf::new())
}

// ---------------------------------------------------------------------------
// date_add
// ---------------------------------------------------------------------------

/// Add days to a date.
pub fn create_date_add_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct DateAddUdf {
        signature: Signature,
    }

    impl DateAddUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Date32, DataType::Int64],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for DateAddUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "date_add"
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
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let dates = args[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("date_add: dates must be Date32Array".to_string())
                })?;
            let days = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("date_add: days must be Int64Array".to_string())
                })?;

            let result: Vec<Option<i32>> = dates
                .iter()
                .zip(days.iter())
                .map(|(d, n)| match (d, n) {
                    (Some(date), Some(n)) => {
                        // Date32 is already days since epoch, so just add
                        let n_clamped = n.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                        Some(date + n_clamped)
                    }
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(Date32Array::from(result)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(DateAddUdf::new())
}

// ---------------------------------------------------------------------------
// date_sub
// ---------------------------------------------------------------------------

/// Subtract days from a date.
pub fn create_date_sub_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct DateSubUdf {
        signature: Signature,
    }

    impl DateSubUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(
                    vec![DataType::Date32, DataType::Int64],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for DateSubUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "date_sub"
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
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let dates = args[0]
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("date_sub: dates must be Date32Array".to_string())
                })?;
            let days = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("date_sub: days must be Int64Array".to_string())
                })?;

            let result: Vec<Option<i32>> = dates
                .iter()
                .zip(days.iter())
                .map(|(d, n)| match (d, n) {
                    (Some(date), Some(n)) => {
                        // Negate the days argument for subtraction
                        let n_clamped = n.clamp(i32::MIN as i64, i32::MAX as i64) as i32;
                        Some(date - n_clamped)
                    }
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(Date32Array::from(result)) as Arc<dyn arrow_array::Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(DateSubUdf::new())
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register all date/time UDFs with a DataFusion context.
pub fn register_date_udfs(ctx: &mut datafusion::prelude::SessionContext) {
    ctx.register_udf(create_year_udf());
    ctx.register_udf(create_month_udf());
    ctx.register_udf(create_day_udf());
    ctx.register_udf(create_dayofmonth_udf());
    ctx.register_udf(create_hour_udf());
    ctx.register_udf(create_minute_udf());
    ctx.register_udf(create_second_udf());
    ctx.register_udf(create_dayofweek_udf());
    ctx.register_udf(create_dayofyear_udf());
    ctx.register_udf(create_datediff_udf());
    ctx.register_udf(create_date_format_udf());
    ctx.register_udf(create_str_to_date_udf());
    ctx.register_udf(create_from_unixtime_udf());
    ctx.register_udf(create_unix_timestamp_udf());
    ctx.register_udf(create_makedate_udf());
    ctx.register_udf(create_maketime_udf());
    ctx.register_udf(create_last_day_udf());
    ctx.register_udf(create_curdate_udf());
    ctx.register_udf(create_curtime_udf());
    ctx.register_udf(create_date_add_udf());
    ctx.register_udf(create_date_sub_udf());
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Date32Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    #[tokio::test]
    async fn test_date_add_normal() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_date_add_udf());

        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("n", DataType::Int64, true),
        ]));
        // 2020-01-01 = days since epoch 18262
        let dates = Date32Array::from(vec![Some(18262)]);
        let days = Int64Array::from(vec![Some(10i64)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(dates), Arc::new(days)]).unwrap();
        ctx.register_batch("t", batch).unwrap();

        let result = ctx
            .sql("SELECT date_add(d, n) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(arr.value(0), 18272); // 18262 + 10
    }

    #[tokio::test]
    async fn test_date_add_large_value_clamped() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_date_add_udf());

        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let dates = Date32Array::from(vec![Some(100)]);
        // Value larger than i32::MAX should be clamped, not wrap
        let days = Int64Array::from(vec![Some(i64::MAX)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(dates), Arc::new(days)]).unwrap();
        ctx.register_batch("t", batch).unwrap();

        let result = ctx
            .sql("SELECT date_add(d, n) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        // Should be 100 + i32::MAX (clamped), not a wrapping panic
        assert_eq!(arr.value(0), 100 + i32::MAX);
    }

    #[tokio::test]
    async fn test_date_sub_large_value_clamped() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_date_sub_udf());

        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let dates = Date32Array::from(vec![Some(200_000)]);
        let days = Int64Array::from(vec![Some(i64::MAX)]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(dates), Arc::new(days)]).unwrap();
        ctx.register_batch("t", batch).unwrap();

        let result = ctx
            .sql("SELECT date_sub(d, n) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        // Should be 200000 - i32::MAX (clamped), not panic
        assert_eq!(arr.value(0), 200_000 - i32::MAX);
    }

    #[tokio::test]
    async fn test_date_add_null_handling() {
        let ctx = SessionContext::new();
        ctx.register_udf(create_date_add_udf());

        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let dates = Date32Array::from(vec![None::<i32>, Some(100)]);
        let days = Int64Array::from(vec![Some(10i64), None]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(dates), Arc::new(days)]).unwrap();
        ctx.register_batch("t", batch).unwrap();

        let result = ctx
            .sql("SELECT date_add(d, n) FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let arr = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert!(arr.is_null(0)); // null date → null
        assert!(arr.is_null(1)); // null days → null
    }
}
