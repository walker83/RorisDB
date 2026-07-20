//! Miscellaneous UDF implementations for MySQL/Doris compatibility.
//!
//! Provides MySQL-compatible functions:
//! - hex / unhex: hex string conversion
//! - truncate: numeric truncation
//! - group_concat: aggregate string concatenation
//! - if / ifnull: conditional functions
//! - uuid: random UUID generation
//! - version: MySQL version string
//! - database: current database name

use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::*;
use arrow_schema::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;

// ---------------------------------------------------------------------------
// hex(n) — Convert integer to uppercase hex string
// ---------------------------------------------------------------------------

pub fn create_hex_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct HexUdf {
        signature: Signature,
    }

    impl HexUdf {
        fn new() -> Self {
            Self {
                signature: Signature::one_of(
                    vec![
                        TypeSignature::Exact(vec![DataType::Int64]),
                        TypeSignature::Exact(vec![DataType::Utf8]),
                    ],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for HexUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "hex"
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
            let arr = &args[0];

            if arr.data_type() == &DataType::Int64 {
                let int_arr = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Internal("hex: expected Int64Array".to_string())
                })?;
                let result: Vec<Option<String>> = int_arr
                    .iter()
                    .map(|v| v.map(|n| format!("{:X}", n)))
                    .collect();
                Ok(ColumnarValue::Array(
                    Arc::new(StringArray::from(result)) as Arc<dyn Array>
                ))
            } else {
                let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Internal("hex: expected StringArray".to_string())
                })?;
                let result: Vec<Option<String>> = str_arr
                    .iter()
                    .map(|s| s.map(|v| v.bytes().map(|b| format!("{:02X}", b)).collect::<String>()))
                    .collect();
                Ok(ColumnarValue::Array(
                    Arc::new(StringArray::from(result)) as Arc<dyn Array>
                ))
            }
        }
    }

    ScalarUDF::new_from_impl(HexUdf::new())
}

// ---------------------------------------------------------------------------
// unhex(str) — Convert hex string to bytes
// ---------------------------------------------------------------------------

pub fn create_unhex_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct UnhexUdf {
        signature: Signature,
    }

    impl UnhexUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for UnhexUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "unhex"
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
            let str_arr = args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("unhex: expected StringArray".to_string())
                })?;

            let result: Vec<Option<String>> = str_arr
                .iter()
                .map(|s| s.and_then(|v| decode_hex(&v)))
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(StringArray::from(result)) as Arc<dyn Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(UnhexUdf::new())
}

/// Decode a hex string to a UTF-8 string. Returns None for invalid hex.
fn decode_hex(s: &str) -> Option<String> {
    if s.len() % 2 != 0 {
        return None;
    }
    let bytes: Option<Vec<u8>> = s
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let hi = hex_char_to_u8(pair[0])?;
            let lo = hex_char_to_u8(pair[1])?;
            Some((hi << 4) | lo)
        })
        .collect();
    bytes.and_then(|b| String::from_utf8(b).ok())
}

fn hex_char_to_u8(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// truncate(n, d) — Truncate number to d decimal places (MySQL TRUNCATE)
// ---------------------------------------------------------------------------

pub fn create_truncate_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct TruncateUdf {
        signature: Signature,
    }

    impl TruncateUdf {
        fn new() -> Self {
            Self {
                signature: Signature::one_of(
                    vec![
                        TypeSignature::Exact(vec![DataType::Float64, DataType::Int64]),
                        TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                    ],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for TruncateUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "truncate"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Float64)
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let decimals_arr = args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("truncate: decimals must be Int64Array".to_string())
                })?;

            let result: Vec<Option<f64>> = if args[0].data_type() == &DataType::Float64 {
                let nums = args[0]
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "truncate: first arg must be Float64Array".to_string(),
                        )
                    })?;
                nums.iter()
                    .zip(decimals_arr.iter())
                    .map(|(n, d)| match (n, d) {
                        (Some(n), Some(d)) => {
                            let factor = 10f64.powi(d as i32);
                            Some((n * factor).trunc() / factor)
                        }
                        _ => None,
                    })
                    .collect()
            } else {
                let nums = args[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "truncate: first arg must be Int64Array".to_string(),
                        )
                    })?;
                nums.iter()
                    .zip(decimals_arr.iter())
                    .map(|(n, d)| match (n, d) {
                        (Some(n), Some(d)) => {
                            let n_f = n as f64;
                            let factor = 10f64.powi(d as i32);
                            Some((n_f * factor).trunc() / factor)
                        }
                        _ => None,
                    })
                    .collect()
            };

            Ok(ColumnarValue::Array(
                Arc::new(Float64Array::from(result)) as Arc<dyn Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(TruncateUdf::new())
}

// ---------------------------------------------------------------------------
// group_concat(col) — Aggregate: concatenate values with comma
// ---------------------------------------------------------------------------

pub fn create_group_concat_udf() -> AggregateUDF {
    #[derive(Debug)]
    struct GroupConcatAccumulator {
        values: Vec<String>,
        seen: std::collections::HashSet<String>, // For DISTINCT support
        is_distinct: bool,
    }

    impl GroupConcatAccumulator {
        fn new() -> Self {
            Self {
                values: Vec::new(),
                seen: std::collections::HashSet::new(),
                is_distinct: false,
            }
        }

        fn new_distinct() -> Self {
            Self {
                values: Vec::new(),
                seen: std::collections::HashSet::new(),
                is_distinct: true,
            }
        }
    }

    impl Accumulator for GroupConcatAccumulator {
        fn update_batch(&mut self, values: &[Arc<dyn Array>]) -> datafusion::error::Result<()> {
            if values.is_empty() {
                return Ok(());
            }
            let arr = &values[0];
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Internal("group_concat: expected StringArray".to_string())
            })?;
            for i in 0..str_arr.len() {
                if !str_arr.is_null(i) {
                    let val = str_arr.value(i).to_string();
                    if self.is_distinct {
                        // Only add if we haven't seen this value before
                        if self.seen.insert(val.clone()) {
                            self.values.push(val);
                        }
                    } else {
                        self.values.push(val);
                    }
                }
            }
            Ok(())
        }

        fn merge_batch(&mut self, states: &[Arc<dyn Array>]) -> datafusion::error::Result<()> {
            if states.is_empty() {
                return Ok(());
            }
            let list_arr = states[0]
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "group_concat merge: expected ListArray state".to_string(),
                    )
                })?;

            for i in 0..list_arr.len() {
                if list_arr.is_null(i) {
                    continue;
                }
                let inner = list_arr.value(i);
                let str_inner = inner
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "group_concat merge: expected StringArray in list".to_string(),
                        )
                    })?;
                for j in 0..str_inner.len() {
                    if !str_inner.is_null(j) {
                        let val = str_inner.value(j).to_string();
                        if self.is_distinct {
                            if self.seen.insert(val.clone()) {
                                self.values.push(val);
                            }
                        } else {
                            self.values.push(val);
                        }
                    }
                }
            }
            Ok(())
        }

        fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
            let values: Vec<ScalarValue> = self
                .values
                .iter()
                .map(|s| ScalarValue::Utf8(Some(s.clone())))
                .collect();
            let field: Arc<arrow_schema::Field> =
                Arc::new(arrow_schema::Field::new("item", DataType::Utf8, true));

            if values.is_empty() {
                let list_array = ListArray::new_null(field, 1);
                return Ok(vec![ScalarValue::List(Arc::new(list_array))]);
            }

            let values_array = ScalarValue::iter_to_array(values.into_iter())?;
            let offsets = arrow_buffer::OffsetBuffer::from_lengths([values_array.len()]);
            let list_array = ListArray::new(field, offsets, values_array, None);
            Ok(vec![ScalarValue::List(Arc::new(list_array))])
        }

        fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
            let result = self.values.join(",");
            Ok(ScalarValue::Utf8(Some(result)))
        }

        fn size(&self) -> usize {
            self.values.iter().map(|s| s.len()).sum()
        }
    }

    #[derive(Debug)]
    struct GroupConcatUDFImpl {
        signature: Signature,
    }

    impl GroupConcatUDFImpl {
        fn new() -> Self {
            Self {
                signature: Signature::any(1, Volatility::Immutable),
            }
        }
    }

    impl AggregateUDFImpl for GroupConcatUDFImpl {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "group_concat"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn accumulator(
            &self,
            _acc_args: AccumulatorArgs,
        ) -> datafusion::error::Result<Box<dyn Accumulator>> {
            Ok(Box::new(GroupConcatAccumulator::new()))
        }

        fn state_fields(
            &self,
            args: StateFieldsArgs,
        ) -> datafusion::error::Result<Vec<Arc<arrow_schema::Field>>> {
            Ok(vec![Arc::new(arrow_schema::Field::new(
                format_state_name(args.name, "value"),
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
                true,
            ))])
        }
    }

    AggregateUDF::from(GroupConcatUDFImpl::new())
}

// ---------------------------------------------------------------------------
// if(cond, a, b) — Conditional function (MySQL IF)
// ---------------------------------------------------------------------------

pub fn create_if_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct IfUdf {
        signature: Signature,
    }

    impl IfUdf {
        fn new() -> Self {
            Self {
                signature: Signature::one_of(
                    vec![
                        TypeSignature::Exact(vec![
                            DataType::Boolean,
                            DataType::Utf8,
                            DataType::Utf8,
                        ]),
                        TypeSignature::Exact(vec![
                            DataType::Boolean,
                            DataType::Int64,
                            DataType::Int64,
                        ]),
                        TypeSignature::Exact(vec![
                            DataType::Boolean,
                            DataType::Float64,
                            DataType::Float64,
                        ]),
                    ],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for IfUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "if"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            if arg_types.len() >= 2 {
                Ok(arg_types[1].clone())
            } else {
                Ok(DataType::Utf8)
            }
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let cond_arr = args[0]
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("if: cond must be BooleanArray".to_string())
                })?;

            match args[1].data_type() {
                DataType::Utf8 => {
                    let a_arr =
                        args[1]
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "if: arg1 must be StringArray".to_string(),
                                )
                            })?;
                    let b_arr =
                        args[2]
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "if: arg2 must be StringArray".to_string(),
                                )
                            })?;
                    let result: Vec<Option<&str>> = (0..cond_arr.len())
                        .map(|i| {
                            if cond_arr.is_null(i) {
                                None
                            } else if cond_arr.value(i) {
                                if a_arr.is_null(i) {
                                    None
                                } else {
                                    Some(a_arr.value(i))
                                }
                            } else {
                                if b_arr.is_null(i) {
                                    None
                                } else {
                                    Some(b_arr.value(i))
                                }
                            }
                        })
                        .collect();
                    // Convert Vec<Option<&str>> to Vec<Option<String>> for StringArray
                    let result: Vec<Option<String>> = result
                        .into_iter()
                        .map(|s| s.map(|s| s.to_string()))
                        .collect();
                    Ok(ColumnarValue::Array(
                        Arc::new(StringArray::from(result)) as Arc<dyn Array>
                    ))
                }
                DataType::Int64 => {
                    let a_arr = args[1]
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("if: arg1 must be Int64Array".to_string())
                        })?;
                    let b_arr = args[2]
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("if: arg2 must be Int64Array".to_string())
                        })?;
                    let result: Vec<Option<i64>> = (0..cond_arr.len())
                        .map(|i| {
                            if cond_arr.is_null(i) {
                                None
                            } else if cond_arr.value(i) {
                                if a_arr.is_null(i) {
                                    None
                                } else {
                                    Some(a_arr.value(i))
                                }
                            } else {
                                if b_arr.is_null(i) {
                                    None
                                } else {
                                    Some(b_arr.value(i))
                                }
                            }
                        })
                        .collect();
                    Ok(ColumnarValue::Array(
                        Arc::new(Int64Array::from(result)) as Arc<dyn Array>
                    ))
                }
                DataType::Float64 => {
                    let a_arr =
                        args[1]
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "if: arg1 must be Float64Array".to_string(),
                                )
                            })?;
                    let b_arr =
                        args[2]
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "if: arg2 must be Float64Array".to_string(),
                                )
                            })?;
                    let result: Vec<Option<f64>> = (0..cond_arr.len())
                        .map(|i| {
                            if cond_arr.is_null(i) {
                                None
                            } else if cond_arr.value(i) {
                                if a_arr.is_null(i) {
                                    None
                                } else {
                                    Some(a_arr.value(i))
                                }
                            } else {
                                if b_arr.is_null(i) {
                                    None
                                } else {
                                    Some(b_arr.value(i))
                                }
                            }
                        })
                        .collect();
                    Ok(ColumnarValue::Array(
                        Arc::new(Float64Array::from(result)) as Arc<dyn Array>
                    ))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "if: unsupported type {:?}",
                    args[1].data_type()
                ))),
            }
        }
    }

    ScalarUDF::new_from_impl(IfUdf::new())
}

// ---------------------------------------------------------------------------
// ifnull(a, b) — Return a if not null, else b (IFNULL = COALESCE)
// ---------------------------------------------------------------------------

pub fn create_ifnull_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct IfnullUdf {
        signature: Signature,
    }

    impl IfnullUdf {
        fn new() -> Self {
            Self {
                signature: Signature::one_of(
                    vec![
                        TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                        TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                        TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
                    ],
                    Volatility::Immutable,
                ),
            }
        }
    }

    impl ScalarUDFImpl for IfnullUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "ifnull"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            if arg_types.is_empty() {
                Ok(DataType::Utf8)
            } else {
                Ok(arg_types[0].clone())
            }
        }

        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;

            match args[0].data_type() {
                DataType::Utf8 => {
                    let a_arr =
                        args[0]
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "ifnull: arg0 must be StringArray".to_string(),
                                )
                            })?;
                    let b_arr =
                        args[1]
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "ifnull: arg1 must be StringArray".to_string(),
                                )
                            })?;
                    let result: Vec<Option<String>> = (0..a_arr.len())
                        .map(|i| {
                            if !a_arr.is_null(i) {
                                Some(a_arr.value(i).to_string())
                            } else if !b_arr.is_null(i) {
                                Some(b_arr.value(i).to_string())
                            } else {
                                None
                            }
                        })
                        .collect();
                    Ok(ColumnarValue::Array(
                        Arc::new(StringArray::from(result)) as Arc<dyn Array>
                    ))
                }
                DataType::Int64 => {
                    let a_arr = args[0]
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("ifnull: arg0 must be Int64Array".to_string())
                        })?;
                    let b_arr = args[1]
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| {
                            DataFusionError::Internal("ifnull: arg1 must be Int64Array".to_string())
                        })?;
                    let result: Vec<Option<i64>> = (0..a_arr.len())
                        .map(|i| {
                            if !a_arr.is_null(i) {
                                Some(a_arr.value(i))
                            } else if !b_arr.is_null(i) {
                                Some(b_arr.value(i))
                            } else {
                                None
                            }
                        })
                        .collect();
                    Ok(ColumnarValue::Array(
                        Arc::new(Int64Array::from(result)) as Arc<dyn Array>
                    ))
                }
                DataType::Float64 => {
                    let a_arr =
                        args[0]
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "ifnull: arg0 must be Float64Array".to_string(),
                                )
                            })?;
                    let b_arr =
                        args[1]
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or_else(|| {
                                DataFusionError::Internal(
                                    "ifnull: arg1 must be Float64Array".to_string(),
                                )
                            })?;
                    let result: Vec<Option<f64>> = (0..a_arr.len())
                        .map(|i| {
                            if !a_arr.is_null(i) {
                                Some(a_arr.value(i))
                            } else if !b_arr.is_null(i) {
                                Some(b_arr.value(i))
                            } else {
                                None
                            }
                        })
                        .collect();
                    Ok(ColumnarValue::Array(
                        Arc::new(Float64Array::from(result)) as Arc<dyn Array>
                    ))
                }
                _ => Err(DataFusionError::Internal(format!(
                    "ifnull: unsupported type {:?}",
                    args[0].data_type()
                ))),
            }
        }
    }

    ScalarUDF::new_from_impl(IfnullUdf::new())
}

// ---------------------------------------------------------------------------
// uuid() — Generate random UUID
// ---------------------------------------------------------------------------

pub fn create_uuid_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct UuidUdf {
        signature: Signature,
    }

    impl UuidUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![], Volatility::Volatile),
            }
        }
    }

    impl ScalarUDFImpl for UuidUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "uuid"
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
            let n = args.number_rows.max(1);
            let mut counter = 0u64;

            let result: Vec<String> = (0..n)
                .map(|_| {
                    // UUID v4 with random components
                    let now = SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default();
                    let nanos = now.as_nanos();
                    let time_low = (nanos & 0xFFFF_FFFF) as u32;
                    let time_mid = ((nanos >> 32) & 0xFFFF) as u16;
                    let time_hi_and_version = ((nanos >> 48) as u16 & 0x0FFF) | 0x4000; // version 4
                    let pid = std::process::id() as u8;
                    let thread_id = {
                        use std::hash::{Hash, Hasher};
                        let mut h = std::collections::hash_map::DefaultHasher::new();
                        std::thread::current().id().hash(&mut h);
                        (h.finish() & 0xFF) as u8
                    };
                    let clock_seq_hi = ((pid ^ thread_id) & 0x3F) | 0x80; // variant
                    let clock_seq_lo = ((nanos >> 60) as u8).wrapping_mul(counter.wrapping_add(1) as u8);
                    // Use nanosecond precision + counter + thread hash for node to avoid collisions
                    let node_hash = {
                        use std::hash::{Hash, Hasher};
                        let mut h = std::collections::hash_map::DefaultHasher::new();
                        nanos.hash(&mut h);
                        counter.hash(&mut h);
                        std::thread::current().id().hash(&mut h);
                        h.finish()
                    };
                    let node_low = node_hash & 0xFFFF_FFFF_FFFF;

                    counter += 1;

                    format!(
                        "{:08x}-{:04x}-{:04x}-{:02x}{:02x}-{:012x}",
                        time_low,
                        time_mid,
                        time_hi_and_version,
                        clock_seq_hi,
                        clock_seq_lo,
                        node_low,
                    )
                })
                .collect();

            Ok(ColumnarValue::Array(
                Arc::new(StringArray::from(result)) as Arc<dyn Array>
            ))
        }
    }

    ScalarUDF::new_from_impl(UuidUdf::new())
}

// ---------------------------------------------------------------------------
// version() — Return MySQL version string
// ---------------------------------------------------------------------------

pub fn create_version_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct VersionUdf {
        signature: Signature,
    }

    impl VersionUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![], Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for VersionUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "version"
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
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "5.7.38-harness-0.3.0".to_string(),
            ))))
        }
    }

    ScalarUDF::new_from_impl(VersionUdf::new())
}

// ---------------------------------------------------------------------------
// database() — Return current database name
// ---------------------------------------------------------------------------

pub fn create_database_udf() -> ScalarUDF {
    #[derive(Debug)]
    struct DatabaseUdf {
        signature: Signature,
    }

    impl DatabaseUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![], Volatility::Stable),
            }
        }
    }

    impl ScalarUDFImpl for DatabaseUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "database"
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
            // TODO: UDFs don't have session context access.
            // Ideally this would return the current database from the session.
            // DataFusion may have a built-in current_schema() or similar;
            // if so, we could try to delegate to that. For now, return "default".
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "default".to_string(),
            ))))
        }
    }

    ScalarUDF::new_from_impl(DatabaseUdf::new())
}

// ---------------------------------------------------------------------------
// UDF Registration
// ---------------------------------------------------------------------------

/// Register all miscellaneous UDFs with a DataFusion context.
pub fn register_misc_udfs(ctx: &mut datafusion::prelude::SessionContext) {
    ctx.register_udf(create_hex_udf());
    ctx.register_udf(create_unhex_udf());
    ctx.register_udf(create_truncate_udf());
    ctx.register_udaf(create_group_concat_udf());
    ctx.register_udf(create_if_udf());
    ctx.register_udf(create_ifnull_udf());
    ctx.register_udf(create_uuid_udf());
    ctx.register_udf(create_version_udf());
    ctx.register_udf(create_database_udf());
}
