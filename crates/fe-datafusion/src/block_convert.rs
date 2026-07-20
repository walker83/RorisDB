// Block ↔ Arrow RecordBatch conversion utilities.
//
// HarnessDB has its own type system (types::Vector / types::Block).
// DataFusion works with Arrow (RecordBatch / Array).
// This module bridges the two.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::array::*;
use arrow_schema::{DataType as ArrowDataType, Field};

use types::vector::*;
use types::Block;

// ---------------------------------------------------------------------------
// Block → RecordBatch  (IMPLEMENTED)
// ---------------------------------------------------------------------------

/// Convert a HarnessDB `Block` into an Arrow `RecordBatch`.
pub fn block_to_record_batch(block: &Block) -> Result<RecordBatch, String> {
    let schema = block.schema();
    let arrow_fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            Field::new(
                &f.name,
                crate::types::to_arrow_data_type(&f.data_type),
                f.nullable,
            )
        })
        .collect();
    let arrow_schema = Arc::new(arrow_schema::Schema::new(arrow_fields));

    let columns: Vec<Arc<dyn arrow_array::Array>> = block
        .columns()
        .iter()
        .map(|col| vector_to_array(col))
        .collect::<Result<_, _>>()?;

    RecordBatch::try_new(arrow_schema, columns)
        .map_err(|e| format!("RecordBatch build failed: {}", e))
}

fn vector_to_array(vec: &Vector) -> Result<Arc<dyn arrow_array::Array>, String> {
    match vec {
        Vector::Boolean(v) => {
            let iter = (0..v.len()).map(|i| v.get(i));
            Ok(Arc::new(BooleanArray::from_iter(iter)))
        }
        Vector::Int8(v) => {
            let iter = (0..v.len()).map(|i| v.get(i));
            Ok(Arc::new(Int8Array::from_iter(iter)))
        }
        Vector::Int16(v) => {
            let iter = (0..v.len()).map(|i| v.get(i));
            Ok(Arc::new(Int16Array::from_iter(iter)))
        }
        Vector::Int32(v) => {
            let iter = (0..v.len()).map(|i| v.get(i));
            Ok(Arc::new(Int32Array::from_iter(iter)))
        }
        Vector::Int64(v) => {
            let iter = (0..v.len()).map(|i| v.get(i));
            Ok(Arc::new(Int64Array::from_iter(iter)))
        }
        Vector::Float32(v) => {
            let iter = (0..v.len()).map(|i| v.get(i));
            Ok(Arc::new(Float32Array::from_iter(iter)))
        }
        Vector::Float64(v) => {
            let iter = (0..v.len()).map(|i| v.get(i));
            Ok(Arc::new(Float64Array::from_iter(iter)))
        }
        Vector::String(v) => {
            // StringVector::get(i) returns Option<&str>
            let iter = (0..v.len()).map(|i| v.get(i).map(|s| s.to_string()));
            Ok(Arc::new(StringArray::from_iter(iter)))
        }
        Vector::Date(v) => {
            let iter = (0..v.len()).map(|i| v.get(i));
            Ok(Arc::new(Date32Array::from_iter(iter)))
        }
        Vector::DateTime(v) => {
            let iter = (0..v.len()).map(|i| v.get(i));
            Ok(Arc::new(TimestampSecondArray::from_iter(iter)))
        }
        Vector::Null(_) => {
            let len = vec.len();
            Ok(Arc::new(NullArray::new(len)))
        }
        _ => Err(format!(
            "Unsupported vector type for Arrow conversion: {:?}",
            vec
        )),
    }
}

// ---------------------------------------------------------------------------
// RecordBatch → Block
// ---------------------------------------------------------------------------

pub fn record_batch_to_block(rb: &RecordBatch) -> Result<Block, String> {
    
    use types::{Field, Schema};

    // Build schema from RecordBatch schema
    let mut fields = Vec::new();
    let mut columns = Vec::new();

    let arrow_schema = rb.schema();

    for (col_idx, array) in rb.columns().iter().enumerate() {
        let f = arrow_schema.field(col_idx);
        let name = f.name().clone();
        let is_nullable = f.is_nullable();
        let arrow_dt = f.data_type();

        let data_type = convert_arrow_data_type(arrow_dt);

        fields.push(Field {
            name: name.clone(),
            data_type: data_type.clone(),
            nullable: is_nullable,
        });

        // Convert array to Vector
        let vector = convert_array_to_vector_by_type(array, arrow_dt)?;
        columns.push(vector);
    }

    let schema = Schema::new(fields);
    Ok(Block::new(schema, columns))
}

fn convert_arrow_data_type(dt: &ArrowDataType) -> types::DataType {
    match dt {
        ArrowDataType::Boolean => types::DataType::Boolean,
        ArrowDataType::Int8 => types::DataType::Int8,
        ArrowDataType::Int16 => types::DataType::Int16,
        ArrowDataType::Int32 => types::DataType::Int32,
        ArrowDataType::Int64 => types::DataType::Int64,
        ArrowDataType::UInt8 => types::DataType::Int16,
        ArrowDataType::UInt16 => types::DataType::Int32,
        ArrowDataType::UInt32 => types::DataType::Int64,
        ArrowDataType::UInt64 => types::DataType::Int64,
        ArrowDataType::Float32 => types::DataType::Float32,
        ArrowDataType::Float64 => types::DataType::Float64,
        ArrowDataType::Utf8 => types::DataType::String,
        ArrowDataType::LargeUtf8 => types::DataType::String,
        ArrowDataType::Date32 => types::DataType::Date,
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, None) => types::DataType::DateTime,
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None) => {
            types::DataType::DateTime
        }
        ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
            types::DataType::DateTime
        }
        _ => types::DataType::String, // fallback
    }
}

#[allow(dead_code)]
fn convert_array_to_vector(
    array: &Arc<dyn Array>,
    data_type: &types::DataType,
) -> Result<Vector, String> {
    use types::{Vector, vector::*};

    Ok(match data_type {
        types::DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("Not BooleanArray")?;
            let data: Vec<Option<bool>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Boolean(BooleanVector::from_nullable_vec(data))
        }
        types::DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or("Not Int8Array")?;
            let data: Vec<Option<i8>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Int8(Int8Vector::from_nullable_vec(data))
        }
        types::DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or("Not Int16Array")?;
            let data: Vec<Option<i16>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Int16(Int16Vector::from_nullable_vec(data))
        }
        types::DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("Not Int32Array")?;
            let data: Vec<Option<i32>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Int32(Int32Vector::from_nullable_vec(data))
        }
        types::DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("Not Int64Array")?;
            let data: Vec<Option<i64>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Int64(Int64Vector::from_nullable_vec(data))
        }
        types::DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("Not Float32Array")?;
            let data: Vec<Option<f32>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Float32(Float32Vector::from_nullable_vec(data))
        }
        types::DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Not Float64Array")?;
            let data: Vec<Option<f64>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Float64(Float64Vector::from_nullable_vec(data))
        }
        types::DataType::String => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Not StringArray")?;
            let data: Vec<Option<String>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i).to_string()))
                .collect();
            Vector::String(StringVector::from_nullable_vec(data))
        }
        types::DataType::Date => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or("Not Date32Array")?;
            let data: Vec<Option<i32>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Date(DateVector::from_nullable_vec(data))
        }
        types::DataType::DateTime => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or("Not TimestampSecondArray")?;
            let data: Vec<Option<i64>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::DateTime(DateTimeVector::from_nullable_vec(data))
        }
        _ => {
            // Fallback: extract individual elements as strings via ScalarValue
            use datafusion::scalar::ScalarValue;
            let data: Vec<Option<String>> = (0..array.len())
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        ScalarValue::try_from_array(array, i)
                            .ok()
                            .map(|s| format!("{:?}", s))
                    }
                })
                .collect();
            Vector::String(StringVector::from_nullable_vec(data))
        }
    })
}

// ---------------------------------------------------------------------------
// Arrow Array → Vector  (STUB — implement when needed)
// ---------------------------------------------------------------------------

pub fn array_to_vector(_array: &Arc<dyn arrow_array::Array>) -> Result<Vector, String> {
    // TODO: implement downcast logic when needed
    Err("array_to_vector not yet implemented".to_string())
}

fn convert_array_to_vector_by_type(
    array: &Arc<dyn Array>,
    arrow_dt: &arrow_schema::DataType,
) -> Result<Vector, String> {
    use arrow_schema::DataType as ArrowDataType;
    use arrow_schema::TimeUnit;
    use types::{Vector, vector::*};

    Ok(match arrow_dt {
        ArrowDataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("Not BooleanArray")?;
            let data: Vec<Option<bool>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Boolean(BooleanVector::from_nullable_vec(data))
        }
        ArrowDataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or("Not Int8Array")?;
            let data: Vec<Option<i8>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Int8(Int8Vector::from_nullable_vec(data))
        }
        ArrowDataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or("Not Int16Array")?;
            let data: Vec<Option<i16>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Int16(Int16Vector::from_nullable_vec(data))
        }
        ArrowDataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("Not Int32Array")?;
            let data: Vec<Option<i32>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Int32(Int32Vector::from_nullable_vec(data))
        }
        ArrowDataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("Not Int64Array")?;
            let data: Vec<Option<i64>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Int64(Int64Vector::from_nullable_vec(data))
        }
        ArrowDataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("Not Float32Array")?;
            let data: Vec<Option<f32>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Float32(Float32Vector::from_nullable_vec(data))
        }
        ArrowDataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Not Float64Array")?;
            let data: Vec<Option<f64>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Float64(Float64Vector::from_nullable_vec(data))
        }
        ArrowDataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Not StringArray")?;
            let data: Vec<Option<String>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i).to_string()))
                .collect();
            Vector::String(StringVector::from_nullable_vec(data))
        }
        ArrowDataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or("Not LargeStringArray")?;
            let data: Vec<Option<String>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i).to_string()))
                .collect();
            Vector::String(StringVector::from_nullable_vec(data))
        }
        ArrowDataType::UInt8 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or("Not UInt8Array")?;
            let data: Vec<Option<i16>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i16))
                .collect();
            Vector::Int16(Int16Vector::from_nullable_vec(data))
        }
        ArrowDataType::UInt16 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or("Not UInt16Array")?;
            let data: Vec<Option<i32>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i32))
                .collect();
            Vector::Int32(Int32Vector::from_nullable_vec(data))
        }
        ArrowDataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or("Not UInt32Array")?;
            let data: Vec<Option<i64>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i) as i64))
                .collect();
            Vector::Int64(Int64Vector::from_nullable_vec(data))
        }
        ArrowDataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("Not UInt64Array")?;
            let data: Vec<Option<i64>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(i64::try_from(arr.value(i)).unwrap_or(i64::MAX))
                    }
                })
                .collect();
            Vector::Int64(Int64Vector::from_nullable_vec(data))
        }
        ArrowDataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or("Not Date32Array")?;
            let data: Vec<Option<i32>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::Date(DateVector::from_nullable_vec(data))
        }
        ArrowDataType::Timestamp(TimeUnit::Second, None) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or("Not TimestampSecondArray")?;
            let data: Vec<Option<i64>> = (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| arr.value(i)))
                .collect();
            Vector::DateTime(DateTimeVector::from_nullable_vec(data))
        }
        _ => {
            // Fallback: extract individual elements as strings via ScalarValue
            use datafusion::scalar::ScalarValue;
            let data: Vec<Option<String>> = (0..array.len())
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        ScalarValue::try_from_array(array, i)
                            .ok()
                            .map(|s| format!("{:?}", s))
                    }
                })
                .collect();
            Vector::String(StringVector::from_nullable_vec(data))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn test_uint64_overflow_clamped_to_i64_max() {
        // Values > i64::MAX should be clamped to i64::MAX, not wrap/panic
        let schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::UInt64,
            true,
        )]));
        let arr = UInt64Array::from(vec![
            Some(100u64),
            Some(u64::MAX),
            Some(i64::MAX as u64),
            Some(i64::MAX as u64 + 1),
            None,
        ]);
        let rb = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let block = record_batch_to_block(&rb).unwrap();

        let col = block.column(0).unwrap();
        if let Vector::Int64(v) = col {
            assert_eq!(v.get(0), Some(100i64));
            assert_eq!(v.get(1), Some(i64::MAX));
            assert_eq!(v.get(2), Some(i64::MAX));
            assert_eq!(v.get(3), Some(i64::MAX));
            assert_eq!(v.get(4), None);
        } else {
            panic!("Expected Int64Vector");
        }
    }

    #[test]
    fn test_uint64_normal_values_pass_through() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "u",
            DataType::UInt64,
            false,
        )]));
        let arr = UInt64Array::from(vec![0u64, 1, 42, 1000]);
        let rb = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        let block = record_batch_to_block(&rb).unwrap();

        let col = block.column(0).unwrap();
        if let Vector::Int64(v) = col {
            assert_eq!(v.get(0), Some(0i64));
            assert_eq!(v.get(1), Some(1i64));
            assert_eq!(v.get(2), Some(42i64));
            assert_eq!(v.get(3), Some(1000i64));
        } else {
            panic!("Expected Int64Vector");
        }
    }
}
