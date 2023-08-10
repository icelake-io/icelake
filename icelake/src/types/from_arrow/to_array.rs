use super::to_primitive::ToPrimitiveValue;
use crate::types::PrimitiveValue;
use crate::types::{Any, AnyValue, StructValueBuilder};
use crate::Result;
use arrow::array::{
    Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::{
    array::{ArrayRef, BooleanArray, PrimitiveArray, StructArray},
    datatypes::ArrowPrimitiveType,
};
use std::iter::Iterator;
pub trait ToArray {
    fn to_anyvalue_array(&self) -> Result<Vec<Option<AnyValue>>>;

    /// This interface is used to convert the struct array. We pass he target type instead of
    /// converting it internally is to save the extra cost.
    fn to_anyvalue_array_with_type(&self, target_type: Any) -> Result<Vec<Option<AnyValue>>>;
}

impl ToArray for BooleanArray {
    fn to_anyvalue_array(&self) -> Result<Vec<Option<AnyValue>>> {
        Ok(self
            .iter()
            .map(|x| x.map(|x| AnyValue::Primitive(PrimitiveValue::Boolean(x))))
            .collect())
    }

    fn to_anyvalue_array_with_type(&self, _target_type: Any) -> Result<Vec<Option<AnyValue>>> {
        unimplemented!()
    }
}

impl<T: ArrowPrimitiveType> ToArray for PrimitiveArray<T>
where
    T::Native: ToPrimitiveValue,
{
    fn to_anyvalue_array(&self) -> Result<Vec<Option<AnyValue>>> {
        self.into_iter()
            .map(|x| {
                if let Some(x) = x {
                    Ok(Some(AnyValue::Primitive(x.to_primitive(T::DATA_TYPE)?)))
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<_>>()
    }

    fn to_anyvalue_array_with_type(&self, _target_type: Any) -> Result<Vec<Option<AnyValue>>> {
        unimplemented!()
    }
}

impl ToArray for StructArray {
    fn to_anyvalue_array(&self) -> Result<Vec<Option<AnyValue>>> {
        unimplemented!()
    }

    fn to_anyvalue_array_with_type(&self, target_type: Any) -> Result<Vec<Option<AnyValue>>> {
        let row_num = self.len();
        if let Any::Struct(target_struct) = target_type {
            let arrays = self.columns();

            let mut arrays = arrays
                .iter()
                .zip(target_struct.fields().iter())
                .map(|(array, target_field)| {
                    Ok(array
                        .to_anyvalue_array_with_type(target_field.field_type.clone())?
                        .into_iter())
                })
                .collect::<Result<Vec<_>>>()?;

            let mut res = Vec::with_capacity(row_num);
            for _ in 0..row_num {
                let mut builder = StructValueBuilder::new(target_struct.clone());
                for (field, array_iter) in target_struct.fields().iter().zip(arrays.iter_mut()) {
                    builder.add_field(field.id, array_iter.next().unwrap())?;
                }
                res.push(Some(builder.build()?.into()));
            }
            Ok(res)
        } else {
            unreachable!()
        }
    }
}

impl ToArray for ArrayRef {
    fn to_anyvalue_array(&self) -> Result<Vec<Option<AnyValue>>> {
        unimplemented!("Must call `to_anyvalue_array_with_type` instead")
    }

    fn to_anyvalue_array_with_type(&self, target_type: Any) -> Result<Vec<Option<AnyValue>>> {
        let data_type = self.data_type();
        match *data_type {
            arrow::datatypes::DataType::Null => todo!(),
            arrow::datatypes::DataType::Boolean => self
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .to_anyvalue_array(),
            arrow::datatypes::DataType::Struct(_) => self
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .to_anyvalue_array_with_type(target_type),
            arrow::datatypes::DataType::Int8 => self
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .to_anyvalue_array(),
            arrow::datatypes::DataType::Int16 => self
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .to_anyvalue_array(),
            arrow::datatypes::DataType::Int32 => self
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .to_anyvalue_array(),
            arrow::datatypes::DataType::Int64 => self
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .to_anyvalue_array(),
            arrow::datatypes::DataType::UInt8 => self
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .to_anyvalue_array(),
            arrow::datatypes::DataType::UInt16 => self
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .to_anyvalue_array(),
            arrow::datatypes::DataType::UInt32 => self
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .to_anyvalue_array(),
            arrow::datatypes::DataType::UInt64 => self
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .to_anyvalue_array(),
            arrow::datatypes::DataType::Float16 => todo!(),
            arrow::datatypes::DataType::Float32 => self
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .to_anyvalue_array(),
            arrow::datatypes::DataType::Float64 => self
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .to_anyvalue_array(),
            arrow::datatypes::DataType::Timestamp(_, _) => todo!(),
            arrow::datatypes::DataType::Date32 => todo!(),
            arrow::datatypes::DataType::Date64 => todo!(),
            arrow::datatypes::DataType::Time32(_) => todo!(),
            arrow::datatypes::DataType::Time64(_) => todo!(),
            arrow::datatypes::DataType::Duration(_) => todo!(),
            arrow::datatypes::DataType::Interval(_) => todo!(),
            arrow::datatypes::DataType::Binary => todo!(),
            arrow::datatypes::DataType::FixedSizeBinary(_) => todo!(),
            arrow::datatypes::DataType::LargeBinary => todo!(),
            arrow::datatypes::DataType::Utf8 => todo!(),
            arrow::datatypes::DataType::LargeUtf8 => todo!(),
            arrow::datatypes::DataType::List(_) => todo!(),
            arrow::datatypes::DataType::FixedSizeList(_, _) => todo!(),
            arrow::datatypes::DataType::LargeList(_) => todo!(),
            arrow::datatypes::DataType::Union(_, _) => todo!(),
            arrow::datatypes::DataType::Dictionary(_, _) => todo!(),
            arrow::datatypes::DataType::Decimal128(_, _) => todo!(),
            arrow::datatypes::DataType::Decimal256(_, _) => todo!(),
            arrow::datatypes::DataType::Map(_, _) => todo!(),
            arrow::datatypes::DataType::RunEndEncoded(_, _) => todo!(),
        }
    }
}
