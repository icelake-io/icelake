use super::to_primitive::ToPrimitiveValue;
use crate::types::PrimitiveValue;
use crate::types::{Any, AnyValue, StructValueBuilder};
use crate::{Error, Result};
use arrow_array::types::ArrowPrimitiveType;
use arrow_array::types::GenericStringType;
use arrow_array::{
    Array, Date32Array, Date64Array, Float32Array, Float64Array, GenericByteArray, Int16Array,
    Int32Array, Int64Array, Int8Array, OffsetSizeTrait, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_array::{BooleanArray, PrimitiveArray, StructArray};
use arrow_schema::{DataType, TimeUnit};
use std::iter::Iterator;

/// This trait is used to convert arrow array into anyvalue array. Most of the arrow arrays
/// implement this trait. Excepct the array which need to pre-compute the target type like
/// `StructArray`.
pub trait ToArray {
    /// Convert arrow array into anyvalue array.
    fn to_anyvalue_array(&self) -> Result<Vec<Option<AnyValue>>>;
}

impl ToArray for BooleanArray {
    fn to_anyvalue_array(&self) -> Result<Vec<Option<AnyValue>>> {
        Ok(self
            .iter()
            .map(|x| x.map(|x| AnyValue::Primitive(PrimitiveValue::Boolean(x))))
            .collect())
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
                    Ok(Some(AnyValue::Primitive(x.to_primitive(self.data_type())?)))
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<_>>()
    }
}

impl<T: OffsetSizeTrait> ToArray for GenericByteArray<GenericStringType<T>> {
    fn to_anyvalue_array(&self) -> Result<Vec<Option<AnyValue>>> {
        self.iter()
            .map(|x| {
                if let Some(x) = x {
                    Ok(Some(AnyValue::Primitive(PrimitiveValue::String(
                        x.to_string(),
                    ))))
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<_>>()
    }
}

/// We use the custom function to convert struct array to anyvalue array instead of using
/// `ToArray` beacsue we need to pre-compute the target type of the struct array. This can
/// save convert time and the memory cost in some case.
///
/// # NOTE
/// Caller should guarantee target type is match with type of array. It's order sensitive.
pub fn struct_to_anyvalue_array_with_type(
    struct_array: &StructArray,
    target_type: Any,
) -> Result<Vec<Option<AnyValue>>> {
    let row_num = struct_array.len();
    if let Any::Struct(target_struct) = target_type {
        let arrays = struct_array.columns();

        let mut arrays = arrays
            .iter()
            .zip(target_struct.fields().iter())
            .map(|(array, target_field)| {
                if target_field.field_type != array.data_type().clone().try_into()? {
                    return Err(Error::new(crate::ErrorKind::DataTypeUnsupported,format!("target_type {:?} is not match with array type {}. You should guarantee the target_type is the same with array type (including order).",target_struct,struct_array.data_type())));
                }
                Ok(
                    to_anyvalue_array_with_type(&array, target_field.field_type.clone())?
                        .into_iter(),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let mut null_iter = struct_array.nulls().map(|null_buf| null_buf.into_iter());
        let mut res = Vec::with_capacity(row_num);
        for _ in 0..row_num {
            if let Some(null_iter) = null_iter.as_mut() {
                // return false if the value is null
                if !null_iter.next().unwrap() {
                    res.push(None);
                    continue;
                }
            }
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

/// Convert an arrow array to an anyvalue array.
pub fn to_anyvalue_array_with_type(
    array: &dyn Array,
    target_type: Any,
) -> Result<Vec<Option<AnyValue>>> {
    let data_type = array.data_type();
    match data_type {
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::Struct(_) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            struct_to_anyvalue_array_with_type(array, target_type)
        }
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::Float16 => todo!(),
        DataType::Null => todo!(),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => array
                .as_any()
                .downcast_ref::<arrow_array::TimestampSecondArray>()
                .unwrap()
                .to_anyvalue_array(),
            TimeUnit::Millisecond => array
                .as_any()
                .downcast_ref::<arrow_array::TimestampMillisecondArray>()
                .unwrap()
                .to_anyvalue_array(),
            TimeUnit::Microsecond => array
                .as_any()
                .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
                .unwrap()
                .to_anyvalue_array(),
            TimeUnit::Nanosecond => array
                .as_any()
                .downcast_ref::<arrow_array::TimestampNanosecondArray>()
                .unwrap()
                .to_anyvalue_array(),
        },
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => array
                .as_any()
                .downcast_ref::<arrow_array::Time32SecondArray>()
                .unwrap()
                .to_anyvalue_array(),
            TimeUnit::Millisecond => array
                .as_any()
                .downcast_ref::<arrow_array::Time32MillisecondArray>()
                .unwrap()
                .to_anyvalue_array(),
            TimeUnit::Microsecond => Err(Error::new(
                crate::ErrorKind::DataTypeUnsupported,
                "Time32Microsecond is not supported",
            )),
            TimeUnit::Nanosecond => Err(Error::new(
                crate::ErrorKind::DataTypeUnsupported,
                "Time32Nanosecond is not supported",
            )),
        },
        DataType::Time64(_) => todo!(),
        DataType::Duration(_) => todo!(),
        DataType::Interval(_) => todo!(),
        DataType::FixedSizeBinary(_) => todo!(),
        DataType::List(_) => todo!(),
        DataType::FixedSizeList(_, _) => todo!(),
        DataType::LargeList(_) => todo!(),
        DataType::Union(_, _) => todo!(),
        DataType::Dictionary(_, _) => todo!(),
        DataType::Decimal128(_, _) => todo!(),
        DataType::Decimal256(_, _) => todo!(),
        DataType::Map(_, _) => todo!(),
        DataType::RunEndEncoded(_, _) => todo!(),
        DataType::Binary => todo!(),
        DataType::LargeBinary => todo!(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .to_anyvalue_array(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<arrow_array::LargeStringArray>()
            .unwrap()
            .to_anyvalue_array(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Field;
    use crate::types::Primitive;
    use crate::types::Struct;
    use crate::types::{arrow::from_arrow::to_array::ToArray, AnyValue, PrimitiveValue};
    use arrow_schema::DataType as ArrowDataType;
    use arrow_schema::Field as ArrowField;
    use arrow_schema::Fields as ArrowFields;
    use std::sync::Arc;
    #[test]
    fn test_from_bool_array() {
        let array = arrow_array::BooleanArray::from(vec![Some(true), None, Some(false)]);
        let expect: Vec<Option<AnyValue>> = vec![
            Some(PrimitiveValue::Boolean(true).into()),
            None,
            Some(PrimitiveValue::Boolean(false).into()),
        ];
        assert_eq!(array.to_anyvalue_array().unwrap(), expect);
    }

    #[test]
    fn test_from_primitive_array() {
        let array = arrow_array::Int32Array::from(vec![Some(1), None, Some(3)]);
        let expect: Vec<Option<AnyValue>> = vec![
            Some(PrimitiveValue::Int(1).into()),
            None,
            Some(PrimitiveValue::Int(3).into()),
        ];
        assert_eq!(array.to_anyvalue_array().unwrap(), expect);
    }

    #[test]
    fn test_from_simple_struct_array() {
        // construct a arrow struct array
        let fields: ArrowFields = vec![
            Arc::new(ArrowField::new("b", ArrowDataType::Boolean, false)),
            Arc::new(ArrowField::new("c", ArrowDataType::Int32, false)),
        ]
        .into();
        let boolean = Arc::new(BooleanArray::from(vec![false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 28]));
        let struct_array = StructArray::new(
            fields.clone(),
            vec![boolean, int],
            Some(vec![true, false, true].into()),
        );

        // construct a anyvalue struct array
        let struct_ty = Arc::new(Struct::new(vec![
            Field::required(0, "b", Primitive::Boolean.into()).into(),
            Field::required(1, "c", Primitive::Int.into()).into(),
        ]));
        let mut expect: Vec<Option<AnyValue>> = Vec::with_capacity(4);
        let mut struct_builder = StructValueBuilder::new(struct_ty.clone());
        struct_builder
            .add_field(0, Some(PrimitiveValue::Boolean(false).into()))
            .unwrap();
        struct_builder
            .add_field(1, Some(PrimitiveValue::Int(42).into()))
            .unwrap();
        expect.push(Some(struct_builder.build().unwrap().into()));
        expect.push(None);
        let mut struct_builder = StructValueBuilder::new(struct_ty);
        struct_builder
            .add_field(0, Some(PrimitiveValue::Boolean(true).into()))
            .unwrap();
        struct_builder
            .add_field(1, Some(PrimitiveValue::Int(28).into()))
            .unwrap();
        expect.push(Some(struct_builder.build().unwrap().into()));

        let struct_ty: Any = ArrowDataType::Struct(fields).try_into().unwrap();
        assert_eq!(
            struct_to_anyvalue_array_with_type(&struct_array, struct_ty).unwrap(),
            expect
        );
    }

    #[test]
    fn test_from_nested_struct_array() {
        // this test test a struct with type like
        // struct {
        //      struct {
        //          bool,
        //          int
        //      }
        //      struct {
        //          bool,
        //          int
        //      }
        // }
        // construct a arrow struct array.
        let sub_fields: ArrowFields = vec![
            Arc::new(ArrowField::new("c", ArrowDataType::Boolean, false)),
            Arc::new(ArrowField::new("d", ArrowDataType::Int32, false)),
        ]
        .into();
        let fields: ArrowFields = vec![
            Arc::new(ArrowField::new(
                "a",
                ArrowDataType::Struct(sub_fields.clone()),
                false,
            )),
            Arc::new(ArrowField::new(
                "b",
                ArrowDataType::Struct(sub_fields.clone()),
                false,
            )),
        ]
        .into();
        let boolean = Arc::new(BooleanArray::from(vec![false, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28]));
        let struct_array: Arc<_> = StructArray::new(sub_fields, vec![boolean, int], None).into();
        let struct_array = StructArray::new(
            fields.clone(),
            vec![struct_array.clone(), struct_array],
            None,
        );

        // construct the expect any value struct array.
        let struct_sub_ty = Arc::new(Struct::new(vec![
            Field::required(0, "c", Primitive::Boolean.into()).into(),
            Field::required(1, "d", Primitive::Int.into()).into(),
        ]));
        let struct_ty = Arc::new(Struct::new(vec![
            Field::required(0, "a", Any::Struct(struct_sub_ty.clone())).into(),
            Field::required(1, "b", Any::Struct(struct_sub_ty.clone())).into(),
        ]));
        let mut expect: Vec<Option<AnyValue>> = Vec::with_capacity(4);
        let mut struct_builder = StructValueBuilder::new(struct_ty.clone());
        let mut sub_struct_builder = StructValueBuilder::new(struct_sub_ty.clone());
        sub_struct_builder
            .add_field(0, Some(PrimitiveValue::Boolean(false).into()))
            .unwrap();
        sub_struct_builder
            .add_field(1, Some(PrimitiveValue::Int(42).into()))
            .unwrap();
        let sub_struct_value = sub_struct_builder.build().unwrap();
        struct_builder
            .add_field(0, Some(sub_struct_value.clone().into()))
            .unwrap();
        struct_builder
            .add_field(1, Some(sub_struct_value.into()))
            .unwrap();
        expect.push(Some(struct_builder.build().unwrap().into()));

        let mut struct_builder = StructValueBuilder::new(struct_ty);
        let mut sub_struct_builder = StructValueBuilder::new(struct_sub_ty);
        sub_struct_builder
            .add_field(0, Some(PrimitiveValue::Boolean(true).into()))
            .unwrap();
        sub_struct_builder
            .add_field(1, Some(PrimitiveValue::Int(28).into()))
            .unwrap();
        let sub_struct_value = sub_struct_builder.build().unwrap();
        struct_builder
            .add_field(0, Some(sub_struct_value.clone().into()))
            .unwrap();
        struct_builder
            .add_field(1, Some(sub_struct_value.into()))
            .unwrap();
        expect.push(Some(struct_builder.build().unwrap().into()));

        let struct_ty: Any = ArrowDataType::Struct(fields).try_into().unwrap();
        assert_eq!(
            struct_to_anyvalue_array_with_type(&struct_array, struct_ty).unwrap(),
            expect
        );
    }
}
