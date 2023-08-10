use crate::types;
use crate::types::Any;
use crate::types::Field;
use crate::Error;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::TimeUnit;

impl TryFrom<ArrowDataType> for Any {
    type Error = Error;

    fn try_from(value: ArrowDataType) -> Result<Self, Self::Error> {
        let res: Any = match value {
            ArrowDataType::Boolean => types::Primitive::Boolean.into(),
            ArrowDataType::Int32 => types::Primitive::Int.into(),
            ArrowDataType::Int64 => types::Primitive::Long.into(),
            ArrowDataType::Float32 => types::Primitive::Float.into(),
            ArrowDataType::Float64 => types::Primitive::Double.into(),
            ArrowDataType::Decimal128(precision, scale) => types::Primitive::Decimal {
                precision,
                scale: scale as u8,
            }
            .into(),
            ArrowDataType::Date32 => types::Primitive::Date.into(),
            ArrowDataType::Time32(TimeUnit::Microsecond) => types::Primitive::Time.into(),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None) => {
                types::Primitive::Timestamp.into()
            }
            ArrowDataType::Utf8 => types::Primitive::String.into(),
            ArrowDataType::FixedSizeBinary(16) => types::Primitive::Uuid.into(),
            ArrowDataType::FixedSizeBinary(i) => types::Primitive::Fixed(i as u64).into(),
            ArrowDataType::LargeBinary => types::Primitive::Binary.into(),
            ArrowDataType::Struct(fields) => {
                let fields = fields
                    .into_iter()
                    .enumerate()
                    .map(|(id, field)| {
                        if field.is_nullable() {
                            Ok(Field::optional(
                                id as i32,
                                field.name(),
                                field.data_type().clone().try_into()?,
                            ))
                        } else {
                            Ok(Field::required(
                                id as i32,
                                field.name(),
                                field.data_type().clone().try_into()?,
                            ))
                        }
                    })
                    .collect::<Result<Vec<_>, Self::Error>>()?;
                Any::Struct(types::Struct::new(fields).into())
            }
            _ => {
                return Err(Error::new(
                    crate::ErrorKind::ArrowUnsupported,
                    format!("Unsupported convert arrow type: {:?} to Any", value),
                ));
            }
        };
        Ok(res)
    }
}
