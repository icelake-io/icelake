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
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
                types::Primitive::Timestampz.into()
            }
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => types::Primitive::String.into(),
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
                    crate::ErrorKind::DataTypeUnsupported,
                    format!("Unsupported convert arrow type: {:?} to Any", value),
                ));
            }
        };
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field as ArrowField;
    #[test]
    fn test_simple_try_into_any() {
        let arrow_struct = ArrowDataType::Struct(
            vec![
                ArrowField::new("a", ArrowDataType::Int32, true),
                ArrowField::new("b", ArrowDataType::Utf8, false),
            ]
            .into(),
        );

        let expect_any_struct = Any::Struct(
            types::Struct::new(vec![
                Field::optional(0, "a", types::Primitive::Int.into()),
                Field::required(1, "b", types::Primitive::String.into()),
            ])
            .into(),
        );

        assert_eq!(expect_any_struct, arrow_struct.try_into().unwrap());
    }

    #[test]
    fn test_nest_try_into_any() {
        let arrow_struct = ArrowDataType::Struct(
            vec![
                ArrowField::new("a", ArrowDataType::Int32, true),
                ArrowField::new("b", ArrowDataType::Utf8, false),
                ArrowField::new(
                    "c",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new("d", ArrowDataType::Int32, true),
                            ArrowField::new("e", ArrowDataType::Utf8, false),
                        ]
                        .into(),
                    ),
                    false,
                ),
            ]
            .into(),
        );

        let expect_any_struct = Any::Struct(
            types::Struct::new(vec![
                Field::optional(0, "a", types::Primitive::Int.into()),
                Field::required(1, "b", types::Primitive::String.into()),
                Field::required(
                    2,
                    "c",
                    Any::Struct(
                        types::Struct::new(vec![
                            Field::optional(0, "d", types::Primitive::Int.into()),
                            Field::required(1, "e", types::Primitive::String.into()),
                        ])
                        .into(),
                    ),
                ),
            ])
            .into(),
        );

        assert_eq!(expect_any_struct, arrow_struct.try_into().unwrap());
    }
}
