//! to_arrow module provices the convert functions from iceberg in-memory
//! schema to arrow schema.

use std::convert::TryFrom;
use std::sync::Arc;

use crate::types::in_memory;
use crate::types::in_memory::{Any, Schema};
use arrow_schema::ArrowError;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use arrow_schema::TimeUnit;

impl TryFrom<Schema> for ArrowSchema {
    type Error = ArrowError;

    fn try_from(value: Schema) -> Result<Self, Self::Error> {
        let fields = value
            .fields
            .into_iter()
            .map(ArrowField::try_from)
            .collect::<Result<Vec<ArrowField>, ArrowError>>()?;

        Ok(ArrowSchema::new(fields))
    }
}

impl TryFrom<in_memory::Field> for ArrowField {
    type Error = ArrowError;

    fn try_from(value: in_memory::Field) -> Result<Self, Self::Error> {
        Ok(ArrowField::new_dict(
            value.name,
            value.field_type.try_into()?,
            !value.required,
            value.id as i64,
            false,
        ))
    }
}

impl TryFrom<in_memory::Any> for ArrowDataType {
    type Error = ArrowError;

    fn try_from(value: in_memory::Any) -> Result<Self, Self::Error> {
        match value {
            Any::Primitive(v) => v.try_into(),
            Any::Struct(v) => {
                let mut fields = vec![];
                for f in v.fields {
                    fields.push(ArrowField::try_from(f)?);
                }
                Ok(ArrowDataType::Struct(fields.into()))
            }
            Any::List(v) => {
                let field = ArrowField::new_dict(
                    "item",
                    (*v.element_type).try_into()?,
                    !v.element_required,
                    v.element_id as i64,
                    false,
                );

                Ok(ArrowDataType::List(Arc::new(field)))
            }
            Any::Map(v) => {
                let field = ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new_dict(
                                "key",
                                (*v.key_type).try_into()?,
                                false,
                                v.key_id as i64,
                                false,
                            ),
                            ArrowField::new_dict(
                                "value",
                                (*v.value_type).try_into()?,
                                !v.value_required,
                                v.value_id as i64,
                                false,
                            ),
                        ]
                        .into(),
                    ),
                    v.value_required,
                );

                Ok(ArrowDataType::Map(Arc::new(field), false))
            }
        }
    }
}

impl TryFrom<in_memory::Primitive> for ArrowDataType {
    type Error = ArrowError;

    fn try_from(value: in_memory::Primitive) -> Result<Self, Self::Error> {
        match value {
            in_memory::Primitive::Boolean => Ok(ArrowDataType::Boolean),
            in_memory::Primitive::Int => Ok(ArrowDataType::Int32),
            in_memory::Primitive::Long => Ok(ArrowDataType::Int64),
            in_memory::Primitive::Float => Ok(ArrowDataType::Float32),
            in_memory::Primitive::Double => Ok(ArrowDataType::Float64),
            in_memory::Primitive::Decimal { precision, scale } => {
                Ok(ArrowDataType::Decimal128(precision, scale as i8))
            }
            in_memory::Primitive::Date => Ok(ArrowDataType::Date32),
            in_memory::Primitive::Time => Ok(ArrowDataType::Time32(TimeUnit::Microsecond)),
            in_memory::Primitive::Timestamp => {
                Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
            }
            in_memory::Primitive::Timestampz => {
                // Timestampz always stored as UTC
                Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
            }
            in_memory::Primitive::String => Ok(ArrowDataType::Utf8),
            in_memory::Primitive::Uuid => Ok(ArrowDataType::FixedSizeBinary(16)),
            in_memory::Primitive::Fixed(i) => {
                if i <= i32::MAX as u64 {
                    // FixedSizeBinary only supports up to i32::MAX bytes
                    Ok(ArrowDataType::FixedSizeBinary(i as i32))
                } else {
                    Ok(ArrowDataType::LargeBinary)
                }
            }
            in_memory::Primitive::Binary => Ok(ArrowDataType::LargeBinary),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::in_memory;

    #[test]
    fn test_try_into_arrow_schema() {
        let schema = Schema {
            fields: vec![
                in_memory::Field {
                    name: "id".to_string(),
                    field_type: in_memory::Any::Primitive(in_memory::Primitive::Long),
                    id: 0,
                    required: true,
                    comment: None,
                    initial_default: None,
                    write_default: None,
                },
                in_memory::Field {
                    name: "data".to_string(),
                    field_type: in_memory::Any::Primitive(in_memory::Primitive::String),
                    id: 1,
                    required: false,
                    comment: None,
                    initial_default: None,
                    write_default: None,
                },
            ],
            schema_id: 0,
            identifier_field_ids: None,
        };

        let arrow_schema = ArrowSchema::try_from(schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.fields()[0].name(), "id");
        assert_eq!(arrow_schema.fields()[0].data_type(), &ArrowDataType::Int64);
        assert_eq!(arrow_schema.fields()[1].name(), "data");
        assert_eq!(arrow_schema.fields()[1].data_type(), &ArrowDataType::Utf8);
    }
}
