//! to_arrow module provices the convert functions from iceberg in-memory
//! schema to arrow schema.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::error::Error;
use crate::types;
use crate::types::Any;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Schema as ArrowSchema;
use arrow_schema::TimeUnit;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// The key of column id in the metadata of arrow field.
pub const COLUMN_ID_META_KEY: &str = "column_id";

impl TryFrom<types::Schema> for ArrowSchema {
    type Error = Error;

    fn try_from(value: types::Schema) -> Result<Self, Self::Error> {
        let fields = value
            .fields()
            .iter()
            .map(|field| ArrowField::try_from(field.as_ref().to_owned()))
            .collect::<Result<Vec<ArrowField>, Error>>()?;

        Ok(ArrowSchema::new(fields))
    }
}

impl TryFrom<types::Field> for ArrowField {
    type Error = Error;

    fn try_from(value: types::Field) -> Result<Self, Self::Error> {
        let mut metadata = HashMap::new();
        metadata.insert(COLUMN_ID_META_KEY.to_string(), value.id.to_string());
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), value.id.to_string());
        Ok(
            ArrowField::new(value.name, value.field_type.try_into()?, !value.required)
                .with_metadata(metadata),
        )
    }
}

impl TryFrom<types::Any> for ArrowDataType {
    type Error = Error;

    fn try_from(value: types::Any) -> Result<Self, Self::Error> {
        match value {
            Any::Primitive(v) => v.try_into(),
            Any::Struct(v) => {
                let mut fields = vec![];
                for f in v.fields() {
                    fields.push(ArrowField::try_from(f.as_ref().to_owned())?);
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

impl TryFrom<types::Primitive> for ArrowDataType {
    type Error = Error;

    fn try_from(value: types::Primitive) -> Result<Self, Self::Error> {
        match value {
            types::Primitive::Boolean => Ok(ArrowDataType::Boolean),
            types::Primitive::Int => Ok(ArrowDataType::Int32),
            types::Primitive::Long => Ok(ArrowDataType::Int64),
            types::Primitive::Float => Ok(ArrowDataType::Float32),
            types::Primitive::Double => Ok(ArrowDataType::Float64),
            types::Primitive::Decimal { precision, scale } => {
                Ok(ArrowDataType::Decimal128(precision, scale as i8))
            }
            types::Primitive::Date => Ok(ArrowDataType::Date32),
            types::Primitive::Time => Ok(ArrowDataType::Time32(TimeUnit::Microsecond)),
            types::Primitive::Timestamp => {
                Ok(ArrowDataType::Timestamp(TimeUnit::Microsecond, None))
            }
            types::Primitive::Timestampz => {
                // Timestampz always stored as UTC
                Ok(ArrowDataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("+00:00".into()),
                ))
            }
            types::Primitive::String => Ok(ArrowDataType::Utf8),
            types::Primitive::Uuid => Ok(ArrowDataType::FixedSizeBinary(16)),
            types::Primitive::Fixed(i) => {
                if i <= i32::MAX as u64 {
                    // FixedSizeBinary only supports up to i32::MAX bytes
                    Ok(ArrowDataType::FixedSizeBinary(i as i32))
                } else {
                    Ok(ArrowDataType::LargeBinary)
                }
            }
            types::Primitive::Binary => Ok(ArrowDataType::LargeBinary),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::Struct;

    use super::*;

    #[test]
    fn test_try_into_arrow_schema() {
        let schema = types::Schema::new(
            0,
            None,
            Struct::new(vec![
                types::Field {
                    name: "id".to_string(),
                    field_type: types::Any::Primitive(types::Primitive::Long),
                    id: 0,
                    required: true,
                    comment: None,
                    initial_default: None,
                    write_default: None,
                }
                .into(),
                types::Field {
                    name: "data".to_string(),
                    field_type: types::Any::Primitive(types::Primitive::String),
                    id: 1,
                    required: false,
                    comment: None,
                    initial_default: None,
                    write_default: None,
                }
                .into(),
            ]),
        );

        let arrow_schema = ArrowSchema::try_from(schema).unwrap();

        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(arrow_schema.fields()[0].name(), "id");
        assert_eq!(arrow_schema.fields()[0].data_type(), &ArrowDataType::Int64);
        assert_eq!(arrow_schema.fields()[1].name(), "data");
        assert_eq!(arrow_schema.fields()[1].data_type(), &ArrowDataType::Utf8);
    }
}
