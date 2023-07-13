//! Avro data types related functions.

use crate::error::Result;
use crate::types::in_memory::{Any, Field, Primitive, Schema};
use crate::{Error, ErrorKind};
use apache_avro::schema::{Name, RecordField as AvroRecordField, RecordFieldOrder, RecordSchema};
use apache_avro::Schema as AvroSchema;
use std::collections::BTreeMap;

impl<'a> TryFrom<&'a Schema> for AvroSchema {
    type Error = Error;

    fn try_from(value: &'a Schema) -> Result<AvroSchema> {
        let avro_fields: Vec<AvroRecordField> = value
            .fields
            .iter()
            .map(AvroRecordField::try_from)
            .collect::<Result<Vec<AvroRecordField>>>()?;
        Ok(AvroSchema::Record(RecordSchema {
            name: Name::from(format!("r_{}", value.schema_id).as_str()),
            aliases: None,
            doc: None,
            fields: avro_fields,
            lookup: BTreeMap::new(),
            attributes: BTreeMap::new(),
        }))
    }
}

impl<'a> TryFrom<&'a Field> for AvroRecordField {
    type Error = Error;

    fn try_from(value: &'a Field) -> Result<AvroRecordField> {
        let avro_schema = match &value.field_type {
            Any::Primitive(data_type) => match data_type {
                Primitive::Boolean => AvroSchema::Boolean,
                Primitive::Int => AvroSchema::Int,
                Primitive::Long => AvroSchema::Long,
                Primitive::Float => AvroSchema::Float,
                Primitive::Double => AvroSchema::Double,
                Primitive::Date => AvroSchema::Date,
                Primitive::Time => AvroSchema::TimeMicros,
                Primitive::Timestamp => AvroSchema::TimestampMicros,
                Primitive::Timestampz => AvroSchema::TimestampMicros,
                Primitive::String => AvroSchema::String,
                Primitive::Uuid => AvroSchema::Uuid,
                Primitive::Binary => AvroSchema::Bytes,
                _ => {
                    return Err(Error::new(
                        ErrorKind::IcebergFeatureUnsupported,
                        format!(
                            "Unable to convert iceberg data type {:?} to avro type",
                            data_type
                        ),
                    ))
                }
            },
            Any::Struct(s) => {
                let avro_fields: Vec<AvroRecordField> = s
                    .fields
                    .iter()
                    .map(AvroRecordField::try_from)
                    .collect::<Result<Vec<AvroRecordField>>>()?;
                AvroSchema::Record(RecordSchema {
                    name: Name::from(format!("r_{}", value.id).as_str()),
                    fields: avro_fields,
                    aliases: None,
                    doc: None,
                    lookup: BTreeMap::new(),
                    attributes: BTreeMap::new(),
                })
            }
            r#type => {
                return Err(Error::new(
                    ErrorKind::IcebergFeatureUnsupported,
                    format!(
                        "Unable to convert iceberg data type {:?} to avro type",
                        r#type
                    ),
                ))
            }
        };

        Ok(AvroRecordField {
            name: value.name.clone(),
            doc: value.comment.clone(),
            aliases: None,
            default: None,
            schema: avro_schema,
            order: RecordFieldOrder::Ignore,
            position: 0,
            custom_attributes: BTreeMap::default(),
        })
    }
}
