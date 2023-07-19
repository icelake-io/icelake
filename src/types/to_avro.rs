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
        let mut avro_schema = AvroSchema::try_from(&value.field_type)?;
        // An ugly workaround, let's fix it later.
        if let Any::Struct(_) = &value.field_type {
            match &mut avro_schema {
                AvroSchema::Record(r) => {
                    r.name = Name::from(value.name.as_str());
                    r.doc = value.comment.clone();
                }
                _ => panic!("Struct record should be converted to avro struct schema."),
            }
        }

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

impl<'a> TryFrom<&'a Any> for AvroSchema {
    type Error = Error;

    fn try_from(value: &'a Any) -> Result<AvroSchema> {
        let avro_schema = match &value {
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
            Any::Map(map) => {
                if *map.key_type != Any::Primitive(Primitive::String) {
                    return Err(Error::new(
                        ErrorKind::IcebergFeatureUnsupported,
                        format!(
                            "Unable to convert iceberg data type map with key type {:?} to avro type, since avro assumes keys are always strings",
                            *map.key_type
                        ),
                    ));
                }
                AvroSchema::Map(Box::new(AvroSchema::try_from(&*map.value_type)?))
            }
            Any::List(list) => {
                AvroSchema::Array(Box::new(AvroSchema::try_from(&*list.element_type)?))
            }
            Any::Struct(s) => {
                let avro_fields: Vec<AvroRecordField> = s
                    .fields
                    .iter()
                    .map(AvroRecordField::try_from)
                    .collect::<Result<Vec<AvroRecordField>>>()?;
                AvroSchema::Record(RecordSchema {
                    name: Name::from("invalid_name"),
                    fields: avro_fields,
                    aliases: None,
                    doc: None,
                    lookup: BTreeMap::new(),
                    attributes: BTreeMap::new(),
                })
            }
        };
        Ok(avro_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{List, Map, Struct};

    #[test]
    fn test_convert_to_avro() {
        let schema = Schema {
            schema_id: 0,
            identifier_field_ids: None,
            fields: vec![
                Field {
                    id: 1,
                    name: "a".to_string(),
                    required: true,
                    field_type: Any::Primitive(Primitive::Double),
                    comment: Some("comment_a".to_string()),
                    initial_default: None,
                    write_default: None,
                },
                Field {
                    id: 2,
                    name: "b".to_string(),
                    required: true,
                    field_type: Any::Struct(Struct {
                        fields: vec![
                            Field {
                                id: 3,
                                name: "c".to_string(),
                                required: true,
                                field_type: Any::Primitive(Primitive::Uuid),
                                comment: Some("comment_c".to_string()),
                                initial_default: None,
                                write_default: None,
                            },
                            Field {
                                id: 4,
                                name: "d".to_string(),
                                required: true,
                                field_type: Any::Primitive(Primitive::Boolean),
                                comment: Some("comment_d".to_string()),
                                initial_default: None,
                                write_default: None,
                            },
                            Field {
                                id: 5,
                                name: "e".to_string(),
                                required: true,
                                field_type: Any::List(List {
                                    element_id: 1,
                                    element_required: true,
                                    element_type: Box::new(Any::Primitive(Primitive::Long)),
                                }),
                                comment: Some("comment_e".to_string()),
                                initial_default: None,
                                write_default: None,
                            },
                            Field {
                                id: 6,
                                name: "f".to_string(),
                                required: false,
                                field_type: Any::Map(Map {
                                    key_id: 2,
                                    key_type: Box::new(Any::Primitive(Primitive::String)),
                                    value_id: 3,
                                    value_required: false,
                                    value_type: Box::new(Any::Primitive(Primitive::Binary)),
                                }),
                                comment: Some("comment_f".to_string()),
                                initial_default: None,
                                write_default: None,
                            },
                        ],
                    }),
                    comment: Some("comment_b".to_string()),
                    initial_default: None,
                    write_default: None,
                },
            ],
        };

        let expected_avro_schema = {
            let raw_schema = r#"
{
    "type": "record",
    "name": "r_0",
    "fields": [
        {
            "name": "a",
            "type": "double",
            "doc": "comment_a",
            "order": "ignore"
        },
        {
            "name": "b",
            "type": "record",
            "doc": "comment_b",
            "fields": [
                {
                    "name": "c",
                    "type": "string",
                    "logicalType": "uuid",
                    "doc": "comment_c",
                    "order": "ignore"
                },
                {
                    "name": "d",
                    "type": "boolean",
                    "doc": "comment_d",
                    "order": "ignore"
                },
                {
                    "name": "e",
                    "type": { "type": "array", "items": "long" },
                    "doc": "comment_e",
                    "order": "ignore"
                },
                {
                    "name": "f",
                    "type": { "type": "map", "values": "bytes" },
                    "doc": "comment_f",
                    "order": "ignore"
                }
            ],
            "order": "ignore"
        }
    ]
}
            "#;
            AvroSchema::parse_str(raw_schema).unwrap()
        };

        assert_eq!(expected_avro_schema, AvroSchema::try_from(&schema).unwrap());
    }
}
