//! Avro data types related functions.

use crate::error::Result;
use crate::types::in_memory::{Any, Field, Primitive, Schema};
use crate::{Error, ErrorKind};
use apache_avro::schema::{
    Name, RecordField as AvroRecordField, RecordFieldOrder, RecordSchema as AvroRecordSchema,
    UnionSchema,
};
use apache_avro::Schema as AvroSchema;
use serde_json::{Number, Value as JsonValue};
use std::collections::BTreeMap;
use std::iter::Iterator;

pub fn to_avro_schema(value: &Schema, name: Option<&str>) -> Result<AvroSchema> {
    let avro_fields: Vec<AvroRecordField> = value
        .fields
        .iter()
        .map(AvroRecordField::try_from)
        .collect::<Result<Vec<AvroRecordField>>>()?;

    let name = name
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("r_{}", value.schema_id));
    Ok(AvroSchema::Record(avro_record_schema(name, avro_fields)))
}

impl<'a> TryFrom<&'a Field> for AvroRecordField {
    type Error = Error;

    fn try_from(value: &'a Field) -> Result<AvroRecordField> {
        let avro_schema = match &value.field_type {
            Any::Struct(_) => {
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
                avro_schema
            }
            Any::List(_list) => AvroSchema::try_from(&value.field_type)?,
            _ => {
                let mut avro_schema = AvroSchema::try_from(&value.field_type)?;
                if !value.required {
                    avro_schema =
                        AvroSchema::Union(UnionSchema::new(vec![AvroSchema::Null, avro_schema])?);
                }
                avro_schema
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
                if let Any::Primitive(Primitive::String) = *map.key_type {
                    let mut value_avro_schema = AvroSchema::try_from(&*map.value_type)?;
                    if !map.value_required {
                        value_avro_schema = to_avro_option(value_avro_schema)?;
                    }

                    AvroSchema::Map(Box::new(value_avro_schema))
                } else {
                    let key_field = {
                        let mut field = AvroRecordField {
                            name: "key".to_string(),
                            doc: None,
                            aliases: None,
                            default: None,
                            schema: AvroSchema::try_from(&*map.key_type)?,
                            order: RecordFieldOrder::Ignore,
                            position: 0,
                            custom_attributes: BTreeMap::default(),
                        };
                        field.custom_attributes.insert(
                            "field-id".to_string(),
                            JsonValue::Number(Number::from(map.key_id)),
                        );
                        field
                    };

                    let value_field = {
                        let mut value_schema = AvroSchema::try_from(&*map.value_type)?;
                        if !map.value_required {
                            value_schema = to_avro_option(value_schema)?;
                        }

                        let mut field = AvroRecordField {
                            name: "value".to_string(),
                            doc: None,
                            aliases: None,
                            default: None,
                            schema: value_schema,
                            order: RecordFieldOrder::Ignore,
                            position: 0,
                            custom_attributes: BTreeMap::default(),
                        };
                        field.custom_attributes.insert(
                            "field-id".to_string(),
                            JsonValue::Number(Number::from(map.value_id)),
                        );
                        field
                    };

                    AvroSchema::Array(Box::new(AvroSchema::Record(avro_record_schema(
                        format!("k{}_v{}", map.key_id, map.value_id).as_str(),
                        vec![key_field, value_field],
                    ))))
                }
            }
            Any::List(list) => {
                let mut avro_schema = AvroSchema::try_from(&*list.element_type)?;
                if !list.element_required {
                    avro_schema = to_avro_option(avro_schema)?;
                }
                AvroSchema::Array(Box::new(avro_schema))
            }
            Any::Struct(s) => {
                let avro_fields: Vec<AvroRecordField> = s
                    .fields
                    .iter()
                    .map(AvroRecordField::try_from)
                    .collect::<Result<Vec<AvroRecordField>>>()?;
                AvroSchema::Record(avro_record_schema("invalid_name", avro_fields))
            }
        };
        Ok(avro_schema)
    }
}

fn avro_record_schema(
    name: impl Into<String>,
    fields: impl IntoIterator<Item = AvroRecordField>,
) -> AvroRecordSchema {
    let avro_fields = fields.into_iter().collect::<Vec<AvroRecordField>>();
    let lookup = avro_fields
        .iter()
        .enumerate()
        .map(|f| (f.1.name.clone(), f.0))
        .collect();

    AvroRecordSchema {
        name: Name::from(name.into().as_str()),
        fields: avro_fields,
        aliases: None,
        doc: None,
        lookup,
        attributes: BTreeMap::default(),
    }
}

fn is_avro_option(avro_schema: &AvroSchema) -> bool {
    match avro_schema {
        AvroSchema::Union(u) => u.variants().len() == 2 && u.is_nullable(),
        _ => false,
    }
}

fn to_avro_option(avro_schema: AvroSchema) -> Result<AvroSchema> {
    assert!(!is_avro_option(&avro_schema));
    Ok(AvroSchema::Union(UnionSchema::new(vec![
        AvroSchema::Null,
        avro_schema,
    ])?))
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
                                required: true,
                                field_type: Any::Map(Map {
                                    key_id: 2,
                                    key_type: Box::new(Any::Primitive(Primitive::String)),
                                    value_id: 3,
                                    value_required: true,
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

        assert_eq!(expected_avro_schema, to_avro_schema(&schema, None).unwrap());
    }
}
