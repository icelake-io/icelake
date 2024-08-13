//! Avro data types related functions.

use crate::error::Result;
use crate::types::in_memory::{Any, Field, Primitive, Schema};
use crate::{Error, ErrorKind};
use apache_avro::schema::{
    ArraySchema as AvroArraySchema, DecimalSchema, FixedSchema, MapSchema as AvroMapSchema, Name,
    RecordField as AvroRecordField, RecordFieldOrder, RecordSchema as AvroRecordSchema,
    UnionSchema,
};
use apache_avro::Schema as AvroSchema;
use serde_json::{Number, Value as JsonValue};
use std::collections::BTreeMap;
use std::iter::Iterator;

const ELEMENT_ID: &str = "element-id";
const LOGICAL_TYPE: &str = "logicalType";
const KEY_ID: &str = "key-id";
const VALUE_ID: &str = "value-id";

pub fn to_avro_schema(value: &Schema, name: Option<&str>) -> Result<AvroSchema> {
    let avro_fields: Vec<AvroRecordField> = value
        .fields()
        .iter()
        .map(|field| AvroRecordField::try_from(field.as_ref()))
        .collect::<Result<Vec<AvroRecordField>>>()?;

    let name = name
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("r_{}", value.schema_id));
    Ok(AvroSchema::Record(avro_record_schema(name, avro_fields)))
}

/// Create the avro record filed according the iceberg spec. In this module, we should
/// use this function to create the avro record field all the time.
fn new_avro_record_field(
    name: String,
    doc: Option<String>,
    schema: AvroSchema,
    field_id: i32,
    order: Option<RecordFieldOrder>,
) -> AvroRecordField {
    // If this field is a optional field, default value should be null. (ref: https://iceberg.apache.org/spec/#avro:~:text=Optional%20fields%20must%20always%20set%20the%20Avro%20field%20default%20value%20to%20null.)
    let default = if let AvroSchema::Union(union_schema) = &schema {
        if union_schema.variants().len() == 2 && union_schema.is_nullable() {
            Some(JsonValue::Null)
        } else {
            None
        }
    } else {
        None
    };

    let custom_attributes = {
        let mut map = BTreeMap::new();
        map.insert(
            "field-id".to_string(),
            JsonValue::Number(Number::from(field_id)),
        );
        map
    };

    AvroRecordField {
        name,
        doc,
        aliases: None,
        default,
        schema,
        order: order.unwrap_or(RecordFieldOrder::Ascending),
        position: 0,
        custom_attributes,
    }
}

impl<'a> TryFrom<&'a Field> for AvroRecordField {
    type Error = Error;

    fn try_from(value: &'a Field) -> Result<AvroRecordField> {
        let mut avro_schema = AvroSchema::try_from(AnyWithFieldId {
            any: &value.field_type,
            field_id: &mut Some(value.id),
        })?;
        if !value.required {
            avro_schema = to_avro_option(avro_schema)?;
        }

        Ok(new_avro_record_field(
            value.name.clone(),
            value.comment.clone(),
            avro_schema,
            value.id,
            None,
        ))
    }
}

/// For converting Any to AvroSchema, Record in AvroSchema must has the name.
/// In iceberg(ref: https://github.com/apache/iceberg/blob/c07f2aabc0a1d02f068ecf1514d2479c0fbdd3b0/core/src/main/java/org/apache/iceberg/avro/TypeToSchema.java#L33),
/// this name is "r+ field_id", so we wrap Any with field_id to deal with this case.
struct AnyWithFieldId<'a, 'b> {
    any: &'a Any,
    /// We need to guaranteed that there is only one record use this field_id. Otherwise there will
    /// be dulplicate name in AvroSchema. So we use Option to make sure that there is only one
    /// record take it.
    ///
    /// For now, iceberg spec guaranteed there isn't this schema. So we panic if there is the case
    /// and which means that we may need to fix internal implementation.
    field_id: &'b mut Option<i32>,
}

impl<'a, 'b> TryFrom<AnyWithFieldId<'a, 'b>> for AvroSchema {
    type Error = Error;

    fn try_from(value_with_id: AnyWithFieldId<'a, 'b>) -> Result<AvroSchema> {
        let value = value_with_id.any;
        let avro_schema = match &value {
            Any::Primitive(data_type) => match data_type {
                Primitive::Boolean => AvroSchema::Boolean,
                Primitive::Int => AvroSchema::Int,
                Primitive::Long => AvroSchema::Long,
                Primitive::Float => AvroSchema::Float,
                Primitive::Double => AvroSchema::Double,
                Primitive::Decimal { precision, scale } => AvroSchema::Decimal(DecimalSchema {
                    precision: *precision as usize,
                    scale: *scale as usize,
                    inner: Box::new(AvroSchema::Fixed(FixedSchema {
                        name: Name::new(&*&format!("decimal_{}_{}", precision, scale))?,
                        aliases: None,
                        doc: None,
                        size: Primitive::decimal_required_bytes(*precision as u32)? as usize,
                        attributes: BTreeMap::default(),
                        default: None,
                    })),
                }),
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
                    ));
                }
            },

            Any::Map(map) => {
                if let Any::Primitive(Primitive::String) = *map.key_type {
                    let mut value_avro_schema = AvroSchema::try_from(AnyWithFieldId {
                        any: &map.value_type,
                        field_id: value_with_id.field_id,
                    })?;
                    if !map.value_required {
                        value_avro_schema = to_avro_option(value_avro_schema)?;
                    }

                    AvroSchema::Map(AvroMapSchema {
                        types: Box::new(value_avro_schema),
                        attributes: BTreeMap::from([
                            (
                                KEY_ID.to_string(),
                                JsonValue::String(map.key_id.to_string()),
                            ),
                            (
                                VALUE_ID.to_string(),
                                JsonValue::String(map.value_id.to_string()),
                            ),
                        ]),
                    })
                } else {
                    let key_field = new_avro_record_field(
                        "key".to_string(),
                        None,
                        AvroSchema::try_from(AnyWithFieldId {
                            any: &map.key_type,
                            field_id: &mut Some(map.key_id),
                        })?,
                        map.key_id,
                        None,
                    );

                    let value_field = {
                        let mut value_schema = AvroSchema::try_from(AnyWithFieldId {
                            any: &map.value_type,
                            field_id: &mut Some(map.value_id),
                        })?;
                        if !map.value_required {
                            value_schema = to_avro_option(value_schema)?;
                        }

                        new_avro_record_field(
                            "value".to_string(),
                            None,
                            value_schema,
                            map.value_id,
                            None,
                        )
                    };

                    AvroSchema::Array(AvroArraySchema {
                        items: Box::new(AvroSchema::Record(avro_record_schema(
                            format!("k{}_v{}", map.key_id, map.value_id).as_str(),
                            vec![key_field, value_field],
                        ))),
                        attributes: BTreeMap::from([(
                            LOGICAL_TYPE.to_string(),
                            JsonValue::String("map".to_string()),
                        )]),
                    })
                }
            }
            Any::List(list) => {
                let mut avro_schema = AvroSchema::try_from(AnyWithFieldId {
                    any: &list.element_type,
                    field_id: &mut Some(list.element_id),
                })?;
                if !list.element_required {
                    avro_schema = to_avro_option(avro_schema)?;
                }
                AvroSchema::Array(AvroArraySchema {
                    items: Box::new(avro_schema),
                    attributes: BTreeMap::from([(ELEMENT_ID.to_string(), list.element_id.into())]),
                })
            }
            Any::Struct(s) => {
                let avro_fields: Vec<AvroRecordField> = s
                    .fields()
                    .iter()
                    .map(|field| AvroRecordField::try_from(field.as_ref()))
                    .collect::<Result<Vec<AvroRecordField>>>()?;
                let name = format!(
                    "r{}",
                    value_with_id
                        .field_id
                        .take()
                        .expect("Iceberg Spec guaranteed that only one record in one field.")
                );
                AvroSchema::Record(avro_record_schema(name, avro_fields))
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
    use crate::types::{self, Struct};
    use std::sync::Arc;

    #[test]
    fn test_to_manifest_schema() {
        let schema_str = r#"
    {
      "type" : "record",
      "name" : "manifest_entry",
      "fields" : [ {
        "name" : "status",
        "type" : "int",
        "field-id" : 0
      }, {
        "name" : "snapshot_id",
        "type" : [ "null", "long" ],
        "default" : null,
        "field-id" : 1
      }, {
        "name" : "sequence_number",
        "type" : [ "null", "long" ],
        "default" : null,
        "field-id" : 3
      }, {
        "name" : "file_sequence_number",
        "type" : [ "null", "long" ],
        "default" : null,
        "field-id" : 4
      }, {
        "name" : "data_file",
        "type" : {
          "type" : "record",
          "name" : "r2",
          "fields" : [ {
            "name" : "content",
            "type" : "int",
            "doc" : "Contents of the file: 0=data, 1=position deletes, 2=equality deletes",
            "field-id" : 134
          }, {
            "name" : "file_path",
            "type" : "string",
            "doc" : "Location URI with FS scheme",
            "field-id" : 100
          }, {
            "name" : "file_format",
            "type" : "string",
            "doc" : "File format name: avro, orc, or parquet",
            "field-id" : 101
          }, {
            "name" : "partition",
            "type" : {
              "type" : "record",
              "name" : "r102",
              "fields" : [ ]
            },
            "doc" : "Partition data tuple, schema based on the partition spec",
            "field-id" : 102
          }, {
            "name" : "record_count",
            "type" : "long",
            "doc" : "Number of records in the file",
            "field-id" : 103
          }, {
            "name" : "file_size_in_bytes",
            "type" : "long",
            "doc" : "Total file size in bytes",
            "field-id" : 104
          }, {
            "name" : "column_sizes",
            "type" : [ "null", {
              "type" : "array",
              "items" : {
                "type" : "record",
                "name" : "k117_v118",
                "fields" : [ {
                  "name" : "key",
                  "type" : "int",
                  "field-id" : 117
                }, {
                  "name" : "value",
                  "type" : "long",
                  "field-id" : 118
                } ]
              },
              "logicalType" : "map"
            } ],
            "doc" : "Map of column id to total size on disk",
            "default" : null,
            "field-id" : 108
          }, {
            "name" : "value_counts",
            "type" : [ "null", {
              "type" : "array",
              "items" : {
                "type" : "record",
                "name" : "k119_v120",
                "fields" : [ {
                  "name" : "key",
                  "type" : "int",
                  "field-id" : 119
                }, {
                  "name" : "value",
                  "type" : "long",
                  "field-id" : 120
                } ]
              },
              "logicalType" : "map"
            } ],
            "doc" : "Map of column id to total count, including null and NaN",
            "default" : null,
            "field-id" : 109
          }, {
            "name" : "null_value_counts",
            "type" : [ "null", {
              "type" : "array",
              "items" : {
                "type" : "record",
                "name" : "k121_v122",
                "fields" : [ {
                  "name" : "key",
                  "type" : "int",
                  "field-id" : 121
                }, {
                  "name" : "value",
                  "type" : "long",
                  "field-id" : 122
                } ]
              },
              "logicalType" : "map"
            } ],
            "doc" : "Map of column id to null value count",
            "default" : null,
            "field-id" : 110
          }, {
            "name" : "nan_value_counts",
            "type" : [ "null", {
              "type" : "array",
              "items" : {
                "type" : "record",
                "name" : "k138_v139",
                "fields" : [ {
                  "name" : "key",
                  "type" : "int",
                  "field-id" : 138
                }, {
                  "name" : "value",
                  "type" : "long",
                  "field-id" : 139
                } ]
              },
              "logicalType" : "map"
            } ],
            "doc" : "Map of column id to number of NaN values in the column",
            "default" : null,
            "field-id" : 137
          }, {
            "name" : "lower_bounds",
            "type" : [ "null", {
              "type" : "array",
              "items" : {
                "type" : "record",
                "name" : "k126_v127",
                "fields" : [ {
                  "name" : "key",
                  "type" : "int",
                  "field-id" : 126
                }, {
                  "name" : "value",
                  "type" : "bytes",
                  "field-id" : 127
                } ]
              },
              "logicalType" : "map"
            } ],
            "doc" : "Map of column id to lower bound",
            "default" : null,
            "field-id" : 125
          }, {
            "name" : "upper_bounds",
            "type" : [ "null", {
              "type" : "array",
              "items" : {
                "type" : "record",
                "name" : "k129_v130",
                "fields" : [ {
                  "name" : "key",
                  "type" : "int",
                  "field-id" : 129
                }, {
                  "name" : "value",
                  "type" : "bytes",
                  "field-id" : 130
                } ]
              },
              "logicalType" : "map"
            } ],
            "doc" : "Map of column id to upper bound",
            "default" : null,
            "field-id" : 128
          }, {
            "name" : "key_metadata",
            "type" : [ "null", "bytes" ],
            "doc" : "Encryption key metadata blob",
            "default" : null,
            "field-id" : 131
          }, {
            "name" : "split_offsets",
            "type" : [ "null", {
              "type" : "array",
              "items" : "long",
              "element-id" : 133
            } ],
            "doc" : "Splittable offsets",
            "default" : null,
            "field-id" : 132
          }, {
            "name" : "equality_ids",
            "type" : [ "null", {
              "type" : "array",
              "items" : "int",
              "element-id" : 136
            } ],
            "doc" : "Equality comparison field IDs",
            "default" : null,
            "field-id" : 135
          }, {
            "name" : "sort_order_id",
            "type" : [ "null", "int" ],
            "doc" : "Sort order ID",
            "default" : null,
            "field-id" : 140
          } ]
        },
        "field-id" : 2
    } ] }"#;
        let expect_schema = AvroSchema::parse(&serde_json::from_str(schema_str).unwrap()).unwrap();

        let partition_type = Struct::new(vec![]);
        let avro_schema = to_avro_schema(
            &types::ManifestFile::v2_schema(partition_type),
            Some("manifest_entry"),
        )
        .unwrap();

        assert_eq!(avro_schema, expect_schema);
    }

    #[test]
    fn test_to_manifest_list_schema() {
        let schema_str = r#"
        {
          "type" : "record",
          "name" : "manifest_file",
          "fields" : [ {
            "name" : "manifest_path",
            "type" : "string",
            "doc" : "Location URI with FS scheme",
            "field-id" : 500
          }, {
            "name" : "manifest_length",
            "type" : "long",
            "doc" : "Total file size in bytes",
            "field-id" : 501
          }, {
            "name" : "partition_spec_id",
            "type" : "int",
            "doc" : "Spec ID used to write",
            "field-id" : 502
          }, {
            "name" : "content",
            "type" : "int",
            "doc" : "Contents of the manifest: 0=data, 1=deletes",
            "field-id" : 517
          }, {
            "name" : "sequence_number",
            "type" : "long",
            "doc" : "Sequence number when the manifest was added",
            "field-id" : 515
          }, {
            "name" : "min_sequence_number",
            "type" : "long",
            "doc" : "Lowest sequence number in the manifest",
            "field-id" : 516
          }, {
            "name" : "added_snapshot_id",
            "type" : "long",
            "doc" : "Snapshot ID that added the manifest",
            "field-id" : 503
          }, {
            "name" : "added_files_count",
            "type" : "int",
            "doc" : "Added entry count",
            "field-id" : 504
          }, {
            "name" : "existing_files_count",
            "type" : "int",
            "doc" : "Existing entry count",
            "field-id" : 505
          }, {
            "name" : "deleted_files_count",
            "type" : "int",
            "doc" : "Deleted entry count",
            "field-id" : 506
          }, {
            "name" : "added_rows_count",
            "type" : "long",
            "doc" : "Added rows count",
            "field-id" : 512
          }, {
            "name" : "existing_rows_count",
            "type" : "long",
            "doc" : "Existing rows count",
            "field-id" : 513
          }, {
            "name" : "deleted_rows_count",
            "type" : "long",
            "doc" : "Deleted rows count",
            "field-id" : 514
          }, {
            "name" : "partitions",
            "type" : [ "null", {
              "type" : "array",
              "items" : {
                "type" : "record",
                "name" : "r508",
                "fields" : [ {
                  "name" : "contains_null",
                  "type" : "boolean",
                  "doc" : "True if any file has a null partition value",
                  "field-id" : 509
                }, {
                  "name" : "contains_nan",
                  "type" : [ "null", "boolean" ],
                  "doc" : "True if any file has a nan partition value",
                  "default" : null,
                  "field-id" : 518
                }, {
                  "name" : "lower_bound",
                  "type" : [ "null", "bytes" ],
                  "doc" : "Partition lower bound for all files",
                  "default" : null,
                  "field-id" : 510
                }, {
                  "name" : "upper_bound",
                  "type" : [ "null", "bytes" ],
                  "doc" : "Partition upper bound for all files",
                  "default" : null,
                  "field-id" : 511
                } ]
              },
              "element-id" : 508
            } ],
            "doc" : "Summary for each partition",
            "default" : null,
            "field-id" : 507
          }, {
                "name" : "key_metadata",
                "type" : [ "null", "bytes" ],
                "default" : null,
                "field-id" : 519
          } ]
        }"#;

        let expect_schema = AvroSchema::parse(&serde_json::from_str(schema_str).unwrap()).unwrap();
        let avro_schema =
            to_avro_schema(&types::ManifestList::v2_schema(), Some("manifest_file")).unwrap();

        assert_eq!(avro_schema, expect_schema);
    }

    #[test]
    fn test_avro_schema_with_decimal() {
        let schema = Schema::new(
            0,
            None,
            Struct::new(vec![Arc::new(Field::required(
                1,
                "test_decimal",
                Any::Primitive(Primitive::Decimal {
                    precision: 36,
                    scale: 2,
                }),
            ))]),
        );

        let avro_schema = to_avro_schema(&schema, None).unwrap();

        let expected_str = r#"{
  "type": "record",
  "name": "r_0",
  "fields": [
    {
      "name": "test_decimal",
      "type": {
        "type": "fixed",
        "name": "decimal_36_2",
        "size": 16,
        "logicalType": "decimal",
        "scale": 2,
        "precision": 36
      },
      "field-id": 1
    }
  ]
}"#;

        assert_eq!(
            expected_str,
            serde_json::to_string_pretty(&avro_schema).unwrap()
        );
    }
}
