use serde::{Deserialize, Serialize};

use super::types::*;
use crate::types;
use crate::Error;
use crate::Result;

/// Parse schema from json bytes.
pub fn parse_schema(schema: &[u8]) -> Result<types::Schema> {
    let schema: Schema = serde_json::from_slice(schema)?;
    schema.try_into()
}

/// Serialize schema to json string.
pub fn serialize_schema(schema: &types::Schema) -> Result<String> {
    Ok(serde_json::to_string(&Schema::try_from(schema)?)?)
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    schema_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    identifier_field_ids: Option<Vec<i32>>,
    fields: Vec<Field>,
}

impl TryFrom<Schema> for types::Schema {
    type Error = Error;

    fn try_from(value: Schema) -> Result<Self> {
        let mut fields = Vec::with_capacity(value.fields.len());
        for field in value.fields {
            fields.push(field.try_into()?);
        }

        Ok(types::Schema {
            schema_id: value.schema_id,
            identifier_field_ids: value.identifier_field_ids,
            fields,
        })
    }
}

impl<'a> TryFrom<&'a types::Schema> for Schema {
    type Error = Error;

    fn try_from(v: &'a types::Schema) -> Result<Self> {
        Ok(Self {
            schema_id: v.schema_id,
            identifier_field_ids: v.identifier_field_ids.as_ref().cloned(),
            fields: v
                .fields
                .iter()
                .map(|v| Field::try_from(v.clone()))
                .collect::<Result<Vec<Field>>>()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_schema_serde(json_schema: &str, expected_schema: types::Schema) {
        let schema = parse_schema(json_schema.as_bytes()).unwrap();
        assert_eq!(expected_schema, schema);

        let serialized_json_schema = serialize_schema(&expected_schema).unwrap();

        assert_eq!(
            expected_schema,
            parse_schema(serialized_json_schema.as_bytes()).unwrap()
        );
    }

    #[test]
    fn test_schema_json_conversion() {
        let json_schema = r#"
{
	"type" : "struct",
	"schema-id" : 0,
   	"fields" : [ {
  		"id" : 1,
		"name" : "VendorID",
  		"required" : false,
 		"type" : "long"
 	} ]
}
        "#;

        let expected_schema = types::Schema {
            schema_id: 0,
            identifier_field_ids: None,
            fields: vec![types::Field {
                id: 1,
                name: "VendorID".to_string(),
                required: false,
                field_type: types::Any::Primitive(types::Primitive::Long),
                comment: None,
                initial_default: None,
                write_default: None,
            }],
        };

        check_schema_serde(json_schema, expected_schema);
    }

    #[test]
    fn test_parse_schema_struct_with_default() {
        let schema = r#"
{
	"type" : "struct",
	"schema-id" : 0,
   	"fields" : [ {
  		"id" : 1,
		"name" : "VendorID",
  		"required" : false,
 		"type" : "long",
        "initial-default": 123,
        "write-default": 456
 	} ]
}
        "#;

        let expected_schema = types::Schema {
            schema_id: 0,
            identifier_field_ids: None,
            fields: vec![types::Field {
                id: 1,
                name: "VendorID".to_string(),
                required: false,
                field_type: types::Any::Primitive(types::Primitive::Long),
                comment: None,
                initial_default: Some(types::AnyValue::Primitive(types::PrimitiveValue::Long(123))),
                write_default: Some(types::AnyValue::Primitive(types::PrimitiveValue::Long(456))),
            }],
        };

        let schema = parse_schema(schema.as_bytes()).unwrap();

        assert_eq!(expected_schema, schema);
    }

    #[test]
    fn test_parse_schema_list() {
        let json_schema = r#"
{
    "type" : "struct",
    "schema-id" : 0,
    "fields" : [
        {
            "id" : 1,
            "name" : "VendorID",
            "required" : false,
            "type": {
                "type": "list",
                "element-id": 3,
                "element-required": true,
                "element": "string"
            }
        }
    ]
}
        "#;

        let expected_schema = types::Schema {
            schema_id: 0,
            identifier_field_ids: None,
            fields: vec![types::Field {
                id: 1,
                name: "VendorID".to_string(),
                required: false,
                field_type: types::Any::List(types::List {
                    element_id: 3,
                    element_required: true,
                    element_type: types::Any::Primitive(types::Primitive::String).into(),
                }),
                comment: None,
                initial_default: None,
                write_default: None,
            }],
        };

        check_schema_serde(json_schema, expected_schema);
    }

    #[test]
    fn test_parse_schema_map() {
        let json_schema = r#"
{
    "type" : "struct",
    "schema-id" : 0,
    "fields" : [
        {
            "id" : 1,
            "name" : "VendorID",
            "required" : false,
            "type": {
                "type": "map",
                "key-id": 4,
                "key": "string",
                "value-id": 5,
                "value-required": false,
                "value": "double"
            }
        }
    ]
}
        "#;

        let expected_schema = types::Schema {
            schema_id: 0,
            identifier_field_ids: None,
            fields: vec![types::Field {
                id: 1,
                name: "VendorID".to_string(),
                required: false,
                field_type: types::Any::Map(types::Map {
                    key_id: 4,
                    key_type: types::Any::Primitive(types::Primitive::String).into(),
                    value_id: 5,
                    value_required: false,
                    value_type: types::Any::Primitive(types::Primitive::Double).into(),
                }),
                comment: None,
                initial_default: None,
                write_default: None,
            }],
        };

        check_schema_serde(json_schema, expected_schema);
    }
}
