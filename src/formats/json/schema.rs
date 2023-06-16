use serde::Deserialize;

use crate::types;
use anyhow::anyhow;
use anyhow::Result;

use super::types::string_or_struct;
use super::types::*;

/// Parse schema from json bytes.
pub fn parse_schema(schema: &[u8]) -> Result<types::Schema> {
    let schema: Schema = serde_json::from_slice(schema)?;
    schema.try_into()
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Schema {
    schema_id: i32,
    identifier_field_ids: Option<Vec<i32>>,
    #[serde(rename = "type", flatten, deserialize_with = "string_or_struct")]
    typ: Types,
}

impl TryFrom<Schema> for types::Schema {
    type Error = anyhow::Error;

    fn try_from(value: Schema) -> Result<Self, Self::Error> {
        let types: types::Any = value.typ.try_into()?;
        let types = if let types::Any::Struct(v) = types {
            v
        } else {
            return Err(anyhow!("schema type must be struct"));
        };

        Ok(types::Schema {
            id: value.schema_id,
            identifier_field_ids: value.identifier_field_ids,
            types,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schema_struct() {
        let schema = r#"
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

        let schema = parse_schema(schema.as_bytes()).unwrap();

        assert_eq!(schema.id, 0);
        assert_eq!(schema.identifier_field_ids, None);
        assert_eq!(schema.types.fields.len(), 1);
        assert_eq!(schema.types.fields[0].id, 1);
        assert_eq!(schema.types.fields[0].name, "VendorID");
        assert!(!schema.types.fields[0].required);
        assert_eq!(
            schema.types.fields[0].field_type,
            types::Any::Primitive(types::Primitive::Long)
        );
    }

    #[test]
    fn test_parse_schema_list() {
        let schema = r#"
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

        let schema = parse_schema(schema.as_bytes()).unwrap();

        assert_eq!(schema.id, 0);
        assert_eq!(schema.identifier_field_ids, None);
        assert_eq!(schema.types.fields.len(), 1);
        assert_eq!(schema.types.fields[0].id, 1);
        assert_eq!(schema.types.fields[0].name, "VendorID");
        assert!(!schema.types.fields[0].required);
        assert_eq!(
            schema.types.fields[0].field_type,
            types::Any::List(types::List {
                element_id: 3,
                element_required: true,
                element_type: types::Any::Primitive(types::Primitive::String).into(),
            })
        );
    }

    #[test]
    fn test_parse_schema_map() {
        let schema = r#"
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

        let schema = parse_schema(schema.as_bytes()).unwrap();

        assert_eq!(schema.id, 0);
        assert_eq!(schema.identifier_field_ids, None);
        assert_eq!(schema.types.fields.len(), 1);
        assert_eq!(schema.types.fields[0].id, 1);
        assert_eq!(schema.types.fields[0].name, "VendorID");
        assert!(!schema.types.fields[0].required);
        assert_eq!(
            schema.types.fields[0].field_type,
            types::Any::Map(types::Map {
                key_id: 4,
                key_type: types::Any::Primitive(types::Primitive::String).into(),
                value_id: 5,
                value_required: false,
                value_type: types::Any::Primitive(types::Primitive::Double).into(),
            })
        );
    }
}
