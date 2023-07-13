use serde::Deserialize;

use super::types::*;
use crate::datatypes;
use crate::Error;
use crate::Result;

/// Parse schema from json bytes.
pub fn parse_schema(schema: &[u8]) -> Result<datatypes::Schema> {
    let schema: Schema = serde_json::from_slice(schema)?;
    schema.try_into()
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Schema {
    schema_id: i32,
    identifier_field_ids: Option<Vec<i32>>,
    fields: Vec<Field>,
}

impl TryFrom<Schema> for datatypes::Schema {
    type Error = Error;

    fn try_from(value: Schema) -> Result<Self> {
        let mut fields = Vec::with_capacity(value.fields.len());
        for field in value.fields {
            fields.push(field.try_into()?);
        }

        Ok(datatypes::Schema {
            schema_id: value.schema_id,
            identifier_field_ids: value.identifier_field_ids,
            fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes;

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

        assert_eq!(schema.schema_id, 0);
        assert_eq!(schema.identifier_field_ids, None);
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].id, 1);
        assert_eq!(schema.fields[0].name, "VendorID");
        assert!(!schema.fields[0].required);
        assert_eq!(
            schema.fields[0].field_type,
            datatypes::Any::Primitive(datatypes::Primitive::Long)
        );
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

        let schema = parse_schema(schema.as_bytes()).unwrap();

        assert_eq!(schema.schema_id, 0);
        assert_eq!(schema.identifier_field_ids, None);
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].id, 1);
        assert_eq!(schema.fields[0].name, "VendorID");
        assert!(!schema.fields[0].required);
        assert_eq!(
            schema.fields[0].field_type,
            datatypes::Any::Primitive(datatypes::Primitive::Long)
        );
        assert_eq!(
            schema.fields[0].initial_default,
            Some(datatypes::AnyValue::Primitive(
                datatypes::PrimitiveValue::Long(123)
            ))
        );
        assert_eq!(
            schema.fields[0].write_default,
            Some(datatypes::AnyValue::Primitive(
                datatypes::PrimitiveValue::Long(456)
            ))
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

        assert_eq!(schema.schema_id, 0);
        assert_eq!(schema.identifier_field_ids, None);
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].id, 1);
        assert_eq!(schema.fields[0].name, "VendorID");
        assert!(!schema.fields[0].required);
        assert_eq!(
            schema.fields[0].field_type,
            datatypes::Any::List(datatypes::List {
                element_id: 3,
                element_required: true,
                element_type: datatypes::Any::Primitive(datatypes::Primitive::String).into(),
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

        assert_eq!(schema.schema_id, 0);
        assert_eq!(schema.identifier_field_ids, None);
        assert_eq!(schema.fields.len(), 1);
        assert_eq!(schema.fields[0].id, 1);
        assert_eq!(schema.fields[0].name, "VendorID");
        assert!(!schema.fields[0].required);
        assert_eq!(
            schema.fields[0].field_type,
            datatypes::Any::Map(datatypes::Map {
                key_id: 4,
                key_type: datatypes::Any::Primitive(datatypes::Primitive::String).into(),
                value_id: 5,
                value_required: false,
                value_type: datatypes::Any::Primitive(datatypes::Primitive::Double).into(),
            })
        );
    }
}
