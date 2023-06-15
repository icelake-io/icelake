use serde::Deserialize;

use crate::types;
use anyhow::anyhow;
use anyhow::Result;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Schema {
    schema_id: i32,
    identifier_field_ids: Option<Vec<i32>>,
    #[serde(flatten)]
    typ: NestedType,
}

impl TryFrom<Schema> for types::SchemaV2 {
    type Error = anyhow::Error;

    fn try_from(value: Schema) -> Result<Self, Self::Error> {
        let types: types::Any = (&value.typ).try_into()?;
        let types = if let types::Any::Struct(v) = types {
            v
        } else {
            return Err(anyhow!("schema type must be struct"));
        };

        Ok(types::SchemaV2 {
            id: value.schema_id,
            identifier_field_ids: value.identifier_field_ids,
            types,
        })
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum StringOrTypes {
    PrimitiveType(PrimitiveType),
    NestedType(NestedType),
}

#[derive(Deserialize)]
struct PrimitiveType {
    #[serde(rename = "type")]
    typ: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct NestedType {
    #[serde(rename = "type")]
    typ: String,

    /// Only available when typ == "struct"
    fields: Option<Vec<Field>>,

    /// Only available when typ == "list"
    element_id: Option<i32>,
    element_required: Option<bool>,
    element: Option<Box<StringOrTypes>>,

    /// Only available when typ == "map"
    key_id: Option<i32>,
    key: Option<Box<StringOrTypes>>,
    value_id: Option<i32>,
    value_required: Option<bool>,
    value: Option<Box<StringOrTypes>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Field {
    id: i32,
    name: String,
    required: bool,
    #[serde(rename = "type", flatten)]
    typ: StringOrTypes,
    doc: Option<String>,
}

impl TryFrom<&StringOrTypes> for types::Any {
    type Error = anyhow::Error;

    fn try_from(v: &StringOrTypes) -> Result<Self, Self::Error> {
        let t = match v {
            StringOrTypes::PrimitiveType(s) => match s.typ.as_str() {
                "boolean" => types::Any::Primitive(types::Primitive::Boolean),
                "int" => types::Any::Primitive(types::Primitive::Int),
                "long" => types::Any::Primitive(types::Primitive::Long),
                "float" => types::Any::Primitive(types::Primitive::Float),
                "double" => types::Any::Primitive(types::Primitive::Double),
                "date" => types::Any::Primitive(types::Primitive::Date),
                "time" => types::Any::Primitive(types::Primitive::Time),
                "timestamp" => types::Any::Primitive(types::Primitive::Timestamp),
                "timestamptz" => types::Any::Primitive(types::Primitive::Timestampz),
                "string" => types::Any::Primitive(types::Primitive::String),
                "uuid" => types::Any::Primitive(types::Primitive::Uuid),
                "binary" => types::Any::Primitive(types::Primitive::Binary),
                v if v.starts_with("fixed") => {
                    let length = v
                        .strip_prefix("fixed")
                        .expect("type must starts with `fixed`")
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .parse()
                        .map_err(|err| anyhow!("fixed type {v:?} is invalid: {err:?}"))?;

                    types::Any::Primitive(types::Primitive::Fixed(length))
                }
                v if v.starts_with("decimal") => {
                    let parts = v
                        .strip_prefix("decimal")
                        .expect("type must starts with `decimal`")
                        .trim_start_matches('(')
                        .trim_end_matches(')')
                        .split(',')
                        .collect::<Vec<_>>();
                    if parts.len() != 2 {
                        return Err(anyhow!("decimal type {v:?} is invalid"));
                    }

                    let precision = parts[0]
                        .parse()
                        .map_err(|err| anyhow!("decimal type {v:?} is invalid: {err:?}"))?;
                    let scale = parts[1]
                        .parse()
                        .map_err(|err| anyhow!("decimal type {v:?} is invalid: {err:?}"))?;

                    types::Any::Primitive(types::Primitive::Decimal { precision, scale })
                }
                s => return Err(anyhow!("type {:?} is not valid primitive type", s)),
            },
            StringOrTypes::NestedType(v) => v.try_into()?,
        };

        Ok(t)
    }
}

impl TryFrom<&NestedType> for types::Any {
    type Error = anyhow::Error;

    fn try_from(v: &NestedType) -> Result<Self, Self::Error> {
        let t = match v.typ.as_str() {
            "struct" => {
                let raw_fields = v
                    .fields
                    .as_ref()
                    .ok_or_else(|| anyhow!("struct type must have fields"))?;

                let mut fields = Vec::with_capacity(raw_fields.len());
                for f in raw_fields {
                    let field = types::Field {
                        id: f.id,
                        name: f.name.clone(),
                        required: f.required,
                        field_type: (&f.typ).try_into()?,
                        comment: f.doc.clone(),
                    };

                    fields.push(field);
                }

                types::Any::Struct(types::Struct { fields })
            }
            "list" => {
                let element_id = v
                    .element_id
                    .ok_or_else(|| anyhow!("list type must have element_id"))?;
                let element_required = v
                    .element_required
                    .ok_or_else(|| anyhow!("list type must have element_required"))?;
                let element_type = v
                    .element
                    .as_ref()
                    .ok_or_else(|| anyhow!("list type must have element"))?;

                types::Any::List(types::List {
                    element_id,
                    element_required,
                    element_type: Box::new(element_type.as_ref().try_into()?),
                })
            }
            "map" => {
                let key_id = v
                    .key_id
                    .ok_or_else(|| anyhow!("map type must have key_id"))?;
                let key_type = v
                    .key
                    .as_ref()
                    .ok_or_else(|| anyhow!("map type must have key"))?;
                let value_id = v
                    .value_id
                    .ok_or_else(|| anyhow!("map type must have value_id"))?;
                let value_required = v
                    .value_required
                    .ok_or_else(|| anyhow!("map type must have value_required"))?;
                let value_type = v
                    .value
                    .as_ref()
                    .ok_or_else(|| anyhow!("map type must have value_type"))?;

                types::Any::Map(types::Map {
                    key_id,
                    key_type: Box::new(key_type.as_ref().try_into()?),
                    value_id,
                    value_required,
                    value_type: Box::new(value_type.as_ref().try_into()?),
                })
            }
            v => return Err(anyhow!("type {:?} is not valid nested type", v)),
        };

        Ok(t)
    }
}

/// Parse schema_v2 from json bytes.
pub fn parse_schema_v2(schema: &[u8]) -> Result<types::SchemaV2> {
    let schema: Schema = serde_json::from_slice(schema)?;
    schema.try_into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_schema_v2_struct() {
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

        let schema = parse_schema_v2(schema.as_bytes()).unwrap();

        assert_eq!(schema.id, 0);
        assert_eq!(schema.identifier_field_ids, None);
        assert_eq!(schema.types.fields.len(), 1);
        assert_eq!(schema.types.fields[0].id, 1);
        assert_eq!(schema.types.fields[0].name, "VendorID");
        assert_eq!(schema.types.fields[0].required, false);
        assert_eq!(
            schema.types.fields[0].field_type,
            types::Any::Primitive(types::Primitive::Long)
        );
    }

    #[test]
    fn test_parse_schema_v2_list() {
        let schema = r#"
{
	"type" : "struct",
	"schema-id" : 0,
   	"fields" : [ {
  		"id" : 1,
		"name" : "VendorID",
  		"required" : false,
 		"type": "list",
        "element-id": 3,
        "element-required": true,
        "element": "string"
 	} ]
}
        "#;

        let schema = parse_schema_v2(schema.as_bytes()).unwrap();

        assert_eq!(schema.id, 0);
        assert_eq!(schema.identifier_field_ids, None);
        assert_eq!(schema.types.fields.len(), 1);
        assert_eq!(schema.types.fields[0].id, 1);
        assert_eq!(schema.types.fields[0].name, "VendorID");
        assert_eq!(schema.types.fields[0].required, false);
        assert_eq!(
            schema.types.fields[0].field_type,
            types::Any::List(types::List {
                element_id: 3,
                element_required: true,
                element_type: types::Any::Primitive(types::Primitive::String).into(),
            })
        );
    }
}
