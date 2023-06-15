use std::fmt;
use std::marker::PhantomData;

use serde::de;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;

use crate::types;
use anyhow::anyhow;
use anyhow::Result;

/// Parse schema_v2 from json bytes.
pub fn parse_schema_v2(schema: &[u8]) -> Result<types::SchemaV2> {
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

#[derive(Deserialize, Default)]
#[serde(rename_all = "kebab-case", default)]
struct Types {
    #[serde(rename = "type")]
    typ: String,

    /// Only available when typ == "struct"
    fields: Vec<Field>,

    /// Only available when typ == "list"
    element_id: i32,
    element_required: bool,
    #[serde(deserialize_with = "string_or_struct")]
    element: Option<Box<Types>>,

    /// Only available when typ == "map"
    key_id: i32,
    #[serde(deserialize_with = "string_or_struct")]
    key: Option<Box<Types>>,
    value_id: i32,
    value_required: bool,
    #[serde(deserialize_with = "string_or_struct")]
    value: Option<Box<Types>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Field {
    id: i32,
    name: String,
    required: bool,
    #[serde(rename = "type", deserialize_with = "string_or_struct")]
    typ: Types,
    doc: Option<String>,
}

/// We need to support both `T` and `Box<T>` so we can't use
/// the `std::str::FromStr` trait directly.
trait FromStr {
    fn from_str(s: &str) -> Self;
}

impl FromStr for Types {
    fn from_str(s: &str) -> Self {
        Types {
            typ: s.to_string(),
            ..Default::default()
        }
    }
}

impl FromStr for Box<Types> {
    fn from_str(s: &str) -> Self {
        Box::new(Types {
            typ: s.to_string(),
            ..Default::default()
        })
    }
}

impl FromStr for Option<Box<Types>> {
    fn from_str(s: &str) -> Self {
        Some(Box::new(Types {
            typ: s.to_string(),
            ..Default::default()
        }))
    }
}

fn string_or_struct<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de> + FromStr,
    D: Deserializer<'de>,
{
    // This is a Visitor that forwards string types to T's `FromStr` impl and
    // forwards map types to T's `Deserialize` impl. The `PhantomData` is to
    // keep the compiler from complaining about T being an unused generic type
    // parameter. We need T in order to know the Value type for the Visitor
    // impl.
    struct StringOrStruct<T>(PhantomData<fn() -> T>);

    impl<'de, T> Visitor<'de> for StringOrStruct<T>
    where
        T: Deserialize<'de> + FromStr,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> Result<T, E>
        where
            E: de::Error,
        {
            Ok(FromStr::from_str(value))
        }

        fn visit_map<M>(self, map: M) -> Result<T, M::Error>
        where
            M: MapAccess<'de>,
        {
            // `MapAccessDeserializer` is a wrapper that turns a `MapAccess`
            // into a `Deserializer`, allowing it to be used as the input to T's
            // `Deserialize` implementation. T then deserializes itself using
            // the entries from the map visitor.
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(StringOrStruct(PhantomData))
}

impl TryFrom<&Types> for types::Any {
    type Error = anyhow::Error;

    fn try_from(v: &Types) -> Result<Self, Self::Error> {
        let t = match v.typ.as_str() {
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
            "struct" => {
                let raw_fields = &v.fields;

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
                let element_id = v.element_id;
                let element_required = v.element_required;
                let element_type = v
                    .element
                    .as_ref()
                    .ok_or_else(|| anyhow!("element type is required"))?;

                types::Any::List(types::List {
                    element_id,
                    element_required,
                    element_type: Box::new(element_type.as_ref().try_into()?),
                })
            }
            "map" => {
                let key_id = v.key_id;
                let key_type = v
                    .key
                    .as_ref()
                    .ok_or_else(|| anyhow!("map type key is required"))?;
                let value_id = v.value_id;
                let value_required = v.value_required;
                let value_type = v
                    .value
                    .as_ref()
                    .ok_or_else(|| anyhow!("map type value is required"))?;

                types::Any::Map(types::Map {
                    key_id,
                    key_type: Box::new(key_type.as_ref().try_into()?),
                    value_id,
                    value_required,
                    value_type: Box::new(value_type.as_ref().try_into()?),
                })
            }
            v => return Err(anyhow!("type {:?} is not valid schema type", v)),
        };

        Ok(t)
    }
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
        assert!(!schema.types.fields[0].required);
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

        let schema = parse_schema_v2(schema.as_bytes()).unwrap();

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
    fn test_parse_schema_v2_map() {
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

        let schema = parse_schema_v2(schema.as_bytes()).unwrap();

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
