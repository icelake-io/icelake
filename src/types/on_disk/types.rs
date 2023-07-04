use std::fmt;
use std::marker::PhantomData;

use serde::de;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;

use crate::types;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

#[derive(Deserialize, Default)]
#[serde(rename_all = "kebab-case", default)]
pub struct Types {
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

impl TryFrom<Types> for types::Any {
    type Error = Error;

    fn try_from(v: Types) -> Result<Self> {
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
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::IcebergDataInvalid,
                            format!("fixed type {v:?} is invalid"),
                        )
                        .set_source(err)
                    })?;

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
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("decimal type {v:?} is invalid"),
                    ));
                }

                let precision = parts[0].parse().map_err(|err| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("decimal type {v:?} is invalid"),
                    )
                    .set_source(err)
                })?;
                let scale = parts[1].parse().map_err(|err| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("decimal type {v:?} is invalid"),
                    )
                    .set_source(err)
                })?;

                types::Any::Primitive(types::Primitive::Decimal { precision, scale })
            }
            "struct" => {
                let raw_fields = v.fields;

                let mut fields = Vec::with_capacity(raw_fields.len());
                for f in raw_fields {
                    let field = types::Field {
                        id: f.id,
                        name: f.name.clone(),
                        required: f.required,
                        field_type: f.typ.try_into()?,
                        comment: f.doc.clone(),
                        initial_default: None,
                        write_default: None,
                    };

                    fields.push(field);
                }

                types::Any::Struct(types::Struct { fields })
            }
            "list" => {
                let element_id = v.element_id;
                let element_required = v.element_required;
                let element_type = v.element.ok_or_else(|| {
                    Error::new(ErrorKind::IcebergDataInvalid, "element type is required")
                })?;

                types::Any::List(types::List {
                    element_id,
                    element_required,
                    element_type: Box::new((*element_type).try_into()?),
                })
            }
            "map" => {
                let key_id = v.key_id;
                let key_type = v.key.ok_or_else(|| {
                    Error::new(ErrorKind::IcebergDataInvalid, "map type key is required")
                })?;
                let value_id = v.value_id;
                let value_required = v.value_required;
                let value_type = v.value.ok_or_else(|| {
                    Error::new(ErrorKind::IcebergDataInvalid, "map type value is required")
                })?;

                types::Any::Map(types::Map {
                    key_id,
                    key_type: Box::new((*key_type).try_into()?),
                    value_id,
                    value_required,
                    value_type: Box::new((*value_type).try_into()?),
                })
            }
            v => {
                return Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("type {:?} is not valid schema type", v),
                ))
            }
        };

        Ok(t)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Field {
    id: i32,
    name: String,
    required: bool,
    #[serde(rename = "type", deserialize_with = "string_or_struct")]
    typ: Types,
    doc: Option<String>,
}

impl TryFrom<Field> for types::Field {
    type Error = Error;

    fn try_from(v: Field) -> Result<Self> {
        let field = types::Field {
            id: v.id,
            name: v.name,
            required: v.required,
            field_type: v.typ.try_into()?,
            comment: v.doc,
            initial_default: None,
            write_default: None,
        };

        Ok(field)
    }
}

/// We need to support both `T` and `Box<T>` so we can't use
/// the `std::str::FromStr` trait directly.
pub trait FromStr {
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

pub fn string_or_struct<'de, T, D>(deserializer: D) -> std::result::Result<T, D::Error>
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

        fn visit_str<E>(self, value: &str) -> std::result::Result<T, E>
        where
            E: de::Error,
        {
            Ok(FromStr::from_str(value))
        }

        fn visit_map<M>(self, map: M) -> std::result::Result<T, M::Error>
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
