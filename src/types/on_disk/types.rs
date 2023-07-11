use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use chrono::DateTime;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::NaiveTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::de;
use serde::de::MapAccess;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;
use uuid::Uuid;

use crate::types;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

#[derive(Deserialize, Default)]
#[serde(rename_all = "kebab-case", default)]
#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct Types {
    #[serde(rename = "type")]
    pub typ: String,

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
#[cfg_attr(test, derive(Debug, PartialEq))]
pub struct Field {
    id: i32,
    name: String,
    required: bool,
    #[serde(rename = "type", deserialize_with = "string_or_struct")]
    typ: Types,
    doc: Option<String>,
    initial_default: Option<serde_json::Value>,
    write_default: Option<serde_json::Value>,
}

impl TryFrom<Field> for types::Field {
    type Error = Error;

    fn try_from(v: Field) -> Result<Self> {
        let field_type = v.typ.try_into()?;

        let initial_default = v
            .initial_default
            .map(|v| parse_json_value(&field_type, v))
            .transpose()?;

        let write_default = v
            .write_default
            .map(|v| parse_json_value(&field_type, v))
            .transpose()?;

        let field = types::Field {
            id: v.id,
            name: v.name,
            required: v.required,
            field_type,
            comment: v.doc,
            initial_default,
            write_default,
        };

        Ok(field)
    }
}

/// parse_json_value will parse given json value into icelake AnyValue.
///
/// Reference: <https://iceberg.apache.org/spec/#json-single-value-serialization>
fn parse_json_value(expect_type: &types::Any, value: serde_json::Value) -> Result<types::AnyValue> {
    match expect_type {
        types::Any::Primitive(v) => match v {
            types::Primitive::Boolean => parse_json_value_to_boolean(value),
            types::Primitive::Int => parse_json_value_to_int(value),
            types::Primitive::Long => parse_json_value_to_long(value),
            types::Primitive::Float => parse_json_value_to_float(value),
            types::Primitive::Double => parse_json_value_to_double(value),
            types::Primitive::Decimal { .. } => parse_json_value_to_decimal(value),
            types::Primitive::Date => parse_json_value_to_date(value),
            types::Primitive::Time => parse_json_value_to_time(value),
            types::Primitive::Timestamp => parse_json_value_to_timestamp(value),
            types::Primitive::Timestampz => parse_json_value_to_timestampz(value),
            types::Primitive::String => parse_json_value_to_string(value),
            types::Primitive::Uuid => parse_json_value_to_uuid(value),
            types::Primitive::Fixed(size) => parse_json_value_to_fixed(value, *size),
            types::Primitive::Binary => parse_json_value_to_binary(value),
        },
        types::Any::Struct(v) => parse_json_value_to_struct(v, value),
        types::Any::List(v) => parse_json_value_to_list(v, value),
        types::Any::Map(v) => parse_json_value_to_map(v, value),
    }
}

/// JSON single-value serialization requires boolean been stored
/// as bool.
#[inline]
fn parse_json_value_to_boolean(value: serde_json::Value) -> Result<types::AnyValue> {
    match value {
        serde_json::Value::Bool(v) => Ok(types::AnyValue::Primitive(
            types::PrimitiveValue::Boolean(v),
        )),
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type `boolean` but got {:?}", value),
        )),
    }
}

/// JSON single-value serialization requires int been stored
/// as number.
#[inline]
fn parse_json_value_to_int(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`Int`";

    match value {
        serde_json::Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                if v > i32::MAX as i64 {
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!(
                            "expect type {:?} but got {:?} is exceeding i32 range",
                            expect_type, v
                        ),
                    ));
                }

                if v < i32::MIN as i64 {
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!(
                            "expect type {:?} but got is exceeding i32 range {:?}",
                            expect_type, v
                        ),
                    ));
                }

                Ok(types::AnyValue::Primitive(types::PrimitiveValue::Int(
                    v as i32,
                )))
            } else {
                Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("expect type {:?} but got {:?}", expect_type, v),
                ))
            }
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires long been stored
/// as number.
#[inline]
fn parse_json_value_to_long(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`Long`";

    match value {
        serde_json::Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                Ok(types::AnyValue::Primitive(types::PrimitiveValue::Long(v)))
            } else {
                Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("expect type {:?} but got {:?}", expect_type, v),
                ))
            }
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires float been stored
/// as number.
#[inline]
fn parse_json_value_to_float(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`Float`";

    match value {
        serde_json::Value::Number(v) => {
            if let Some(v) = v.as_f64() {
                if v > f32::MAX as f64 {
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!(
                            "expect type {:?} but got {:?} is exceeding f32 range",
                            expect_type, v
                        ),
                    ));
                }

                if v < f32::MIN as f64 {
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!(
                            "expect type {:?} but got is exceeding f32 range {:?}",
                            expect_type, v
                        ),
                    ));
                }

                Ok(types::AnyValue::Primitive(types::PrimitiveValue::Float(
                    v as f32,
                )))
            } else {
                Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("expect type {:?} but got {:?}", expect_type, v),
                ))
            }
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires float been stored
/// as number.
#[inline]
fn parse_json_value_to_double(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`Double`";

    match value {
        serde_json::Value::Number(v) => {
            if let Some(v) = v.as_f64() {
                Ok(types::AnyValue::Primitive(types::PrimitiveValue::Double(v)))
            } else {
                Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("expect type {:?} but got {:?}", expect_type, v),
                ))
            }
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires Decimal been stored
/// as string like `14.20`, `2E+20`.
///
/// # TODO
///
/// we should check the precision and scale.
#[inline]
fn parse_json_value_to_decimal(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`Decimal`";

    match value {
        serde_json::Value::String(v) => {
            let d = Decimal::from_str(&v).map_err(|err| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("parse decimal from json failed: {v}"),
                )
                .set_source(err)
            })?;

            // TODO: we should check the precision and scale here.
            Ok(types::AnyValue::Primitive(types::PrimitiveValue::Decimal(
                d,
            )))
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires Date been stored
/// as string in ISO-8601 standard date, like `2017-11-16`.
#[inline]
fn parse_json_value_to_date(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`Date`";

    match value {
        serde_json::Value::String(v) => {
            let date = NaiveDate::from_str(&v).map_err(|err| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("parse date from json failed: {v}"),
                )
                .set_source(err)
            })?;

            Ok(types::AnyValue::Primitive(types::PrimitiveValue::Date(
                date,
            )))
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires Time been stored
/// as string in ISO-8601 standard time, like `22:31:08.123456`.
#[inline]
fn parse_json_value_to_time(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`Time`";

    match value {
        serde_json::Value::String(v) => {
            let time = NaiveTime::from_str(&v).map_err(|err| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("parse time from json failed: {v}"),
                )
                .set_source(err)
            })?;

            Ok(types::AnyValue::Primitive(types::PrimitiveValue::Time(
                time,
            )))
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires Timestamp been
/// stored as string in ISO-8601 standard time, like
/// `2017-11-16T22:31:08.123456`.
#[inline]
fn parse_json_value_to_timestamp(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`Timestamp`";

    match value {
        serde_json::Value::String(v) => {
            let v = NaiveDateTime::from_str(&v).map_err(|err| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("parse timestamp from json failed: {v}"),
                )
                .set_source(err)
            })?;

            Ok(types::AnyValue::Primitive(
                types::PrimitiveValue::Timestamp(v),
            ))
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires Timestampz been
/// stored as string in ISO-8601 standard time with timezone, like
/// `2017-11-16T22:31:08.123456+00:00`.
#[inline]
fn parse_json_value_to_timestampz(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`Timestampz`";

    match value {
        serde_json::Value::String(v) => {
            let v = DateTime::<Utc>::from_str(&v).map_err(|err| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("parse timestampz from json failed: {v}"),
                )
                .set_source(err)
            })?;

            Ok(types::AnyValue::Primitive(
                types::PrimitiveValue::Timestampz(v),
            ))
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires String been
/// stored as string.
#[inline]
fn parse_json_value_to_string(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`String`";

    match value {
        serde_json::Value::String(v) => {
            Ok(types::AnyValue::Primitive(types::PrimitiveValue::String(v)))
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires Uuid been
/// stored as lowercase uuid string, like `f79c3e09-677c-4bbd-a479-3f349cb785e7`
#[inline]
fn parse_json_value_to_uuid(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`Uuid`";

    match value {
        serde_json::Value::String(v) => {
            let v = Uuid::from_str(&v).map_err(|err| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("parse uuid from json failed: {v}"),
                )
                .set_source(err)
            })?;

            Ok(types::AnyValue::Primitive(types::PrimitiveValue::Uuid(v)))
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires Fixed been
/// stored as hexadecimal string like `000102ff`.
#[inline]
fn parse_json_value_to_fixed(value: serde_json::Value, size: u64) -> Result<types::AnyValue> {
    let expect_type = "`Fixed`";

    match value {
        serde_json::Value::String(v) => {
            let mut bs = vec![0; v.as_bytes().len() / 2];
            faster_hex::hex_decode(v.as_bytes(), &mut bs).map_err(|err| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("parse fixed from json failed: {v}"),
                )
                .set_source(err)
            })?;

            if bs.len() != size as usize {
                return Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!(
                        "expect type {:?} but got {:?} is not equal to fixed size {}",
                        expect_type, v, size
                    ),
                ));
            }

            Ok(types::AnyValue::Primitive(types::PrimitiveValue::Fixed(bs)))
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires Binary been
/// stored as hexadecimal string lie `000102ff`.
#[inline]
fn parse_json_value_to_binary(value: serde_json::Value) -> Result<types::AnyValue> {
    let expect_type = "`Binary`";

    match value {
        serde_json::Value::String(v) => {
            let mut bs = vec![0; v.as_bytes().len() / 2];
            faster_hex::hex_decode(v.as_bytes(), &mut bs).map_err(|err| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("parse fixed from json failed: {v}"),
                )
                .set_source(err)
            })?;

            Ok(types::AnyValue::Primitive(types::PrimitiveValue::Binary(
                bs,
            )))
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires Struct been
/// stored as a JSON object.
///
/// - The keys are field id in string
/// - The values are stored using this JSON single-value format
///
/// For example: `{"1": 1, "2": "bar"}`
fn parse_json_value_to_struct(
    expect_struct: &types::Struct,
    value: serde_json::Value,
) -> Result<types::AnyValue> {
    let fields = expect_struct
        .fields
        .iter()
        .map(|v| (v.id, &v.field_type))
        .collect::<HashMap<_, _>>();

    let mut values = HashMap::with_capacity(fields.len());

    match value {
        serde_json::Value::Object(o) => {
            for (key, value) in o {
                let key: i32 = key.parse().map_err(|err| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("struct field id must be integer, but got: {:?}", key),
                    )
                    .set_source(err)
                })?;

                let expect_type = fields.get(&key).ok_or_else(|| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("expect filed id {:?} but not exist", key),
                    )
                })?;

                let value = parse_json_value(expect_type, value)?;

                values.insert(key, value);
            }
        }
        _ => {
            return Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("expect type object but got {:?}", value),
            ))
        }
    }

    Ok(types::AnyValue::Struct(values))
}

/// JSON single-value serialization requires List been
/// stored as a JSON array of values that are serialized
/// using this JSON single-value format
///
/// For example: `[1, 2, 3]`
fn parse_json_value_to_list(
    expect_list: &types::List,
    value: serde_json::Value,
) -> Result<types::AnyValue> {
    let expect_type = &expect_list.element_type;

    match value {
        serde_json::Value::Array(v) => {
            let mut values = Vec::with_capacity(v.len());
            for v in v {
                let v = parse_json_value(expect_type, v)?;
                values.push(v);
            }

            Ok(types::AnyValue::List(values))
        }
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("expect type {:?} but got {:?}", expect_type, value),
        )),
    }
}

/// JSON single-value serialization requires Map been
/// stored as arrays of keys and values; individual keys and values
/// are serialized using this JSON single-value format
///
/// For example: `{ "keys": ["a", "b"], "values": [1, 2] }`
fn parse_json_value_to_map(
    expect_map: &types::Map,
    value: serde_json::Value,
) -> Result<types::AnyValue> {
    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    struct MapValue {
        keys: Vec<serde_json::Value>,
        values: Vec<serde_json::Value>,
    }

    let mv: MapValue = serde_json::from_value(value)?;

    let key_type = &expect_map.key_type;
    let mut keys = Vec::with_capacity(mv.keys.len());

    for v in mv.keys {
        keys.push(parse_json_value(key_type, v)?)
    }

    let value_type = &expect_map.value_type;
    let mut values = Vec::with_capacity(mv.values.len());

    for v in mv.values {
        values.push(parse_json_value(value_type, v)?)
    }

    Ok(types::AnyValue::Map { keys, values })
}

/// We need to support both `T` and `Box<T>` so we can't use
/// the `std::str::FromStr` trait directly.
pub trait FromTypesStr {
    fn from_str(s: &str) -> Self;
}

impl FromTypesStr for Types {
    fn from_str(s: &str) -> Self {
        Types {
            typ: s.to_string(),
            ..Default::default()
        }
    }
}

impl FromTypesStr for Box<Types> {
    fn from_str(s: &str) -> Self {
        Box::new(Types {
            typ: s.to_string(),
            ..Default::default()
        })
    }
}

impl FromTypesStr for Option<Box<Types>> {
    fn from_str(s: &str) -> Self {
        Some(Box::new(Types {
            typ: s.to_string(),
            ..Default::default()
        }))
    }
}

pub fn string_or_struct<'de, T, D>(deserializer: D) -> std::result::Result<T, D::Error>
where
    T: Deserialize<'de> + FromTypesStr,
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
        T: Deserialize<'de> + FromTypesStr,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> std::result::Result<T, E>
        where
            E: de::Error,
        {
            Ok(FromTypesStr::from_str(value))
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
