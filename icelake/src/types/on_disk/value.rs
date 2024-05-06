use crate::{types::in_memory, Error, ErrorKind};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime};
use in_memory::{Any, AnyValue, Primitive, PrimitiveValue, StructValue};
use serde::{
    de::Visitor,
    ser::{SerializeMap, SerializeSeq, SerializeStruct},
    Deserialize, Serialize, Serializer,
};

#[derive(Debug, PartialEq, Clone)]
pub enum Value {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Option<Value>>),
    Map(Vec<(Value, Option<Value>)>),
    StringMap(Vec<(String, Option<Value>)>),
    Record {
        required: Vec<(String, Value)>,
        optional: Vec<(String, Option<Value>)>,
    },
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Value::Boolean(v) => serializer.serialize_bool(*v),
            Value::Int(v) => serializer.serialize_i32(*v),
            Value::Long(v) => serializer.serialize_i64(*v),
            Value::Float(v) => serializer.serialize_f32(*v),
            Value::Double(v) => serializer.serialize_f64(*v),
            Value::String(v) => serializer.serialize_str(v),
            Value::Bytes(v) => serializer.serialize_bytes(v),
            Value::List(v) => v.serialize(serializer),
            Value::Map(v) => {
                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for (k, v) in v {
                    let record = Value::Record {
                        required: vec![("key".to_string(), k.clone())],
                        optional: vec![("value".to_string(), v.clone())],
                    };
                    seq.serialize_element(&record)?;
                }
                seq.end()
            }
            Value::Record { required, optional } => {
                let len = required.len() + optional.len();
                let mut record = serializer.serialize_struct("", len)?;
                for (k, v) in required {
                    record.serialize_field(Box::leak(k.clone().into_boxed_str()), v)?;
                }
                for (k, v) in optional {
                    record.serialize_field(Box::leak(k.clone().into_boxed_str()), v)?;
                }
                record.end()
            }
            Value::StringMap(v) => {
                let mut map = serializer.serialize_map(Some(v.len()))?;
                for (k, v) in v {
                    map.serialize_entry(&k, &v)?;
                }
                map.end()
            }
            Value::Null => unreachable!(),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;
        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("expect")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Boolean(v))
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Int(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Long(v))
            }

            /// Used in json
            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Long(v as i64))
            }

            fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Float(v))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Double(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::String(v.to_string()))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Bytes(v.to_vec()))
            }

            fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::String(v.to_string()))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Null)
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut required = Vec::new();
                while let Some(key) = map.next_key::<String>()? {
                    let value = map.next_value::<Value>()?;
                    required.push((key, value));
                }
                Ok(Value::Record {
                    required,
                    optional: Vec::new(),
                })
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut list = Vec::new();
                while let Some(value) = seq.next_element::<Value>()? {
                    list.push(Some(value));
                }
                Ok(Value::List(list))
            }
        }
        deserializer.deserialize_any(ValueVisitor)
    }
}

impl Value {
    fn as_long(&self) -> i64 {
        if let Value::Long(v) = self {
            *v
        } else {
            unreachable!()
        }
    }

    fn from_primitive(value: in_memory::PrimitiveValue) -> Self {
        match value {
            in_memory::PrimitiveValue::Boolean(v) => Self::Boolean(v),
            in_memory::PrimitiveValue::Int(v) => Self::Int(v),
            in_memory::PrimitiveValue::Long(v) => Self::Long(v),
            in_memory::PrimitiveValue::Float(v) => Self::Float(v.0),
            in_memory::PrimitiveValue::Double(v) => Self::Double(v.0),
            in_memory::PrimitiveValue::Decimal(v) => Self::Bytes(v.to_be_bytes().to_vec()),
            in_memory::PrimitiveValue::Date(v) => Self::Int(v.num_days_from_ce()),
            in_memory::PrimitiveValue::Time(v) => Self::Long(
                NaiveDateTime::new(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(), v)
                    .and_utc()
                    .timestamp_micros(),
            ),
            in_memory::PrimitiveValue::Timestamp(v) => Self::Long(v.and_utc().timestamp_micros()),
            in_memory::PrimitiveValue::Timestampz(v) => Self::Long(v.timestamp_micros()),
            in_memory::PrimitiveValue::String(v) => Self::String(v),
            in_memory::PrimitiveValue::Uuid(v) => Self::String(v.to_string()),
            in_memory::PrimitiveValue::Fixed(v) => Self::Bytes(v),
            in_memory::PrimitiveValue::Binary(v) => Self::Bytes(v),
        }
    }

    pub fn into_memory(self, ty: &Any) -> Result<Option<in_memory::AnyValue>, Error> {
        let invalid_err = || {
            Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("fail convert to {:?}", ty),
            )
        };
        match self {
            Value::Null => Ok(None),
            Value::Boolean(v) => Ok(Some(in_memory::AnyValue::Primitive(
                in_memory::PrimitiveValue::Boolean(v),
            ))),
            Value::Int(v) => match ty {
                Any::Primitive(Primitive::Int) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Int(v),
                ))),
                Any::Primitive(Primitive::Date) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Date(
                        NaiveDate::from_num_days_from_ce_opt(v).ok_or_else(|| {
                            Error::new(
                                ErrorKind::IcebergDataInvalid,
                                format!("invalid date: {}", v),
                            )
                        })?,
                    ),
                ))),
                _ => Err(invalid_err()),
            },
            Value::Long(v) => match ty {
                Any::Primitive(Primitive::Int) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Int(v as i32),
                ))),
                Any::Primitive(Primitive::Long) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Long(v),
                ))),
                Any::Primitive(Primitive::Time) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Time(
                        DateTime::from_timestamp_micros(v)
                            .ok_or_else(invalid_err)?
                            .naive_utc()
                            .time(),
                    ),
                ))),
                Any::Primitive(Primitive::Timestamp) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Timestamp(
                        DateTime::from_timestamp_micros(v)
                            .ok_or_else(invalid_err)?
                            .naive_utc(),
                    ),
                ))),
                Any::Primitive(Primitive::Timestampz) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Timestampz(
                        DateTime::from_timestamp_micros(v).ok_or_else(invalid_err)?,
                    ),
                ))),
                Any::Primitive(Primitive::Date) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Date(
                        NaiveDate::from_num_days_from_ce_opt(v as i32).ok_or_else(invalid_err)?,
                    ),
                ))),
                _ => Err(invalid_err()),
            },
            Value::Float(v) => match ty {
                Any::Primitive(Primitive::Float) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Float(v.into()),
                ))),
                Any::Primitive(Primitive::Double) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Double((v as f64).into()),
                ))),
                _ => Err(invalid_err()),
            },
            Value::Double(v) => match ty {
                Any::Primitive(Primitive::Float) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Float((v as f32).into()),
                ))),
                Any::Primitive(Primitive::Double) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Double(v.into()),
                ))),
                _ => Err(invalid_err()),
            },
            Value::String(v) => match ty {
                Any::Primitive(Primitive::String) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::String(v),
                ))),
                Any::Primitive(Primitive::Uuid) => Ok(Some(in_memory::AnyValue::Primitive(
                    in_memory::PrimitiveValue::Uuid(
                        uuid::Uuid::parse_str(&v).map_err(|err| invalid_err().set_source(err))?,
                    ),
                ))),
                _ => Err(invalid_err()),
            },
            Value::Bytes(_) => Err(invalid_err()),
            Value::List(v) => match ty {
                Any::List(ty) => {
                    let ty = &ty.element_type;
                    Ok(Some(in_memory::AnyValue::List(
                        v.into_iter()
                            .map(|v| {
                                if let Some(v) = v {
                                    v.into_memory(ty)
                                } else {
                                    Ok(None)
                                }
                            })
                            .collect::<Result<_, Error>>()?,
                    )))
                }
                Any::Map(ref map_ty) => {
                    let key_ty = &map_ty.key_type;
                    let value_ty = &map_ty.value_type;
                    let mut keys = Vec::with_capacity(v.len());
                    let mut values = Vec::with_capacity(v.len());
                    for v in v {
                        let pair = v.ok_or_else(invalid_err)?;
                        if let Value::Record {
                            required,
                            optional: _,
                        } = pair
                        {
                            assert!(required.len() == 2);
                            let key = required
                                .iter()
                                .find(|(k, _)| k == "key")
                                .ok_or_else(invalid_err)?
                                .1
                                .clone();
                            let value = required
                                .iter()
                                .find(|(k, _)| k == "value")
                                .ok_or_else(invalid_err)?
                                .1
                                .clone();
                            let key = key.into_memory(key_ty)?.ok_or_else(invalid_err)?;
                            let value = value.into_memory(value_ty)?;
                            keys.push(key);
                            values.push(value);
                        } else {
                            unreachable!()
                        }
                    }
                    Ok(Some(in_memory::AnyValue::Map { keys, values }))
                }
                Any::Primitive(Primitive::Binary) => {
                    let bytes = v.into_iter().map(|v| v.unwrap().as_long() as u8).collect();
                    Ok(Some(AnyValue::Primitive(PrimitiveValue::Binary(bytes))))
                }
                Any::Primitive(Primitive::Fixed(len)) => {
                    let bytes: Vec<u8> =
                        v.into_iter().map(|v| v.unwrap().as_long() as u8).collect();
                    if bytes.len() as u64 > *len {
                        return Err(invalid_err());
                    }
                    Ok(Some(AnyValue::Primitive(PrimitiveValue::Fixed(bytes))))
                }
                Any::Primitive(Primitive::Decimal {
                    precision: _,
                    scale: _,
                }) => {
                    let bytes: Vec<u8> =
                        v.into_iter().map(|v| v.unwrap().as_long() as u8).collect();
                    let bytes: [u8; 16] = bytes.try_into().map_err(|_| invalid_err())?;
                    let mantissa: i128 = i128::from_be_bytes(bytes);
                    Ok(Some(AnyValue::Primitive(PrimitiveValue::Decimal(mantissa))))
                }
                _ => Err(invalid_err()),
            },
            Value::Map(v) => {
                if let Any::Map(ref map_ty) = ty {
                    let key_type = &map_ty.key_type;
                    let value_type = &map_ty.value_type;
                    let mut keys = Vec::with_capacity(v.len());
                    let mut values = Vec::with_capacity(v.len());
                    for (k, v) in v {
                        keys.push(k.into_memory(key_type)?.ok_or_else(invalid_err)?);
                        values.push(v.map(|v| v.into_memory(value_type)).transpose()?.flatten());
                    }
                    Ok(Some(in_memory::AnyValue::Map { keys, values }))
                } else {
                    Err(invalid_err())
                }
            }
            Value::Record {
                required,
                optional: _,
            } => {
                if let Any::Struct(ref struct_ty) = ty {
                    let mut builder = in_memory::StructValueBuilder::new(struct_ty.clone());
                    for (field_name, value) in required {
                        let field = struct_ty
                            .lookup_field_by_name(field_name.as_str())
                            .ok_or_else(invalid_err)?;
                        let value = value.into_memory(&field.field_type)?;
                        builder.add_field(field.id, value)?;
                    }
                    Ok(Some(in_memory::AnyValue::Struct(builder.build()?)))
                } else if let Any::Map(map_ty) = ty {
                    assert!(*map_ty.key_type == Any::Primitive(Primitive::String));
                    let mut keys = Vec::with_capacity(required.len());
                    let mut values = Vec::with_capacity(required.len());
                    for (k, v) in required {
                        let value = v.into_memory(&map_ty.value_type)?;
                        keys.push(AnyValue::Primitive(PrimitiveValue::String(k)));
                        values.push(value);
                    }
                    Ok(Some(in_memory::AnyValue::Map { keys, values }))
                } else {
                    Err(invalid_err())
                }
            }
            Value::StringMap(_) => Err(invalid_err()),
        }
    }
}

impl From<in_memory::AnyValue> for Value {
    fn from(value: in_memory::AnyValue) -> Self {
        match value {
            in_memory::AnyValue::Primitive(v) => Self::from_primitive(v),
            in_memory::AnyValue::Struct(v) => {
                let mut required_fields = Vec::new();
                let mut optional_fields = Vec::new();
                for (_, value, key, required) in v.iter() {
                    if required {
                        required_fields.push((key.to_string(), value.unwrap().clone().into()));
                    } else {
                        optional_fields.push((key.to_string(), value.map(|v| v.clone().into())));
                    }
                }
                Self::Record {
                    required: required_fields,
                    optional: optional_fields,
                }
            }
            in_memory::AnyValue::List(v) => {
                Value::List(v.into_iter().map(|v| v.map(|v| v.into())).collect())
            }
            in_memory::AnyValue::Map { keys, values } => {
                if let Some(in_memory::AnyValue::Primitive(PrimitiveValue::String(_))) =
                    keys.first()
                {
                    Value::StringMap(
                        keys.into_iter()
                            .zip(values)
                            .map(|(k, v)| {
                                let k = if let AnyValue::Primitive(PrimitiveValue::String(s)) = k {
                                    s
                                } else {
                                    unreachable!()
                                };
                                (k, v.map(|v| v.into()))
                            })
                            .collect(),
                    )
                } else {
                    Value::Map(
                        keys.into_iter()
                            .zip(values)
                            .map(|(k, v)| (k.into(), v.map(|v| v.into())))
                            .collect(),
                    )
                }
            }
        }
    }
}

impl From<StructValue> for Value {
    fn from(value: StructValue) -> Self {
        AnyValue::Struct(value).into()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use chrono::{DateTime, NaiveDate};

    use crate::types::{
        self, to_avro::to_avro_schema, AnyValue, List, Map, PrimitiveValue, Schema, Struct,
        StructValue, StructValueBuilder,
    };

    use super::Value;

    /// # TODO
    /// rust avro can't support:
    /// - Decimal
    /// - Binary
    /// - Fixed
    /// Ignore them now.
    /// After support them, we can integrate this function with `struct_type_for_json`.
    fn struct_type_for_avro() -> Arc<Struct> {
        let struct_type = types::Struct::new(vec![
            types::Field::required(1, "v_int", types::Any::Primitive(types::Primitive::Int)).into(),
            types::Field::required(2, "v_long", types::Any::Primitive(types::Primitive::Long))
                .into(),
            types::Field::required(
                3,
                "v_string",
                types::Any::Primitive(types::Primitive::String),
            )
            .into(),
            types::Field::required(4, "v_float", types::Any::Primitive(types::Primitive::Float))
                .into(),
            types::Field::required(
                5,
                "v_double",
                types::Any::Primitive(types::Primitive::Double),
            )
            .into(),
            types::Field::required(
                6,
                "v_boolean",
                types::Any::Primitive(types::Primitive::Boolean),
            )
            .into(),
            types::Field::required(7, "v_date", types::Any::Primitive(types::Primitive::Date))
                .into(),
            types::Field::required(8, "v_time", types::Any::Primitive(types::Primitive::Time))
                .into(),
            types::Field::optional(
                9,
                "v_timestamp",
                types::Any::Primitive(types::Primitive::Timestamp),
            )
            .into(),
            types::Field::optional(10, "v_uuid", types::Any::Primitive(types::Primitive::Uuid))
                .into(),
            types::Field::optional(
                11,
                "v_timestampz",
                types::Any::Primitive(types::Primitive::Timestampz),
            )
            .into(),
        ]);
        Arc::new(struct_type)
    }

    fn struct_type_for_json() -> Arc<Struct> {
        let struct_type = types::Struct::new(vec![
            types::Field::required(1, "v_int", types::Any::Primitive(types::Primitive::Int)).into(),
            types::Field::required(2, "v_long", types::Any::Primitive(types::Primitive::Long))
                .into(),
            types::Field::required(
                3,
                "v_string",
                types::Any::Primitive(types::Primitive::String),
            )
            .into(),
            types::Field::required(4, "v_float", types::Any::Primitive(types::Primitive::Float))
                .into(),
            types::Field::required(
                5,
                "v_double",
                types::Any::Primitive(types::Primitive::Double),
            )
            .into(),
            types::Field::required(
                6,
                "v_boolean",
                types::Any::Primitive(types::Primitive::Boolean),
            )
            .into(),
            types::Field::required(7, "v_date", types::Any::Primitive(types::Primitive::Date))
                .into(),
            types::Field::required(8, "v_time", types::Any::Primitive(types::Primitive::Time))
                .into(),
            types::Field::optional(
                9,
                "v_timestamp",
                types::Any::Primitive(types::Primitive::Timestamp),
            )
            .into(),
            types::Field::optional(10, "v_uuid", types::Any::Primitive(types::Primitive::Uuid))
                .into(),
            types::Field::optional(
                11,
                "v_timestampz",
                types::Any::Primitive(types::Primitive::Timestampz),
            )
            .into(),
            types::Field::optional(
                12,
                "v_decimal",
                types::Any::Primitive(types::Primitive::Decimal {
                    precision: 20,
                    scale: 5,
                }),
            )
            .into(),
            types::Field::optional(
                13,
                "v_binary",
                types::Any::Primitive(types::Primitive::Binary),
            )
            .into(),
            types::Field::optional(
                14,
                "v_fixed",
                types::Any::Primitive(types::Primitive::Fixed(10)),
            )
            .into(),
        ]);
        Arc::new(struct_type)
    }

    fn compose_strcut_type() -> (Arc<Struct>, Arc<Struct>) {
        let simple_struct_type = Arc::new(types::Struct::new(vec![
            types::Field::required(3, "v_int", types::Any::Primitive(types::Primitive::Int)).into(),
            types::Field::required(4, "v_long", types::Any::Primitive(types::Primitive::Long))
                .into(),
        ]));
        let struct_type = types::Struct::new(vec![
            types::Field::required(
                1,
                "v_list",
                types::Any::List(List {
                    element_id: 2,
                    element_required: true,
                    element_type: Box::new(types::Any::Primitive(types::Primitive::Int)),
                }),
            )
            .into(),
            types::Field::required(
                2,
                "v_struct",
                types::Any::Struct(simple_struct_type.clone()),
            )
            .into(),
            types::Field::required(
                5,
                "v_map",
                types::Any::Map(Map {
                    key_id: 6,
                    key_type: Box::new(types::Any::Primitive(types::Primitive::Long)),
                    value_id: 7,
                    value_required: true,
                    value_type: Box::new(types::Any::Primitive(types::Primitive::String)),
                }),
            )
            .into(),
            types::Field::required(
                6,
                "v_str_map",
                types::Any::Map(Map {
                    key_id: 6,
                    key_type: Box::new(types::Any::Primitive(types::Primitive::String)),
                    value_id: 7,
                    value_required: true,
                    value_type: Box::new(types::Any::Primitive(types::Primitive::String)),
                }),
            )
            .into(),
        ]);
        (simple_struct_type, Arc::new(struct_type))
    }

    fn compose_struct() -> (Arc<Struct>, StructValue) {
        let (simple_struct_type, struct_type) = compose_strcut_type();

        let mut builder = StructValueBuilder::new(simple_struct_type);
        builder
            .add_field(3, Some(AnyValue::Primitive(PrimitiveValue::Int(1))))
            .unwrap();
        builder
            .add_field(4, Some(AnyValue::Primitive(PrimitiveValue::Long(2))))
            .unwrap();
        let simple_struct_value = builder.build().unwrap();

        let mut builder = StructValueBuilder::new(struct_type.clone());
        builder
            .add_field(
                1,
                Some(AnyValue::List(vec![
                    Some(AnyValue::Primitive(PrimitiveValue::Int(1))),
                    Some(AnyValue::Primitive(PrimitiveValue::Int(2))),
                ])),
            )
            .unwrap();
        builder
            .add_field(2, Some(AnyValue::Struct(simple_struct_value)))
            .unwrap();
        builder
            .add_field(
                5,
                Some(AnyValue::Map {
                    keys: vec![
                        AnyValue::Primitive(PrimitiveValue::Long(1)),
                        AnyValue::Primitive(PrimitiveValue::Long(2)),
                    ],
                    values: vec![
                        Some(AnyValue::Primitive(PrimitiveValue::String("1".to_string()))),
                        Some(AnyValue::Primitive(PrimitiveValue::String("2".to_string()))),
                    ],
                }),
            )
            .unwrap();
        builder
            .add_field(
                6,
                Some(AnyValue::Map {
                    keys: vec![
                        AnyValue::Primitive(PrimitiveValue::String("1".to_string())),
                        AnyValue::Primitive(PrimitiveValue::String("2".to_string())),
                    ],
                    values: vec![
                        Some(AnyValue::Primitive(PrimitiveValue::String("1".to_string()))),
                        Some(AnyValue::Primitive(PrimitiveValue::String("2".to_string()))),
                    ],
                }),
            )
            .unwrap();

        (struct_type, builder.build().unwrap())
    }

    fn struct_for_avro() -> (Arc<Struct>, StructValue) {
        let struct_type = struct_type_for_avro();
        let mut builder = StructValueBuilder::new(struct_type.clone());
        builder
            .add_field(1, Some(AnyValue::Primitive(PrimitiveValue::Int(1))))
            .unwrap();
        builder
            .add_field(2, Some(AnyValue::Primitive(PrimitiveValue::Long(2))))
            .unwrap();
        builder
            .add_field(
                3,
                Some(AnyValue::Primitive(PrimitiveValue::String("3".to_string()))),
            )
            .unwrap();
        builder
            .add_field(
                4,
                Some(AnyValue::Primitive(PrimitiveValue::Float(4.0.into()))),
            )
            .unwrap();
        builder
            .add_field(
                5,
                Some(AnyValue::Primitive(PrimitiveValue::Double(5.0.into()))),
            )
            .unwrap();
        builder
            .add_field(6, Some(AnyValue::Primitive(PrimitiveValue::Boolean(true))))
            .unwrap();
        builder
            .add_field(
                7,
                Some(AnyValue::Primitive(PrimitiveValue::Date(
                    NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
                ))),
            )
            .unwrap();
        builder
            .add_field(
                8,
                Some(AnyValue::Primitive(PrimitiveValue::Time(
                    DateTime::from_timestamp(0, 1000)
                        .unwrap()
                        .naive_utc()
                        .time(),
                ))),
            )
            .unwrap();
        builder.add_field(9, None).unwrap();
        builder
            .add_field(
                10,
                Some(AnyValue::Primitive(PrimitiveValue::Uuid(
                    uuid::Uuid::new_v4(),
                ))),
            )
            .unwrap();
        builder
            .add_field(
                11,
                Some(AnyValue::Primitive(PrimitiveValue::Timestampz(
                    chrono::DateTime::from_timestamp(0, 1000).unwrap(),
                ))),
            )
            .unwrap();
        (struct_type, builder.build().unwrap())
    }

    fn struct_for_json() -> (Arc<Struct>, StructValue) {
        let struct_type = struct_type_for_json();
        let mut builder = StructValueBuilder::new(struct_type.clone());
        builder
            .add_field(1, Some(AnyValue::Primitive(PrimitiveValue::Int(1))))
            .unwrap();
        builder
            .add_field(2, Some(AnyValue::Primitive(PrimitiveValue::Long(2))))
            .unwrap();
        builder
            .add_field(
                3,
                Some(AnyValue::Primitive(PrimitiveValue::String("3".to_string()))),
            )
            .unwrap();
        builder
            .add_field(
                4,
                Some(AnyValue::Primitive(PrimitiveValue::Float(4.0.into()))),
            )
            .unwrap();
        builder
            .add_field(
                5,
                Some(AnyValue::Primitive(PrimitiveValue::Double(5.0.into()))),
            )
            .unwrap();
        builder
            .add_field(6, Some(AnyValue::Primitive(PrimitiveValue::Boolean(true))))
            .unwrap();
        builder
            .add_field(
                7,
                Some(AnyValue::Primitive(PrimitiveValue::Date(
                    NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
                ))),
            )
            .unwrap();
        builder
            .add_field(
                8,
                Some(AnyValue::Primitive(PrimitiveValue::Time(
                    DateTime::from_timestamp(0, 1000)
                        .unwrap()
                        .naive_utc()
                        .time(),
                ))),
            )
            .unwrap();
        builder
            .add_field(
                9,
                Some(AnyValue::Primitive(PrimitiveValue::Timestamp(
                    DateTime::from_timestamp(0, 1000).unwrap().naive_utc(),
                ))),
            )
            .unwrap();
        builder
            .add_field(
                10,
                Some(AnyValue::Primitive(PrimitiveValue::Uuid(
                    uuid::Uuid::new_v4(),
                ))),
            )
            .unwrap();
        builder
            .add_field(
                11,
                Some(AnyValue::Primitive(PrimitiveValue::Timestampz(
                    DateTime::from_timestamp(0, 1000).unwrap(),
                ))),
            )
            .unwrap();
        builder
            .add_field(12, Some(AnyValue::Primitive(PrimitiveValue::Decimal(2222))))
            .unwrap();
        builder
            .add_field(
                13,
                Some(AnyValue::Primitive(PrimitiveValue::Binary(vec![1, 2, 3]))),
            )
            .unwrap();
        builder
            .add_field(
                14,
                Some(AnyValue::Primitive(PrimitiveValue::Fixed(vec![
                    1, 2, 3, 4, 5,
                ]))),
            )
            .unwrap();
        (struct_type, builder.build().unwrap())
    }

    fn test_avro(struct_type: Arc<Struct>, struct_value: StructValue) {
        // Prepare writer
        let schema = Schema::new(0, None, struct_type.as_ref().clone());
        let avro_schema = to_avro_schema(&schema, Some("partition")).unwrap();
        let mut avro_writer = apache_avro::Writer::new(&avro_schema, Vec::new());

        let ori_any_value = AnyValue::Struct(struct_value);

        // Write value
        let value: Value = ori_any_value.clone().into();
        let value = apache_avro::to_value(value).unwrap();
        let value = value.resolve(&avro_schema).unwrap();
        avro_writer.append(value).unwrap();
        let _length = avro_writer.flush().unwrap();
        let content = avro_writer.into_inner().unwrap();

        // Read value
        let reader = apache_avro::Reader::new(&content[..]).unwrap();
        let struct_type = types::Any::Struct(struct_type);
        for value in reader {
            let value = apache_avro::from_value::<Value>(&value.unwrap()).unwrap();
            let value = value.into_memory(&struct_type).unwrap();
            assert_eq!(value.unwrap(), ori_any_value);
        }
    }

    #[test]
    fn test_simple_value_avro() {
        let (struct_type, struct_value) = struct_for_avro();
        test_avro(struct_type, struct_value);
    }

    #[test]
    fn test_compose_value_avro() {
        let (struct_type, struct_value) = compose_struct();
        test_avro(struct_type, struct_value);
    }

    fn test_json(struct_type: Arc<Struct>, struct_value: StructValue) {
        let ori_any_value = AnyValue::Struct(struct_value);

        // Write value
        let value: Value = ori_any_value.clone().into();
        let value = serde_json::to_value(value).unwrap();
        // Read value
        let any_value = serde_json::from_value::<Value>(value)
            .unwrap()
            .into_memory(&types::Any::Struct(struct_type))
            .unwrap()
            .unwrap();
        assert_eq!(any_value, ori_any_value);
    }

    #[test]
    fn test_simple_value_json() {
        let (struct_type, struct_value) = struct_for_json();
        test_json(struct_type, struct_value);
    }

    #[test]
    fn test_compose_value_json() {
        let (struct_type, struct_value) = compose_struct();
        test_json(struct_type, struct_value);
    }
}
