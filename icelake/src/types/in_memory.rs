//! in_memory module provides the definition of iceberg in-memory data types.

use std::fmt::{Display, Formatter};
use std::hash::Hasher;
use std::sync::Arc;
use std::{collections::HashMap, str::FromStr};

use bitvec::vec::BitVec;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::NaiveTime;
use chrono::Utc;
use chrono::{DateTime, Datelike};
use derive_builder::Builder;
use opendal::Operator;
use ordered_float::OrderedFloat;
use parquet::format::FileMetaData;
use serde::ser::SerializeMap;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::hash::Hash;
use uuid::Uuid;

use crate::types::in_memory::_decimal::REQUIRED_LENGTH;
use crate::types::parse_manifest_list;
use crate::ErrorKind;
use crate::Result;
use crate::{Error, Table};

pub(crate) const UNASSIGNED_SEQ_NUM: i64 = -1;
pub(crate) const MAIN_BRANCH: &str = "main";
const EMPTY_SNAPSHOT_ID: i64 = -1;

pub(crate) const MAX_DECIMAL_BYTES: u32 = 24;
pub(crate) const MAX_DECIMAL_PRECISION: u32 = 38;

mod _decimal {
    use lazy_static::lazy_static;

    use super::{MAX_DECIMAL_BYTES, MAX_DECIMAL_PRECISION};

    lazy_static! {
        // Max precision of bytes, starts from 1
        pub(super) static ref MAX_PRECISION: [u32; MAX_DECIMAL_BYTES as usize] = {
            let mut ret: [u32; 24] = [0; 24];
            for (i, prec) in ret.iter_mut().enumerate() {
                *prec = 2f64.powi((8 * (i + 1) - 1) as i32).log10().floor() as u32;
            }

            ret
        };

        //  Required bytes of precision, starts from 1
        pub(super) static ref REQUIRED_LENGTH: [u32; MAX_DECIMAL_PRECISION as usize] = {
            let mut ret: [u32; MAX_DECIMAL_PRECISION as usize] = [0; MAX_DECIMAL_PRECISION as usize];

            for (i, required_len) in ret.iter_mut().enumerate() {
                for j in 0..MAX_PRECISION.len() {
                    if MAX_PRECISION[j] >= ((i+1) as u32) {
                        *required_len = (j+1) as u32;
                        break;
                    }
                }
            }

            ret
        };

    }
}

/// All data types are either primitives or nested types, which are maps, lists, or structs.
#[derive(Debug, PartialEq, Clone, Eq)]
pub enum Any {
    /// A Primitive type
    Primitive(Primitive),
    /// A Struct type
    Struct(Arc<Struct>),
    /// A List type.
    List(List),
    /// A Map type
    Map(Map),
}

/// All data values are either primitives or nested values, which are maps, lists, or structs.
///
/// # TODO:
/// Allow derived hash because Map will not be used in hash, so it's ok to derive hash.
/// But this may be misuse in the future.
#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Debug, Clone, Eq, Hash)]
pub enum AnyValue {
    /// A Primitive type
    Primitive(PrimitiveValue),
    /// A Struct type is a tuple of typed values.
    ///
    /// struct value carries the value of a struct type, could be used as
    /// default value.
    ///
    /// struct value stores as a map from field id to field value.
    Struct(StructValue),
    /// A list type with a list of typed values.
    List(Vec<Option<AnyValue>>),
    /// A map is a collection of key-value pairs with a key type and a value type.
    ///
    /// map value carries the value of a map type, could be used as
    /// default value.
    Map {
        /// All keys in this map.
        keys: Vec<AnyValue>,
        /// All values in this map.
        values: Vec<Option<AnyValue>>,
    },
}

impl PartialEq for AnyValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Primitive(l0), Self::Primitive(r0)) => l0 == r0,
            (Self::Struct(l0), Self::Struct(r0)) => l0 == r0,
            (Self::List(l0), Self::List(r0)) => l0 == r0,
            (
                Self::Map {
                    keys: l_keys,
                    values: l_values,
                },
                Self::Map {
                    keys: r_keys,
                    values: r_values,
                },
            ) => {
                // # TODO
                // A inefficient way to compare map.
                let mut map = HashMap::with_capacity(l_keys.len());
                l_keys.iter().zip(l_values.iter()).for_each(|(key, value)| {
                    map.insert(key, value);
                });
                r_keys
                    .iter()
                    .zip(r_values.iter())
                    .all(|(key, value)| map.get(key).map_or(false, |v| *v == value))
            }
            _ => false,
        }
    }
}

impl Serialize for AnyValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            AnyValue::Primitive(value) => value.serialize(serializer),
            AnyValue::Struct(value) => value.serialize(serializer),
            AnyValue::List(value) => value.serialize(serializer),
            AnyValue::Map { keys, values } => {
                let mut map = serializer.serialize_map(Some(keys.len()))?;
                for (key, value) in keys.iter().zip(values.iter()) {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

/// Primitive Types within a schema.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Primitive {
    /// True or False
    Boolean,
    /// 32-bit signed integer, Can promote to long
    Int,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 753 floating bit, Can promote to double
    Float,
    /// 64-bit IEEE 753 floating bit.
    Double,
    /// Fixed point decimal
    ///
    /// - Precision can only be widened.
    /// - Scale is fixed and cannot be changed by schema evolution.
    Decimal {
        /// The number of digits in the number, precision must be 38 or less
        precision: u8,
        /// The number of digits to the right of the decimal point.
        scale: u8,
    },
    /// Calendar date without timezone or time
    Date,
    /// Time of day without date or timezone.
    ///
    /// Time values are stored with microsecond precision.
    Time,
    /// Timestamp without timezone
    ///
    /// Timestamp values are stored with microsecond precision.
    ///
    /// Timestamps without time zone represent a date and time of day regardless of zone:
    /// the time value is independent of zone adjustments (`2017-11-16 17:10:34` is always retrieved as `2017-11-16 17:10:34`).
    /// Timestamp values are stored as a long that encodes microseconds from the unix epoch.
    Timestamp,
    /// Timestamp with timezone
    ///
    /// Timestampz values are stored with microsecond precision.
    ///
    /// Timestamps with time zone represent a point in time:
    /// values are stored as UTC and do not retain a source time zone
    /// (`2017-11-16 17:10:34 PST` is stored/retrieved as `2017-11-17 01:10:34 UTC` and these values are considered identical).
    Timestampz,
    /// Arbitrary-length character sequences, Encoded with UTF-8
    ///
    /// Character strings must be stored as UTF-8 encoded byte arrays.
    String,
    /// Universally Unique Identifiers, Should use 16-byte fixed
    Uuid,
    /// Fixed-length byte array of length.
    Fixed(u64),
    /// Arbitrary-length byte array.
    Binary,
}

impl From<Primitive> for Any {
    fn from(value: Primitive) -> Self {
        Any::Primitive(value)
    }
}

impl Primitive {
    /// Returns minimum bytes required for decimal with [`precision`].
    #[inline(always)]
    pub fn decimal_required_bytes(precision: u32) -> Result<u32> {
        if precision == 0 || precision > MAX_DECIMAL_PRECISION {
            return Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!(
                    "Decimal precision must be between 1 and {MAX_DECIMAL_PRECISION}: {precision}",
                ),
            ));
        }
        Ok(REQUIRED_LENGTH[precision as usize - 1])
    }
}

/// Primitive Values within a schema.
///
/// Used to represent the value of a primitive type, like as default value.
#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub enum PrimitiveValue {
    /// True or False
    Boolean(bool),
    /// 32-bit signed integer, Can promote to long
    Int(i32),
    /// 64-bit signed integer
    Long(i64),
    /// 32-bit IEEE 753 floating bit, Can promote to double
    Float(OrderedFloat<f32>),
    /// 64-bit IEEE 753 floating bit.
    Double(OrderedFloat<f64>),
    /// Fixed point decimal
    Decimal(i128),
    /// Calendar date without timezone or time
    Date(NaiveDate),
    /// Time of day without date or timezone.
    ///
    /// Time values are stored with microsecond precision.
    Time(NaiveTime),
    /// Timestamp without timezone
    ///
    /// Timestamp values are stored with microsecond precision.
    ///
    /// Timestamps without time zone represent a date and time of day regardless of zone:
    /// the time value is independent of zone adjustments (`2017-11-16 17:10:34` is always retrieved as `2017-11-16 17:10:34`).
    /// Timestamp values are stored as a long that encodes microseconds from the unix epoch.
    Timestamp(NaiveDateTime),
    /// Timestamp with timezone
    ///
    /// Timestampz values are stored with microsecond precision.
    ///
    /// Timestamps with time zone represent a point in time:
    /// values are stored as UTC and do not retain a source time zone
    /// (`2017-11-16 17:10:34 PST` is stored/retrieved as `2017-11-17 01:10:34 UTC` and these values are considered identical).
    Timestampz(DateTime<Utc>),
    /// Arbitrary-length character sequences, Encoded with UTF-8
    ///
    /// Character strings must be stored as UTF-8 encoded byte arrays.
    String(String),
    /// Universally Unique Identifiers, Should use 16-byte fixed
    Uuid(Uuid),
    /// Fixed-length byte array of length.
    Fixed(Vec<u8>),
    /// Arbitrary-length byte array.
    Binary(Vec<u8>),
}

impl From<PrimitiveValue> for AnyValue {
    fn from(value: PrimitiveValue) -> Self {
        AnyValue::Primitive(value)
    }
}

impl Serialize for PrimitiveValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            PrimitiveValue::Boolean(value) => serializer.serialize_bool(*value),
            PrimitiveValue::Int(value) => serializer.serialize_i32(*value),
            PrimitiveValue::Long(value) => serializer.serialize_i64(*value),
            PrimitiveValue::Float(value) => serializer.serialize_f32(value.0),
            PrimitiveValue::Double(value) => serializer.serialize_f64(value.0),
            PrimitiveValue::Decimal(value) => serializer.serialize_bytes(&value.to_be_bytes()),
            PrimitiveValue::Date(value) => serializer.serialize_i32(value.num_days_from_ce()),
            PrimitiveValue::Time(value) => serializer.serialize_i64(
                NaiveDateTime::new(NaiveDate::default(), *value)
                    .and_utc()
                    .timestamp_micros(),
            ),
            PrimitiveValue::Timestamp(value) => {
                serializer.serialize_i64(value.and_utc().timestamp_micros())
            }
            PrimitiveValue::Timestampz(value) => serializer.serialize_i64(value.timestamp_micros()),
            PrimitiveValue::String(value) => serializer.serialize_str(value),
            PrimitiveValue::Uuid(value) => serializer.serialize_str(&value.to_string()),
            PrimitiveValue::Fixed(value) => serializer.serialize_bytes(value),
            PrimitiveValue::Binary(value) => serializer.serialize_bytes(value),
        }
    }
}

/// A struct is a tuple of typed values.
///
/// - Each field in the tuple is named and has an integer id that is unique in the table schema.
/// - Each field can be either optional or required, meaning that values can (or cannot) be null.
/// - Fields may be any type.
/// - Fields may have an optional comment or doc string.
/// - Fields can have default values.
#[derive(Default, Debug, Clone, Eq)]
pub struct Struct {
    /// Fields contained in this struct.
    fields: Vec<FieldRef>,
    /// Map field id to index
    id_lookup: HashMap<i32, FieldRef>,
}

impl PartialEq for Struct {
    fn eq(&self, other: &Self) -> bool {
        for (id, field) in self.id_lookup.iter() {
            if let Some(other_field) = other.id_lookup.get(id) {
                if field != other_field {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }
}

impl Struct {
    /// Create a new struct.
    pub fn new(fields: Vec<FieldRef>) -> Self {
        let mut id_lookup = HashMap::with_capacity(fields.len());
        fields.iter().for_each(|field| {
            id_lookup.insert(field.id, field.clone());
            Self::fetch_any_field_id_map(&field.field_type, &mut id_lookup)
        });
        Struct { fields, id_lookup }
    }

    fn fetch_any_field_id_map(ty: &Any, map: &mut HashMap<i32, FieldRef>) {
        match ty {
            Any::Primitive(_) => {
                // just skip
            }
            Any::Struct(inner) => Self::fetch_struct_field_id_map(inner, map),
            Any::List(_) => {
                // #TODO
                // field id map is only used in partition now. For partition, it don't need list,
                // so we ignore it now.
            }
            Any::Map(_) => {
                // #TODO
                // field id map is only used in partition now. For partition, it don't need map,
                // so we ignore it now.
            }
        }
    }

    fn fetch_struct_field_id_map(ty: &Struct, map: &mut HashMap<i32, FieldRef>) {
        map.extend(ty.id_lookup.clone())
    }

    /// Return the number of fields in the struct.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Check if the struct is empty.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Return the reference to the field of this struct.
    pub fn fields(&self) -> &[FieldRef] {
        &self.fields
    }

    /// Lookup the field type according to the field id.
    pub fn lookup_type(&self, field_id: i32) -> Option<Any> {
        self.id_lookup
            .get(&field_id)
            .map(|field| field.field_type.clone())
    }

    /// Lookup the field according to the field id.
    pub fn lookup_field(&self, field_id: i32) -> Option<&FieldRef> {
        self.id_lookup.get(&field_id)
    }

    /// Lookup field by field name.
    pub fn lookup_field_by_name(&self, field_name: &str) -> Option<&FieldRef> {
        self.fields.iter().find(|field| field.name == field_name)
    }
}

/// The reference to a Field.
pub type FieldRef = Arc<Field>;

/// A Field is the field of a struct.
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct Field {
    /// An integer id that is unique in the table schema
    pub id: i32,
    /// Field Name
    pub name: String,
    /// Optional or required, meaning that values can (or can not be null)
    pub required: bool,
    /// Field can have any type
    pub field_type: Any,
    /// Fields can have any optional comment or doc string.
    pub comment: Option<String>,
    /// `initial-default` is used to populate the field’s value for all records that were written before the field was added to the schema
    pub initial_default: Option<AnyValue>,
    /// `write-default` is used to populate the field’s value for any
    /// records written after the field was added to the schema, if the
    /// writer does not supply the field’s value
    pub write_default: Option<AnyValue>,
}

impl Field {
    /// Create a required field.
    pub fn required(id: i32, name: impl Into<String>, r#type: Any) -> Self {
        Self {
            id,
            name: name.into(),
            required: true,
            field_type: r#type,
            comment: None,
            initial_default: None,
            write_default: None,
        }
    }

    /// Create an optional field.
    pub fn optional(id: i32, name: impl Into<String>, r#type: Any) -> Self {
        Self {
            id,
            name: name.into(),
            required: false,
            field_type: r#type,
            comment: None,
            initial_default: None,
            write_default: None,
        }
    }

    fn with_comment(mut self, doc: impl Into<String>) -> Self {
        self.comment = Some(doc.into());
        self
    }

    fn with_required(mut self) -> Self {
        self.required = true;
        self
    }
}

/// A Struct type is a tuple of typed values.
#[derive(Default, Debug, PartialEq, Clone, Eq)]
pub struct StructValue {
    field_values: Vec<AnyValue>,
    type_info: Arc<Struct>,
    null_bitmap: BitVec,
}

impl From<StructValue> for AnyValue {
    fn from(value: StructValue) -> Self {
        AnyValue::Struct(value)
    }
}

impl StructValue {
    /// Create a iterator to read the field in order of (field_id, field_value, field_name,
    /// field_required).
    pub fn iter(&self) -> impl Iterator<Item = (i32, Option<&AnyValue>, &str, bool)> {
        self.null_bitmap
            .iter()
            .zip(self.field_values.iter())
            .zip(self.type_info.fields().iter())
            .map(|((null, value), field)| {
                (
                    field.id,
                    if *null { None } else { Some(value) },
                    field.name.as_str(),
                    field.required,
                )
            })
    }
}

impl Serialize for StructValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut record = serializer.serialize_struct("", self.field_values.len())?;
        for (_, value, key, required) in self.iter() {
            if required {
                record.serialize_field(Box::leak(key.to_string().into_boxed_str()), &value.expect("Struct Builder should guaranteed that the value is always if the field is required."))?;
            } else {
                record.serialize_field(Box::leak(key.to_string().into_boxed_str()), &value)?;
            }
        }
        record.end()
    }
}

impl Hash for StructValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for (id, value, name, required) in self.iter() {
            id.hash(state);
            value.hash(state);
            name.hash(state);
            required.hash(state);
        }
    }
}

/// A builder to build a struct value. Buidler will guaranteed that the StructValue is valid for the Struct.
pub struct StructValueBuilder {
    fields: HashMap<i32, Option<AnyValue>>,
    type_info: Arc<Struct>,
}

impl StructValueBuilder {
    /// Create a new builder.
    pub fn new(type_info: Arc<Struct>) -> Self {
        Self {
            fields: HashMap::with_capacity(type_info.len()),
            type_info,
        }
    }

    /// Add a field to the struct value.
    pub fn add_field(&mut self, field_id: i32, field_value: Option<AnyValue>) -> Result<()> {
        // Check the value is valid if the field is required.
        if self
            .type_info
            .lookup_field(field_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("Field {} is not found", field_id),
                )
            })?
            .required
            && field_value.is_none()
        {
            return Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("Field {} is required", field_id),
            ));
        }
        // Check the field id is valid.
        self.type_info.lookup_type(field_id).ok_or_else(|| {
            Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("Field {} is not found", field_id),
            )
        })?;
        // TODO: Check the field type is consistent.
        // TODO: Check the duplication of field.

        self.fields.insert(field_id, field_value);
        Ok(())
    }

    /// Build the struct value.
    pub fn build(mut self) -> Result<StructValue> {
        let mut field_values = Vec::with_capacity(self.fields.len());
        let mut null_bitmap = BitVec::with_capacity(self.fields.len());

        for field in self.type_info.fields.iter() {
            let field_value = self.fields.remove(&field.id).ok_or_else(|| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("Field {} is required", field.name),
                )
            })?;
            if let Some(value) = field_value {
                null_bitmap.push(false);
                field_values.push(value);
            } else {
                null_bitmap.push(true);
                // `1` is just as a placeholder. It will be ignored.
                field_values.push(AnyValue::Primitive(PrimitiveValue::Int(1)));
            }
        }

        Ok(StructValue {
            field_values,
            type_info: self.type_info,
            null_bitmap,
        })
    }
}

/// A list is a collection of values with some element type.
///
/// - The element field has an integer id that is unique in the table schema.
/// - Elements can be either optional or required.
/// - Element types may be any type.
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct List {
    /// an integer id that is unique in the table schema.
    pub element_id: i32,
    /// Optional or required, meaning that values can (or can not be null)
    pub element_required: bool,
    /// Element types may be any type.
    pub element_type: Box<Any>,
}

/// A map is a collection of key-value pairs with a key type and a value type.
///
/// - Both the key field and value field each have an integer id that is unique in the table schema.
/// - Map keys are required and map values can be either optional or required.
/// - Both map keys and map values may be any type, including nested types.
#[derive(Debug, PartialEq, Clone, Eq)]
pub struct Map {
    /// an integer id that is unique in the table schema
    pub key_id: i32,
    /// Both map keys and map values may be any type, including nested types.
    pub key_type: Box<Any>,

    /// an integer id that is unique in the table schema
    pub value_id: i32,
    /// map values can be either optional or required.
    pub value_required: bool,
    /// Both map keys and map values may be any type, including nested types.
    pub value_type: Box<Any>,
}

/// A table’s schema is a list of named columns.
///
/// All data types are either primitives or nested types, which are maps, lists, or structs.
/// A table schema is also a struct type.
#[derive(Debug, PartialEq, Clone)]
pub struct Schema {
    /// The unique id for this schema.
    pub schema_id: i32,
    /// A schema can optionally track the set of primitive fields that
    /// identify rows in a table, using the property identifier-field-ids
    pub identifier_field_ids: Option<Vec<i32>>,
    /// fields contained in this schema.
    r#struct: Struct,
}

impl Schema {
    /// Create a schema
    pub fn new(schema_id: i32, identifier_field_ids: Option<Vec<i32>>, r#struct: Struct) -> Self {
        Schema {
            schema_id,
            identifier_field_ids,
            r#struct,
        }
    }

    /// Return the fields of the schema
    pub fn fields(&self) -> &[FieldRef] {
        self.r#struct.fields()
    }

    /// Look up field by field id
    pub fn look_up_field_by_id(&self, field_id: i32) -> Option<&FieldRef> {
        self.r#struct.lookup_field(field_id)
    }
}

/// Transform is used to transform predicates to partition predicates,
/// in addition to transforming data values.
///
/// Deriving partition predicates from column predicates on the table data
/// is used to separate the logical queries from physical storage: the
/// partitioning can change and the correct partition filters are always
/// derived from column predicates.
///
/// This simplifies queries because users don’t have to supply both logical
/// predicates and partition predicates.
///
/// All transforms must return `null` for a `null` input value.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Transform {
    /// Source value, unmodified
    ///
    /// - Source type could be `Any`.
    /// - Return type is the same with source type.
    Identity,
    /// Hash of value, mod `N`.
    ///
    /// Bucket partition transforms use a 32-bit hash of the source value.
    /// The 32-bit hash implementation is the 32-bit Murmur3 hash, x86
    /// variant, seeded with 0.
    ///
    /// Transforms are parameterized by a number of buckets, N. The hash mod
    /// N must produce a positive value by first discarding the sign bit of
    /// the hash value. In pseudo-code, the function is:
    ///
    /// ```text
    /// def bucket_N(x) = (murmur3_x86_32_hash(x) & Integer.MAX_VALUE) % N
    /// ```
    ///
    /// - Source type could be `int`, `long`, `decimal`, `date`, `time`,
    ///   `timestamp`, `timestamptz`, `string`, `uuid`, `fixed`, `binary`.
    /// - Return type is `int`.
    Bucket(i32),
    /// Value truncated to width `W`
    ///
    /// For `int`:
    ///
    /// - `v - (v % W)` remainders must be positive
    /// - example: W=10: 1 ￫ 0, -1 ￫ -10
    /// - note: The remainder, v % W, must be positive.
    ///
    /// For `long`:
    ///
    /// - `v - (v % W)` remainders must be positive
    /// - example: W=10: 1 ￫ 0, -1 ￫ -10
    /// - note: The remainder, v % W, must be positive.
    ///
    /// For `decimal`:
    ///
    /// - `scaled_W = decimal(W, scale(v)) v - (v % scaled_W)`
    /// - example: W=50, s=2: 10.65 ￫ 10.50
    ///
    /// For `string`:
    ///
    /// - Substring of length L: `v.substring(0, L)`
    /// - example: L=3: iceberg ￫ ice
    /// - note: Strings are truncated to a valid UTF-8 string with no more
    ///   than L code points.
    ///
    /// - Source type could be `int`, `long`, `decimal`, `string`
    /// - Return type is the same with source type.
    Truncate(i32),
    /// Extract a date or timestamp year, as years from 1970
    ///
    /// - Source type could be `date`, `timestamp`, `timestamptz`
    /// - Return type is `int`
    Year,
    /// Extract a date or timestamp month, as months from 1970-01-01
    ///
    /// - Source type could be `date`, `timestamp`, `timestamptz`
    /// - Return type is `int`
    Month,
    /// Extract a date or timestamp day, as days from 1970-01-01
    ///
    /// - Source type could be `date`, `timestamp`, `timestamptz`
    /// - Return type is `int`
    Day,
    /// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
    ///
    /// - Source type could be `timestamp`, `timestamptz`
    /// - Return type is `int`
    Hour,
    /// Always produces `null`
    ///
    /// The void transform may be used to replace the transform in an
    /// existing partition field so that the field is effectively dropped in
    /// v1 tables.
    ///
    /// - Source type could be `Any`.
    /// - Return type is Source type or `int`
    Void,
}

impl Transform {
    pub fn result_type(&self, input_type: &Any) -> Result<Any> {
        let check_time = |input_type: &Any| {
            if !matches!(
                input_type,
                Any::Primitive(Primitive::Date)
                    | Any::Primitive(Primitive::Timestamp)
                    | Any::Primitive(Primitive::Timestampz)
            ) {
                return Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("transform year type {input_type:?} is invalid"),
                ));
            }
            Ok(())
        };
        let check_bucket = |input_type: &Any| {
            if !matches!(
                input_type,
                Any::Primitive(Primitive::Int)
                    | Any::Primitive(Primitive::Long)
                    | Any::Primitive(Primitive::Decimal { .. })
                    | Any::Primitive(Primitive::Date)
                    | Any::Primitive(Primitive::Time)
                    | Any::Primitive(Primitive::Timestamp)
                    | Any::Primitive(Primitive::Timestampz)
                    | Any::Primitive(Primitive::String)
                    | Any::Primitive(Primitive::Uuid)
                    | Any::Primitive(Primitive::Fixed(_))
                    | Any::Primitive(Primitive::Binary)
            ) {
                return Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("transform bucket type {input_type:?} is invalid"),
                ));
            }
            Ok(())
        };
        let check_truncate = |input_type: &Any| {
            if !matches!(
                input_type,
                Any::Primitive(Primitive::Int)
                    | Any::Primitive(Primitive::Long)
                    | Any::Primitive(Primitive::Decimal { .. })
                    | Any::Primitive(Primitive::String)
            ) {
                return Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("transform truncate type {input_type:?} is invalid"),
                ));
            }
            Ok(())
        };
        let check_hour = |input_type: &Any| {
            if !matches!(
                input_type,
                Any::Primitive(Primitive::Timestamp) | Any::Primitive(Primitive::Timestampz)
            ) {
                return Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("transform hour type {input_type:?} is invalid"),
                ));
            }
            Ok(())
        };
        match self {
            Transform::Identity => Ok(input_type.clone()),
            Transform::Void => Ok(Primitive::Int.into()),
            Transform::Year => {
                check_time(input_type)?;
                Ok(Primitive::Int.into())
            }
            Transform::Month => {
                check_time(input_type)?;
                Ok(Primitive::Int.into())
            }
            Transform::Day => {
                check_time(input_type)?;
                Ok(Primitive::Int.into())
            }
            Transform::Hour => {
                check_hour(input_type)?;
                Ok(Primitive::Int.into())
            }
            Transform::Bucket(_) => {
                check_bucket(input_type)?;
                Ok(Primitive::Int.into())
            }
            Transform::Truncate(_) => {
                check_truncate(input_type)?;
                Ok(input_type.clone())
            }
        }
    }
}

impl<'a> Display for &'a Transform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Transform::Identity => write!(f, "identity"),
            Transform::Year => write!(f, "year"),
            Transform::Month => write!(f, "month"),
            Transform::Day => write!(f, "day"),
            Transform::Hour => write!(f, "hour"),
            Transform::Void => write!(f, "void"),
            Transform::Bucket(length) => write!(f, "bucket[{}]", length),
            Transform::Truncate(width) => write!(f, "truncate[{}]", width),
        }
    }
}

impl FromStr for Transform {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let t = match s {
            "identity" => Transform::Identity,
            "year" => Transform::Year,
            "month" => Transform::Month,
            "day" => Transform::Day,
            "hour" => Transform::Hour,
            "void" => Transform::Void,
            v if v.starts_with("bucket") => {
                let length = v
                    .strip_prefix("bucket")
                    .expect("transform must starts with `bucket`")
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse()
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::IcebergDataInvalid,
                            format!("transform bucket type {v:?} is invalid"),
                        )
                        .set_source(err)
                    })?;

                Transform::Bucket(length)
            }
            v if v.starts_with("truncate") => {
                let width = v
                    .strip_prefix("truncate")
                    .expect("transform must starts with `truncate`")
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse()
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::IcebergDataInvalid,
                            format!("transform truncate type {v:?} is invalid"),
                        )
                        .set_source(err)
                    })?;

                Transform::Truncate(width)
            }
            v => {
                return Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("transform {v:?} is invalid"),
                ))
            }
        };

        Ok(t)
    }
}

/// Data files are stored in manifests with a tuple of partition values
/// that are used in scans to filter out files that cannot contain records
///  that match the scan’s filter predicate.
///
/// Partition values for a data file must be the same for all records stored
/// in the data file. (Manifests store data files from any partition, as long
/// as the partition spec is the same for the data files.)
///
/// Tables are configured with a partition spec that defines how to produce a tuple of partition values from a record.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PartitionSpec {
    /// The spec id.
    pub spec_id: i32,
    /// Partition fields.
    pub fields: Vec<PartitionField>,
}

impl PartitionSpec {
    pub(crate) fn partition_type(&self, schema: &Schema) -> Result<Struct> {
        let mut fields = Vec::with_capacity(self.fields.len());
        for partition_field in &self.fields {
            let source_field = schema
                .look_up_field_by_id(partition_field.source_column_id)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!(
                            "Can't find field id {} in schema",
                            partition_field.source_column_id
                        ),
                    )
                })?;
            let result_type = partition_field
                .transform
                .result_type(&source_field.field_type)?;
            fields.push(
                Field::optional(
                    partition_field.partition_field_id,
                    partition_field.name.as_str(),
                    result_type,
                )
                .into(),
            );
        }

        Ok(Struct::new(fields))
    }

    pub fn column_ids(&self) -> Vec<i32> {
        self.fields
            .iter()
            .map(|field| field.source_column_id)
            .collect()
    }

    /// Check if this partition spec is unpartitioned.
    pub fn is_unpartitioned(&self) -> bool {
        self.fields.is_empty()
    }
}

/// Field of the specified partition spec.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PartitionField {
    /// A source column id from the table’s schema
    pub source_column_id: i32,
    /// A partition field id that is used to identify a partition field
    /// and is unique within a partition spec.
    ///
    /// In v2 table metadata, it is unique across all partition specs.
    pub partition_field_id: i32,
    /// A transform that is applied to the source column to produce
    /// a partition value
    ///
    /// The source column, selected by id, must be a primitive type
    /// and cannot be contained in a map or list, but may be nested in
    /// a struct.
    pub transform: Transform,
    /// A partition name
    pub name: String,
}

/// Users can sort their data within partitions by columns to gain
/// performance. The information on how the data is sorted can be declared
/// per data or delete file, by a sort order.
///
/// - Order id `0` is reserved for the unsorted order.
/// - Sorting floating-point numbers should produce the following behavior:
///   `-NaN` < `-Infinity` < `-value` < `-0` < `0` < `value` < `Infinity`
///   < `NaN`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SortOrder {
    /// The sort order id of this SortOrder
    pub order_id: i32,
    /// The order of the sort fields within the list defines the order in
    /// which the sort is applied to the data
    pub fields: Vec<SortField>,
}

/// Field of the specified sort order.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SortField {
    /// A source column id from the table’s schema
    pub source_column_id: i32,
    /// A transform that is applied to the source column to produce
    /// a partition value
    ///
    /// The source column, selected by id, must be a primitive type
    /// and cannot be contained in a map or list, but may be nested in
    /// a struct.
    pub transform: Transform,
    /// sort direction, that can only be either `asc` or `desc`
    pub direction: SortDirection,
    /// A null order that describes the order of null values when sorted.
    /// Can only be either nulls-first or nulls-last
    pub null_order: NullOrder,
}

/// sort direction, that can only be either `asc` or `desc`
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SortDirection {
    /// Ascending order
    ASC,
    /// Descending order
    DESC,
}

impl Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortDirection::ASC => write!(f, "asc"),
            SortDirection::DESC => write!(f, "desc"),
        }
    }
}

impl FromStr for SortDirection {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "asc" => Ok(SortDirection::ASC),
            "desc" => Ok(SortDirection::DESC),
            v => Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("sort direction {:?} is invalid", v),
            )),
        }
    }
}

/// A null order that describes the order of null values when sorted.
/// Can only be either nulls-first or nulls-last
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum NullOrder {
    /// Nulls are sorted before non-null values
    First,
    /// Nulls are sorted after non-null values
    Last,
}

impl Display for NullOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NullOrder::First => write!(f, "nulls-first"),
            NullOrder::Last => write!(f, "nulls-last"),
        }
    }
}

impl FromStr for NullOrder {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "nulls-first" => Ok(NullOrder::First),
            "nulls-last" => Ok(NullOrder::Last),
            v => Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("null order {:?} is invalid", v),
            )),
        }
    }
}

/// Snapshots are embedded in table metadata, but the list of manifests for a
/// snapshot are stored in a separate manifest list file.
///
/// A new manifest list is written for each attempt to commit a snapshot
/// because the list of manifests always changes to produce a new snapshot.
/// When a manifest list is written, the (optimistic) sequence number of the
/// snapshot is written for all new manifest files tracked by the list.
///
/// A manifest list includes summary metadata that can be used to avoid
/// scanning all of the manifests in a snapshot when planning a table scan.
/// This includes the number of added, existing, and deleted files, and a
/// summary of values for each field of the partition spec used to write the
/// manifest.
#[derive(Debug, PartialEq, Clone)]
pub struct ManifestList {
    /// Entries in a manifest list.
    pub entries: Vec<ManifestListEntry>,
}

impl ManifestList {
    pub(crate) fn v2_schema() -> Schema {
        Schema::new(
            1,
            None,
            Struct::new(vec![
                manifest_list::MANIFEST_PATH.clone().into(),
                manifest_list::MANIFEST_LENGTH.clone().into(),
                manifest_list::PARTITION_SPEC_ID.clone().into(),
                manifest_list::CONTENT.clone().into(),
                manifest_list::SEQUENCE_NUMBER.clone().into(),
                manifest_list::MIN_SEQUENCE_NUMBER.clone().into(),
                manifest_list::ADDED_SNAPSHOT_ID.clone().into(),
                manifest_list::ADDED_FILES_COUNT.clone().into(),
                manifest_list::EXISTING_FILES_COUNT.clone().into(),
                manifest_list::DELETED_FILES_COUNT.clone().into(),
                manifest_list::ADDED_ROWS_COUNT.clone().into(),
                manifest_list::EXISTING_ROWS_COUNT.clone().into(),
                manifest_list::DELETED_ROWS_COUNT.clone().into(),
                manifest_list::PARTITIONS.clone().into(),
                manifest_list::KEY_METADATA.clone().into(),
            ]),
        )
    }
}

/// Entry in a manifest list.
#[derive(Debug, PartialEq, Clone)]
pub struct ManifestListEntry {
    /// field: 500
    ///
    /// Location of the manifest file
    pub manifest_path: String,
    /// field: 501
    ///
    /// Length of the manifest file in bytes
    pub manifest_length: i64,
    /// field: 502
    ///
    /// ID of a partition spec used to write the manifest; must be listed
    /// in table metadata partition-specs
    pub partition_spec_id: i32,
    /// field: 517
    ///
    /// The type of files tracked by the manifest, either data or delete
    /// files; 0 for all v1 manifests
    pub content: ManifestContentType,
    /// field: 515
    ///
    /// The sequence number when the manifest was added to the table; use 0
    /// when reading v1 manifest lists
    pub sequence_number: i64,
    /// field: 516
    ///
    /// The minimum data sequence number of all live data or delete files in
    /// the manifest; use 0 when reading v1 manifest lists
    pub min_sequence_number: i64,
    /// field: 503
    ///
    /// ID of the snapshot where the manifest file was added
    pub added_snapshot_id: i64,
    /// field: 504
    ///
    /// Number of entries in the manifest that have status ADDED, when null
    /// this is assumed to be non-zero
    pub added_files_count: i32,
    /// field: 505
    ///
    /// Number of entries in the manifest that have status EXISTING (0),
    /// when null this is assumed to be non-zero
    pub existing_files_count: i32,
    /// field: 506
    ///
    /// Number of entries in the manifest that have status DELETED (2),
    /// when null this is assumed to be non-zero
    pub deleted_files_count: i32,
    /// field: 512
    ///
    /// Number of rows in all of files in the manifest that have status
    /// ADDED, when null this is assumed to be non-zero
    pub added_rows_count: i64,
    /// field: 513
    ///
    /// Number of rows in all of files in the manifest that have status
    /// EXISTING, when null this is assumed to be non-zero
    pub existing_rows_count: i64,
    /// field: 514
    ///
    /// Number of rows in all of files in the manifest that have status
    /// DELETED, when null this is assumed to be non-zero
    pub deleted_rows_count: i64,
    /// field: 507
    /// element_field: 508
    ///
    /// A list of field summaries for each partition field in the spec. Each
    /// field in the list corresponds to a field in the manifest file’s
    /// partition spec.
    pub partitions: Option<Vec<FieldSummary>>,
    /// field: 519
    ///
    /// Implementation-specific key metadata for encryption
    pub key_metadata: Option<Vec<u8>>,
}

mod manifest_list {
    use super::*;
    use once_cell::sync::Lazy;
    pub static MANIFEST_PATH: Lazy<Field> =
        Lazy::new(|| Field::required(500, "manifest_path", Any::Primitive(Primitive::String)));
    pub static MANIFEST_LENGTH: Lazy<Field> =
        Lazy::new(|| Field::required(501, "manifest_length", Any::Primitive(Primitive::Long)));
    pub static PARTITION_SPEC_ID: Lazy<Field> =
        Lazy::new(|| Field::required(502, "partition_spec_id", Any::Primitive(Primitive::Int)));
    pub static CONTENT: Lazy<Field> =
        Lazy::new(|| Field::required(517, "content", Any::Primitive(Primitive::Int)));
    pub static SEQUENCE_NUMBER: Lazy<Field> =
        Lazy::new(|| Field::required(515, "sequence_number", Any::Primitive(Primitive::Long)));
    pub static MIN_SEQUENCE_NUMBER: Lazy<Field> =
        Lazy::new(|| Field::required(516, "min_sequence_number", Any::Primitive(Primitive::Long)));
    pub static ADDED_SNAPSHOT_ID: Lazy<Field> =
        Lazy::new(|| Field::required(503, "added_snapshot_id", Any::Primitive(Primitive::Long)));
    pub static ADDED_FILES_COUNT: Lazy<Field> = Lazy::new(|| {
        Field::required(
            504,
            "added_files_count",
            Any::Primitive(Primitive::Int),
        )
    });
    pub static EXISTING_FILES_COUNT: Lazy<Field> = Lazy::new(|| {
        Field::required(
            505,
            "existing_files_count",
            Any::Primitive(Primitive::Int),
        )
    });
    pub static DELETED_FILES_COUNT: Lazy<Field> = Lazy::new(|| {
        Field::required(
            506,
            "deleted_files_count",
            Any::Primitive(Primitive::Int),
        )
    });
    pub static ADDED_ROWS_COUNT: Lazy<Field> =
        Lazy::new(|| Field::required(512, "added_rows_count", Any::Primitive(Primitive::Long)));
    pub static EXISTING_ROWS_COUNT: Lazy<Field> =
        Lazy::new(|| Field::required(513, "existing_rows_count", Any::Primitive(Primitive::Long)));
    pub static DELETED_ROWS_COUNT: Lazy<Field> =
        Lazy::new(|| Field::required(514, "deleted_rows_count", Any::Primitive(Primitive::Long)));
    pub static PARTITIONS: Lazy<Field> = Lazy::new(|| {
        Field::optional(
            507,
            "partitions",
            Any::List(List {
                element_id: 508,
                element_required: true,
                element_type: Box::new(Any::Struct(
                    Struct::new(vec![
                        Field::required(509, "contains_null", Any::Primitive(Primitive::Boolean))
                            .into(),
                        Field::optional(518, "contains_nan", Any::Primitive(Primitive::Boolean))
                            .into(),
                        Field::optional(510, "lower_bound", Any::Primitive(Primitive::Binary))
                            .into(),
                        Field::optional(511, "upper_bound", Any::Primitive(Primitive::Binary))
                            .into(),
                    ])
                    .into(),
                )),
            }),
        )
    });
    pub static KEY_METADATA: Lazy<Field> =
        Lazy::new(|| Field::optional(519, "key_metadata", Any::Primitive(Primitive::Binary)));
}

/// Field summary for partition field in the spec.
///
/// Each field in the list corresponds to a field in the manifest file’s partition spec.
///
/// TODO: add lower_bound and upper_bound support
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FieldSummary {
    /// field: 509
    ///
    /// Whether the manifest contains at least one partition with a null
    /// value for the field
    pub contains_null: bool,
    /// field: 518
    /// Whether the manifest contains at least one partition with a NaN
    /// value for the field
    pub contains_nan: Option<bool>,
    /// field: 510
    /// The minimum value for the field in the manifests
    /// partitions.
    pub lower_bound: Option<Vec<u8>>,
    /// field: 511
    /// The maximum value for the field in the manifests
    /// partitions.
    pub upper_bound: Option<Vec<u8>>,
}

/// A manifest is an immutable Avro file that lists data files or delete
/// files, along with each file’s partition data tuple, metrics, and tracking
/// information.
#[derive(Debug, PartialEq, Clone)]
pub struct ManifestEntry {
    /// field: 0
    ///
    /// Used to track additions and deletions.
    pub status: ManifestStatus,
    /// field id: 1
    ///
    /// Snapshot id where the file was added, or deleted if status is 2.
    /// Inherited when null.
    pub snapshot_id: Option<i64>,
    /// field id: 3
    ///
    /// Data sequence number of the file.
    /// Inherited when null and status is 1 (added).
    pub sequence_number: Option<i64>,
    /// field id: 4
    ///
    /// File sequence number indicating when the file was added.
    /// Inherited when null and status is 1 (added).
    pub file_sequence_number: Option<i64>,
    /// field id: 2
    ///
    /// File path, partition tuple, metrics, …
    pub data_file: DataFile,
}

impl ManifestEntry {
    /// Check if this manifest entry is deleted.
    pub fn is_alive(&self) -> bool {
        matches!(
            self.status,
            ManifestStatus::Added | ManifestStatus::Existing
        )
    }
}

mod manifest_file {
    use super::*;
    use once_cell::sync::Lazy;
    pub static STATUS: Lazy<Field> =
        Lazy::new(|| Field::required(0, "status", Any::Primitive(Primitive::Int)));
    pub static SNAPSHOT_ID: Lazy<Field> =
        Lazy::new(|| Field::optional(1, "snapshot_id", Any::Primitive(Primitive::Long)));
    pub static SEQUENCE_NUMBER: Lazy<Field> =
        Lazy::new(|| Field::optional(3, "sequence_number", Any::Primitive(Primitive::Long)));
    pub static FILE_SEQUENCE_NUMBER: Lazy<Field> =
        Lazy::new(|| Field::optional(4, "file_sequence_number", Any::Primitive(Primitive::Long)));

    pub const DATA_FILE_ID: i32 = 2;
    pub const DATA_FILE_NAME: &str = "data_file";
}

/// FIXME: partition_spec is not parsed.
#[derive(Debug, PartialEq, Clone)]
pub struct ManifestMetadata {
    /// The table schema at the time the manifest
    /// was written
    pub schema: Schema,
    /// ID of the schema used to write the manifest as a string
    pub schema_id: i32,

    /// The partition spec used  to write the manifest
    pub partition_spec: PartitionSpec,

    /// Table format version number of the manifest as a string
    pub format_version: Option<TableFormatVersion>,
    /// Type of content files tracked by the manifest: “data” or “deletes”
    pub content: ManifestContentType,
}

/// A manifest contains metadata and a list of entries.
#[derive(Debug, PartialEq, Clone)]
pub struct ManifestFile {
    /// Metadata of a manifest file.
    pub metadata: ManifestMetadata,
    /// Entries in manifest file.
    pub entries: Vec<ManifestEntry>,
}

impl ManifestFile {
    pub(crate) fn v2_schema(partition_type: Struct) -> Schema {
        Schema::new(
            0,
            None,
            Struct::new(vec![
                manifest_file::STATUS.clone().into(),
                manifest_file::SNAPSHOT_ID.clone().into(),
                manifest_file::SEQUENCE_NUMBER.clone().into(),
                manifest_file::FILE_SEQUENCE_NUMBER.clone().into(),
                Field::required(
                    manifest_file::DATA_FILE_ID,
                    manifest_file::DATA_FILE_NAME,
                    Any::Struct(
                        Struct::new(vec![
                            datafile::CONTENT.clone().with_required().into(),
                            datafile::FILE_PATH.clone().into(),
                            datafile::FILE_FORMAT.clone().into(),
                            DataFile::partition_field(partition_type).into(),
                            datafile::RECORD_COUNT.clone().into(),
                            datafile::FILE_SIZE.clone().into(),
                            datafile::COLUMN_SIZES.clone().into(),
                            datafile::VALUE_COUNTS.clone().into(),
                            datafile::NULL_VALUE_COUNTS.clone().into(),
                            datafile::NAN_VALUE_COUNTS.clone().into(),
                            datafile::LOWER_BOUNDS.clone().into(),
                            datafile::UPPER_BOUNDS.clone().into(),
                            datafile::KEY_METADATA.clone().into(),
                            datafile::SPLIT_OFFSETS.clone().into(),
                            datafile::EQUALITY_IDS.clone().into(),
                            datafile::SORT_ORDER_ID.clone().into(),
                        ])
                        .into(),
                    ),
                )
                .into(),
            ]),
        )
    }
}

/// Type of content files tracked by the manifest
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ManifestContentType {
    /// The manifest content is data.
    Data = 0,
    /// The manifest content is deletes.
    Deletes = 1,
}

impl Display for ManifestContentType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestContentType::Data => write!(f, "data"),
            ManifestContentType::Deletes => write!(f, "deletes"),
        }
    }
}

impl FromStr for ManifestContentType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "data" => Ok(ManifestContentType::Data),
            "deletes" => Ok(ManifestContentType::Deletes),
            _ => Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("Invalid manifest content type: {s}"),
            )),
        }
    }
}

impl TryFrom<u8> for ManifestContentType {
    type Error = Error;

    fn try_from(v: u8) -> Result<ManifestContentType> {
        match v {
            0 => Ok(ManifestContentType::Data),
            1 => Ok(ManifestContentType::Deletes),
            _ => Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("manifest content type {} is invalid", v),
            )),
        }
    }
}

/// Used to track additions and deletions in ManifestEntry.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ManifestStatus {
    /// Value: 0
    Existing = 0,
    /// Value: 1
    Added = 1,
    /// Value: 2
    ///
    /// Deletes are informational only and not used in scans.
    Deleted = 2,
}

impl TryFrom<u8> for ManifestStatus {
    type Error = Error;

    fn try_from(v: u8) -> Result<ManifestStatus> {
        match v {
            0 => Ok(ManifestStatus::Existing),
            1 => Ok(ManifestStatus::Added),
            2 => Ok(ManifestStatus::Deleted),
            _ => Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("manifest status {} is invalid", v),
            )),
        }
    }
}

/// This type used to build DataFile.
pub struct DataFileBuilder {
    meta_data: FileMetaData,
    file_location: String,
    written_size: u64,
    table_location: String,
    content: Option<DataContentType>,
    partition_value: Option<StructValue>,
    equality_ids: Option<Vec<i32>>,
}

impl DataFileBuilder {
    /// Create a new `DataFileBuilder`.
    pub fn new(
        meta_data: FileMetaData,
        table_location: String,
        file_location: String,
        written_size: u64,
    ) -> Self {
        Self {
            meta_data,
            file_location,
            written_size,
            table_location,
            content: None,
            partition_value: None,
            equality_ids: None,
        }
    }

    /// Set the content type of this data file.
    /// This function must be call before build.
    pub fn with_content(self, content: DataContentType) -> Self {
        Self {
            content: Some(content),
            ..self
        }
    }

    /// Set the partition value of this data file.
    pub fn with_partition_value(self, value: Option<StructValue>) -> Self {
        Self {
            partition_value: value,
            ..self
        }
    }

    /// Set the equality ids of this data file.
    pub fn with_equality_ids(self, ids: Vec<i32>) -> Self {
        Self {
            equality_ids: Some(ids),
            ..self
        }
    }

    /// Build the `DataFile`.
    pub fn build(self) -> DataFile {
        log::info!("{:?}", self.meta_data);
        let (column_sizes, value_counts, null_value_counts, distinct_counts) = {
            // how to decide column id
            let mut per_col_size: HashMap<i32, _> = HashMap::new();
            let mut per_col_val_num: HashMap<i32, _> = HashMap::new();
            let mut per_col_null_val_num: HashMap<i32, _> = HashMap::new();
            let mut per_col_distinct_val_num: HashMap<i32, _> = HashMap::new();
            self.meta_data.row_groups.iter().for_each(|group| {
                group
                    .columns
                    .iter()
                    .enumerate()
                    .for_each(|(column_id, column_chunk)| {
                        if let Some(column_chunk_metadata) = &column_chunk.meta_data {
                            *per_col_size.entry(column_id as i32).or_insert(0) +=
                                column_chunk_metadata.total_compressed_size;
                            *per_col_val_num.entry(column_id as i32).or_insert(0) +=
                                column_chunk_metadata.num_values;
                            *per_col_null_val_num
                                .entry(column_id as i32)
                                .or_insert(0_i64) += column_chunk_metadata
                                .statistics
                                .as_ref()
                                .map(|s| s.null_count)
                                .unwrap_or(None)
                                .unwrap_or(0);
                            *per_col_distinct_val_num
                                .entry(column_id as i32)
                                .or_insert(0_i64) += column_chunk_metadata
                                .statistics
                                .as_ref()
                                .map(|s| s.distinct_count)
                                .unwrap_or(None)
                                .unwrap_or(0);
                        }
                    })
            });
            (
                per_col_size,
                per_col_val_num,
                per_col_null_val_num,
                per_col_distinct_val_num,
            )
        };

        // equality_ids is required when content is EqualityDeletes.
        if self.content.unwrap() == DataContentType::EqualityDeletes {
            assert!(self.equality_ids.is_some());
        }

        DataFile {
            content: self.content.unwrap(),
            file_path: format!("{}/{}", self.table_location, self.file_location),
            file_format: crate::types::DataFileFormat::Parquet,
            // # NOTE
            // DataFileWriter only response to write data. Partition should place by more high level writer.
            partition: self.partition_value.unwrap_or_default(),
            record_count: self.meta_data.num_rows,
            column_sizes: Some(column_sizes),
            value_counts: Some(value_counts),
            null_value_counts: Some(null_value_counts),
            distinct_counts: Some(distinct_counts),
            key_metadata: self.meta_data.footer_signing_key_metadata,
            file_size_in_bytes: self.written_size as i64,
            // # TODO
            //
            // Following fields unsupported now:
            // - `file_size_in_bytes` can't get from `FileMetaData` now.
            // - `file_offset` in `FileMetaData` always be None now.
            // - `nan_value_counts` can't get from `FileMetaData` now.
            // Currently arrow parquet writer doesn't fill row group offsets, we can use first column chunk offset for it.
            split_offsets: Some(
                self.meta_data
                    .row_groups
                    .iter()
                    .filter_map(|group| group.file_offset)
                    .collect(),
            ),
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            equality_ids: self.equality_ids,
            sort_order_id: None,
        }
    }
}

/// Data file carries data file path, partition tuple, metrics, …
#[derive(Debug, PartialEq, Clone, Builder)]
#[builder(name = "DataFileBuilderV2", setter(prefix = "with"))]
pub struct DataFile {
    /// field id: 134
    ///
    /// Type of content stored by the data file: data, equality deletes,
    /// or position deletes (all v1 files are data files)
    pub content: DataContentType,
    /// field id: 100
    ///
    /// Full URI for the file with FS scheme
    pub file_path: String,
    /// field id: 101
    ///
    /// String file format name, avro, orc or parquet
    pub file_format: DataFileFormat,
    /// field id: 102
    ///
    /// Partition data tuple, schema based on the partition spec output using
    /// partition field ids for the struct field ids
    pub partition: StructValue,
    /// field id: 103
    ///
    /// Number of records in this file
    pub record_count: i64,
    /// field id: 104
    ///
    /// Total file size in bytes
    pub file_size_in_bytes: i64,
    /// field id: 108
    /// key field id: 117
    /// value field id: 118
    ///
    /// Map from column id to the total size on disk of all regions that
    /// store the column. Does not include bytes necessary to read other
    /// columns, like footers. Leave null for row-oriented formats (Avro)
    #[builder(setter(strip_option), default)]
    pub column_sizes: Option<HashMap<i32, i64>>,
    /// field id: 109
    /// key field id: 119
    /// value field id: 120
    ///
    /// Map from column id to number of values in the column (including null
    /// and NaN values)
    #[builder(setter(strip_option), default)]
    pub value_counts: Option<HashMap<i32, i64>>,
    /// field id: 110
    /// key field id: 121
    /// value field id: 122
    ///
    /// Map from column id to number of null values in the column
    #[builder(setter(strip_option), default)]
    pub null_value_counts: Option<HashMap<i32, i64>>,
    /// field id: 137
    /// key field id: 138
    /// value field id: 139
    ///
    /// Map from column id to number of NaN values in the column
    #[builder(setter(strip_option), default)]
    pub nan_value_counts: Option<HashMap<i32, i64>>,
    /// field id: 111
    /// key field id: 123
    /// value field id: 124
    ///
    /// Map from column id to number of distinct values in the column;
    /// distinct counts must be derived using values in the file by counting
    /// or using sketches, but not using methods like merging existing
    /// distinct counts
    #[builder(setter(strip_option), default)]
    pub distinct_counts: Option<HashMap<i32, i64>>,
    /// field id: 125
    /// key field id: 126
    /// value field id: 127
    ///
    /// Map from column id to lower bound in the column serialized as binary.
    /// Each value must be less than or equal to all non-null, non-NaN values
    /// in the column for the file.
    ///
    /// Reference:
    ///
    /// - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
    #[builder(setter(strip_option), default)]
    pub lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// field id: 128
    /// key field id: 129
    /// value field id: 130
    ///
    /// Map from column id to upper bound in the column serialized as binary.
    /// Each value must be greater than or equal to all non-null, non-Nan
    /// values in the column for the file.
    ///
    /// Reference:
    ///
    /// - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
    #[builder(setter(strip_option), default)]
    pub upper_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// field id: 131
    ///
    /// Implementation-specific key metadata for encryption
    pub key_metadata: Option<Vec<u8>>,
    /// field id: 132
    /// element field id: 133
    ///
    /// Split offsets for the data file. For example, all row group offsets
    /// in a Parquet file. Must be sorted ascending
    #[builder(setter(strip_option), default)]
    pub split_offsets: Option<Vec<i64>>,
    /// field id: 135
    /// element field id: 136
    ///
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and should be null
    /// otherwise. Fields with ids listed in this column must be present
    /// in the delete file
    #[builder(setter(strip_option), default)]
    pub equality_ids: Option<Vec<i32>>,
    /// field id: 140
    ///
    /// ID representing sort order for this file.
    ///
    /// If sort order ID is missing or unknown, then the order is assumed to
    /// be unsorted. Only data files and equality delete files should be
    /// written with a non-null order id. Position deletes are required to be
    /// sorted by file and position, not a table order, and should set sort
    /// order id to null. Readers must ignore sort order id for position
    /// delete files.
    #[builder(setter(strip_option), default)]
    pub sort_order_id: Option<i32>,
}

mod datafile {
    use super::*;
    use once_cell::sync::Lazy;
    pub static CONTENT: Lazy<Field> = Lazy::new(|| {
        Field::optional(134, "content", Any::Primitive(Primitive::Int))
            .with_comment("Contents of the file: 0=data, 1=position deletes, 2=equality deletes")
    });
    pub static FILE_PATH: Lazy<Field> = Lazy::new(|| {
        Field::required(100, "file_path", Any::Primitive(Primitive::String))
            .with_comment("Location URI with FS scheme")
    });
    pub static FILE_FORMAT: Lazy<Field> = Lazy::new(|| {
        Field::required(101, "file_format", Any::Primitive(Primitive::String))
            .with_comment("File format name: avro, orc, or parquet")
    });
    pub static RECORD_COUNT: Lazy<Field> = Lazy::new(|| {
        Field::required(103, "record_count", Any::Primitive(Primitive::Long))
            .with_comment("Number of records in the file")
    });
    pub static FILE_SIZE: Lazy<Field> = Lazy::new(|| {
        Field::required(104, "file_size_in_bytes", Any::Primitive(Primitive::Long))
            .with_comment("Total file size in bytes")
    });
    pub static COLUMN_SIZES: Lazy<Field> = Lazy::new(|| {
        Field::optional(
            108,
            "column_sizes",
            Any::Map(Map {
                key_id: 117,
                key_type: Box::new(Any::Primitive(Primitive::Int)),
                value_id: 118,
                value_type: Box::new(Any::Primitive(Primitive::Long)),
                value_required: true,
            }),
        )
        .with_comment("Map of column id to total size on disk")
    });
    pub static VALUE_COUNTS: Lazy<Field> = Lazy::new(|| {
        Field::optional(
            109,
            "value_counts",
            Any::Map(Map {
                key_id: 119,
                key_type: Box::new(Any::Primitive(Primitive::Int)),
                value_id: 120,
                value_type: Box::new(Any::Primitive(Primitive::Long)),
                value_required: true,
            }),
        )
        .with_comment("Map of column id to total count, including null and NaN")
    });
    pub static NULL_VALUE_COUNTS: Lazy<Field> = Lazy::new(|| {
        Field::optional(
            110,
            "null_value_counts",
            Any::Map(Map {
                key_id: 121,
                key_type: Box::new(Any::Primitive(Primitive::Int)),
                value_id: 122,
                value_type: Box::new(Any::Primitive(Primitive::Long)),
                value_required: true,
            }),
        )
        .with_comment("Map of column id to null value count")
    });
    pub static NAN_VALUE_COUNTS: Lazy<Field> = Lazy::new(|| {
        Field::optional(
            137,
            "nan_value_counts",
            Any::Map(Map {
                key_id: 138,
                key_type: Box::new(Any::Primitive(Primitive::Int)),
                value_id: 139,
                value_type: Box::new(Any::Primitive(Primitive::Long)),
                value_required: true,
            }),
        )
        .with_comment("Map of column id to number of NaN values in the column")
    });
    pub static LOWER_BOUNDS: Lazy<Field> = Lazy::new(|| {
        Field::optional(
            125,
            "lower_bounds",
            Any::Map(Map {
                key_id: 126,
                key_type: Box::new(Any::Primitive(Primitive::Int)),
                value_id: 127,
                value_type: Box::new(Any::Primitive(Primitive::Binary)),
                value_required: true,
            }),
        )
        .with_comment("Map of column id to lower bound")
    });
    pub static UPPER_BOUNDS: Lazy<Field> = Lazy::new(|| {
        Field::optional(
            128,
            "upper_bounds",
            Any::Map(Map {
                key_id: 129,
                key_type: Box::new(Any::Primitive(Primitive::Int)),
                value_id: 130,
                value_type: Box::new(Any::Primitive(Primitive::Binary)),
                value_required: true,
            }),
        )
        .with_comment("Map of column id to upper bound")
    });
    pub static KEY_METADATA: Lazy<Field> = Lazy::new(|| {
        Field::optional(131, "key_metadata", Any::Primitive(Primitive::Binary))
            .with_comment("Encryption key metadata blob")
    });
    pub static SPLIT_OFFSETS: Lazy<Field> = Lazy::new(|| {
        Field::optional(
            132,
            "split_offsets",
            Any::List(List {
                element_id: 133,
                element_required: true,
                element_type: Box::new(Any::Primitive(Primitive::Long)),
            }),
        )
        .with_comment("Splittable offsets")
    });
    pub static EQUALITY_IDS: Lazy<Field> = Lazy::new(|| {
        Field::optional(
            135,
            "equality_ids",
            Any::List(List {
                element_id: 136,
                element_required: true,
                element_type: Box::new(Any::Primitive(Primitive::Int)),
            }),
        )
        .with_comment("Equality comparison field IDs")
    });
    pub static SORT_ORDER_ID: Lazy<Field> = Lazy::new(|| {
        Field::optional(140, "sort_order_id", Any::Primitive(Primitive::Int))
            .with_comment("Sort order ID")
    });
}

impl DataFile {
    pub(crate) fn partition_field(partition_type: Struct) -> Field {
        Field::required(102, "partition", Any::Struct(partition_type.into()))
            .with_comment("Partition data tuple, schema based on the partition spec")
    }

    #[cfg(test)]
    pub(crate) fn new(
        content: DataContentType,
        file_path: impl Into<String>,
        file_format: DataFileFormat,
        record_count: i64,
        file_size_in_bytes: i64,
    ) -> Self {
        Self {
            content,
            file_path: file_path.into(),
            file_format,
            // // TODO: Should not use default partition here. Replace it after introduce deserialize of `StructValue`.
            partition: StructValue::default(),
            record_count,
            file_size_in_bytes,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            distinct_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
        }
    }
}

/// Type of content stored by the data file: data, equality deletes, or
/// position deletes (all v1 files are data files)
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DataContentType {
    /// value: 0
    Data = 0,
    /// value: 1
    PositionDeletes = 1,
    /// value: 2
    EqualityDeletes = 2,
}

impl TryFrom<u8> for DataContentType {
    type Error = Error;

    fn try_from(v: u8) -> Result<DataContentType> {
        match v {
            0 => Ok(DataContentType::Data),
            1 => Ok(DataContentType::PositionDeletes),
            2 => Ok(DataContentType::EqualityDeletes),
            _ => Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("data content type {} is invalid", v),
            )),
        }
    }
}

/// Format of this data.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum DataFileFormat {
    /// Avro file format: <https://avro.apache.org/>
    Avro,
    /// Orc file format: <https://orc.apache.org/>
    Orc,
    /// Parquet file format: <https://parquet.apache.org/>
    Parquet,
}

impl FromStr for DataFileFormat {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "avro" => Ok(Self::Avro),
            "orc" => Ok(Self::Orc),
            "parquet" => Ok(Self::Parquet),
            _ => Err(Error::new(
                ErrorKind::IcebergFeatureUnsupported,
                format!("Unsupported data file format: {}", s),
            )),
        }
    }
}

impl Display for DataFileFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFileFormat::Avro => f.write_str("avro"),
            DataFileFormat::Orc => f.write_str("orc"),
            DataFileFormat::Parquet => f.write_str("parquet"),
        }
    }
}

/// Snapshot of contains all data of a table at a point in time.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Snapshot {
    /// A unique long ID
    pub snapshot_id: i64,
    /// The snapshot ID of the snapshot’s parent. Omitted for any snapshot
    /// with no parent
    pub parent_snapshot_id: Option<i64>,
    /// A monotonically increasing long that tracks the order of changes to a
    /// table
    pub sequence_number: i64,
    /// A timestamp when the snapshot was created, used for garbage
    /// collection and table inspection
    pub timestamp_ms: i64,
    /// The location of a manifest list for this snapshot that tracks
    /// manifest files with additional metadata
    pub manifest_list: String,
    /// A string map that summarizes the snapshot changes, including
    /// operation (see below)
    ///
    /// The snapshot summary’s operation field is used by some operations,
    /// like snapshot expiration, to skip processing certain snapshots.
    /// Possible operation values are:
    ///
    /// - append – Only data files were added and no files were removed.
    /// - replace – Data and delete files were added and removed without changing table data; i.e., compaction, changing the data file format, or relocating data files.
    /// - overwrite – Data and delete files were added and removed in a logical overwrite operation.
    /// - delete – Data files were removed and their contents logically deleted and/or delete files were added to delete rows.
    ///
    /// For example:
    ///
    /// ```json
    /// {
    ///   "operation" : "append",
    ///   "spark.app.id" : "local-1686911651377",
    ///   "added-data-files" : "3",
    ///   "added-records" : "3",
    ///   "added-files-size" : "1929",
    ///   "changed-partition-count" : "1",
    ///   "total-records" : "3",
    ///   "total-files-size" : "1929",
    ///   "total-data-files" : "3",
    ///   "total-delete-files" : "0",
    ///   "total-position-deletes" : "0",
    ///   "total-equality-deletes" : "0"
    /// }
    /// ```
    pub summary: HashMap<String, String>,
    /// ID of the table’s current schema when the snapshot was created
    pub schema_id: Option<i64>,
}

impl Snapshot {
    pub(crate) async fn load_manifest_list(&self, op: &Operator) -> Result<ManifestList> {
        parse_manifest_list(
            &op.read(Table::relative_path(op, self.manifest_list.as_str())?.as_str())
                .await?
                .to_vec(),
        )
    }

    pub(crate) fn log(&self) -> SnapshotLog {
        SnapshotLog {
            timestamp_ms: self.timestamp_ms,
            snapshot_id: self.snapshot_id,
        }
    }
}

#[derive(Default)]
pub struct SnapshotSummaryBuilder {
    added_data_files: i64,
    added_delete_files: i64,
    added_equality_delete_files: i64,
    added_position_delete_files: i64,

    added_data_records: i64,
    added_position_deletes_records: i64,
    added_equality_deletes_records: i64,

    added_files_size: i64,
}

impl SnapshotSummaryBuilder {
    const OPERATION: &'static str = "operation";
    const ADDED_DATA_FILES: &'static str = "added-data-files";
    const TOTAL_DATA_FILES: &'static str = "total-data-files";
    const ADDED_DELETE_FILES: &'static str = "added-delete-files";
    const ADDED_EQUALITY_DELETE_FILES: &'static str = "added-equality-delete-files";
    const ADDED_POSITION_DELETE_FILES: &'static str = "added-position-delete-files";
    const TOTAL_DELETE_FILES: &'static str = "total-delete-files";
    const ADDED_RECORDS: &'static str = "added-records";
    const TOTAL_RECORDS: &'static str = "total-records";
    const ADDED_POSITION_DELETES: &'static str = "added-position-deletes";
    const TOTAL_POSITION_DELETES: &'static str = "total-position-deletes";
    const ADDED_EQUALITY_DELETES: &'static str = "added-equality-deletes";
    const TOTAL_EQUALITY_DELETES: &'static str = "total-equality-deletes";
    const ADDED_FILES_SIZE: &'static str = "added-files-size";
    const TOTAL_FILES_SIZE: &'static str = "total-files-size";

    pub fn new() -> Self {
        Default::default()
    }

    pub fn add(&mut self, datafile: &DataFile) {
        match datafile.content {
            DataContentType::Data => {
                self.added_data_files += 1;
                self.added_data_records += datafile.record_count;
                self.added_files_size += datafile.file_size_in_bytes;
            }
            DataContentType::PositionDeletes => {
                self.added_delete_files += 1;
                self.added_position_delete_files += 1;
                self.added_position_deletes_records += datafile.record_count;
                self.added_files_size += datafile.file_size_in_bytes;
            }
            DataContentType::EqualityDeletes => {
                self.added_delete_files += 1;
                self.added_equality_delete_files += 1;
                self.added_equality_deletes_records += datafile.record_count;
                self.added_files_size += datafile.file_size_in_bytes;
            }
        }
    }

    fn operation(&self) -> String {
        if self.added_delete_files == 0 && self.added_data_files != 0 {
            "append".to_string()
        } else if self.added_delete_files != 0 && self.added_data_files != 0 {
            "overwrite".to_string()
        } else if self.added_delete_files != 0 && self.added_data_files == 0 {
            "delete".to_string()
        } else {
            "append".to_string()
        }
    }

    pub fn merge(
        self,
        last_summary: &HashMap<String, String>,
        is_compact_op: bool,
    ) -> Result<HashMap<String, String>> {
        let operation = if is_compact_op {
            "replace".to_string()
        } else {
            self.operation()
        };

        #[inline]
        fn get_i64(value: &HashMap<String, String>, key: &str) -> std::result::Result<i64, Error> {
            Ok(value
                .get(key)
                .map(|val| val.parse::<i64>())
                .transpose()?
                .unwrap_or_default())
        }

        let added_data_files = self.added_data_files;
        let total_data_files = {
            let total_data_files = get_i64(last_summary, Self::TOTAL_DATA_FILES)?;
            total_data_files + self.added_data_files
        };
        let added_delete_files = self.added_delete_files;
        let added_equality_delete_files = self.added_equality_delete_files;
        let added_position_delete_files = self.added_position_delete_files;
        let total_delete_files = {
            let total_delete_files = get_i64(last_summary, Self::TOTAL_DELETE_FILES)?;
            total_delete_files + self.added_delete_files
        };
        let added_records = self.added_data_records;
        let total_records = {
            let total_records = get_i64(last_summary, Self::TOTAL_RECORDS)?;
            total_records + self.added_data_records
        };
        let added_position_deletes = self.added_position_deletes_records;
        let total_position_deletes = {
            let total_position_deletes = get_i64(last_summary, Self::TOTAL_POSITION_DELETES)?;
            total_position_deletes + self.added_position_deletes_records
        };
        let added_equality_deletes = self.added_equality_deletes_records;
        let total_equality_deletes = {
            let total_equality_deletes = get_i64(last_summary, Self::TOTAL_EQUALITY_DELETES)?;
            total_equality_deletes + self.added_equality_deletes_records
        };
        let added_files_size = self.added_files_size;
        let total_files_size = {
            let total_files_size = get_i64(last_summary, Self::TOTAL_FILES_SIZE)?;
            total_files_size + self.added_files_size
        };

        let mut m = HashMap::with_capacity(16);
        m.insert(Self::OPERATION.to_string(), operation);
        m.insert(
            Self::ADDED_DATA_FILES.to_string(),
            added_data_files.to_string(),
        );
        m.insert(
            Self::TOTAL_DATA_FILES.to_string(),
            total_data_files.to_string(),
        );
        m.insert(
            Self::ADDED_DELETE_FILES.to_string(),
            added_delete_files.to_string(),
        );
        m.insert(
            Self::ADDED_EQUALITY_DELETE_FILES.to_string(),
            added_equality_delete_files.to_string(),
        );
        m.insert(
            Self::ADDED_POSITION_DELETE_FILES.to_string(),
            added_position_delete_files.to_string(),
        );
        m.insert(
            Self::TOTAL_DELETE_FILES.to_string(),
            total_delete_files.to_string(),
        );
        m.insert(Self::ADDED_RECORDS.to_string(), added_records.to_string());
        m.insert(Self::TOTAL_RECORDS.to_string(), total_records.to_string());
        m.insert(
            Self::ADDED_POSITION_DELETES.to_string(),
            added_position_deletes.to_string(),
        );
        m.insert(
            Self::TOTAL_POSITION_DELETES.to_string(),
            total_position_deletes.to_string(),
        );
        m.insert(
            Self::ADDED_EQUALITY_DELETES.to_string(),
            added_equality_deletes.to_string(),
        );
        m.insert(
            Self::TOTAL_EQUALITY_DELETES.to_string(),
            total_equality_deletes.to_string(),
        );
        m.insert(
            Self::ADDED_FILES_SIZE.to_string(),
            added_files_size.to_string(),
        );
        m.insert(
            Self::TOTAL_FILES_SIZE.to_string(),
            total_files_size.to_string(),
        );

        Ok(m)
    }
}

/// timestamp and snapshot ID pairs that encodes changes to the current
/// snapshot for the table.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct SnapshotLog {
    /// The timestamp of this snapshot log.
    ///
    /// TODO: we should use `chrono::DateTime` instead of `i64`.
    pub timestamp_ms: i64,
    /// The snapshot ID of this snapshot log.
    pub snapshot_id: i64,
}

/// Iceberg tables keep track of branches and tags using snapshot references.
///
/// Tags are labels for individual snapshots. Branches are mutable named
/// references that can be updated by committing a new snapshot as the
/// branch’s referenced snapshot using the Commit Conflict Resolution and
/// Retry procedures.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SnapshotReference {
    /// A reference’s snapshot ID. The tagged snapshot or latest snapshot of
    /// a branch.
    pub snapshot_id: i64,
    /// Type of the reference, tag or branch
    pub typ: SnapshotReferenceType,
    /// For branch type only.
    ///
    /// A positive number for the minimum number of snapshots to keep in a
    /// branch while expiring snapshots.
    ///
    /// Defaults to table property `history.expire.min-snapshots-to-keep`.
    pub min_snapshots_to_keep: Option<i32>,
    /// For branch type only.
    ///
    /// A positive number for the max age of snapshots to keep when expiring,
    /// including the latest snapshot.
    ///
    /// Defaults to table property `history.expire.max-snapshot-age-ms`.
    pub max_snapshot_age_ms: Option<i64>,
    /// For snapshot references except the `main` branch.
    ///
    /// A positive number for the max age of the snapshot reference to keep
    /// while expiring snapshots.
    ///
    /// Defaults to table property `history.expire.max-ref-age-ms`
    ///
    /// The main branch never expires.
    pub max_ref_age_ms: Option<i64>,
}

impl SnapshotReference {
    pub(crate) fn new(snapshot_id: i64, typ: SnapshotReferenceType) -> Self {
        Self {
            snapshot_id,
            typ,
            min_snapshots_to_keep: None,
            max_snapshot_age_ms: None,
            max_ref_age_ms: None,
        }
    }
}

/// Type of the reference
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SnapshotReferenceType {
    /// Tag is used to reference to a specific tag.
    Tag,
    /// Branch is used to reference to a specific branch, like `master`.
    Branch,
}

impl Display for SnapshotReferenceType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotReferenceType::Tag => f.write_str("tag"),
            SnapshotReferenceType::Branch => f.write_str("branch"),
        }
    }
}

impl FromStr for SnapshotReferenceType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "tag" => Ok(SnapshotReferenceType::Tag),
            "branch" => Ok(SnapshotReferenceType::Branch),
            _ => Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("Invalid snapshot reference type: {s}"),
            )),
        }
    }
}

/// timestamp and metadata file location pairs that encodes changes to the
/// previous metadata files for the table
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct MetadataLog {
    /// Related timestamp for this metadata log.
    pub timestamp_ms: i64,
    /// The metadata file's location.
    pub metadata_file: String,
}

/// Table metadata is stored as JSON. Each table metadata change creates a
/// new table metadata file that is committed by an atomic operation. This
/// operation is used to ensure that a new version of table metadata replaces
/// the version on which it was based. This produces a linear history of
/// table versions and ensures that concurrent writes are not lost.
///
/// TODO: statistics is not supported.
#[derive(Debug, PartialEq, Clone)]
pub struct TableMetadata {
    /// Currently, this can be 1 or 2 based on the spec. Implementations
    /// must throw an exception if a table’s version is higher than the
    /// supported version.
    pub format_version: TableFormatVersion,
    /// A UUID that identifies the table, generated when the table is
    /// created. Implementations must throw an exception if a table’s UUID
    /// does not match the expected UUID after refreshing metadata.
    pub table_uuid: String,
    /// The table’s base location.
    ///
    /// This is used by writers to determine where to store data files, manifest files, and table metadata files.
    pub location: String,
    /// The table’s highest assigned sequence number, a monotonically
    /// increasing long that tracks the order of snapshots in a table.
    pub last_sequence_number: i64,
    /// Timestamp in milliseconds from the unix epoch when the table was last
    /// updated. Each table metadata file should update this field just
    /// before writing.
    pub last_updated_ms: i64,
    /// The highest assigned column ID for the table.
    ///
    /// This is used to ensure columns are always assigned an unused ID when
    /// evolving schemas.
    pub last_column_id: i32,
    /// A list of schemas, stored as objects with schema-id.
    pub schemas: Vec<Schema>,
    /// ID of the table’s current schema.
    pub current_schema_id: i32,
    /// A list of partition specs, stored as full partition spec objects.
    pub partition_specs: Vec<PartitionSpec>,
    /// ID of the “current” spec that writers should use by default.
    pub default_spec_id: i32,
    /// the highest assigned partition field ID across all partition specs
    /// for the table. This is used to ensure partition fields are always
    /// assigned an unused ID when evolving specs.
    pub last_partition_id: i32,
    /// A string to string map of table properties.
    ///
    /// This is used to control settings that affect reading and writing and
    /// is not intended to be used for arbitrary metadata. For example,
    /// `commit.retry.num-retries` is used to control the number of commit
    /// retries.
    pub properties: Option<HashMap<String, String>>,
    /// ID of the current table snapshot; must be the same as the current ID
    /// of the main branch in refs.
    pub current_snapshot_id: Option<i64>,
    /// A list of valid snapshots.
    ///
    /// Valid snapshots are snapshots for which all data files exist in the
    /// file system. A data file must not be deleted from the file system
    /// until the last snapshot in which it was listed is garbage collected.
    pub snapshots: Option<Vec<Snapshot>>,
    /// A list (optional) of timestamp and metadata file location pairs that
    /// encodes changes to the previous metadata files for the table.
    ///
    /// Each time a new metadata file is created, a new entry of the previous
    /// metadata file location should be added to the list. Tables can be
    /// configured to remove oldest metadata log entries and keep a
    /// fixed-size log of the most recent entries after a commit.
    pub snapshot_log: Option<Vec<SnapshotLog>>,
    /// A list (optional) of timestamp and metadata file location pairs that
    /// encodes changes to the previous metadata files for the table.
    ///
    /// Each time a new metadata file is created, a new entry of the previous
    /// metadata file location should be added to the list. Tables can be
    /// configured to remove oldest metadata log entries and keep a
    /// fixed-size log of the most recent entries after a commit.
    pub metadata_log: Option<Vec<MetadataLog>>,
    /// A list of sort orders, stored as full sort order objects.
    pub sort_orders: Vec<SortOrder>,
    /// Default sort order id of the table.
    ///
    /// Note that this could be used by writers, but is not used when reading
    /// because reads use the specs stored in manifest files.
    pub default_sort_order_id: i32,
    /// A map of snapshot references.
    ///
    /// The map keys are the unique snapshot reference names in the table,
    /// and the map values are snapshot reference objects.
    ///
    /// There is always a main branch reference pointing to the
    /// `current-snapshot-id` even if the refs map is null.
    pub refs: HashMap<String, SnapshotReference>,
}

impl TableMetadata {
    /// Current partition spec.
    pub fn current_partition_spec(&self) -> Result<&PartitionSpec> {
        self.partition_spec(self.default_spec_id).ok_or_else(|| {
            Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("Partition spec id {} not found!", self.default_spec_id),
            )
        })
    }

    /// Partition spec by id.
    pub fn partition_spec(&self, spec_id: i32) -> Option<&PartitionSpec> {
        self.partition_specs.iter().find(|p| p.spec_id == spec_id)
    }

    /// Current schema.
    pub fn current_schema(&self) -> Result<&Schema> {
        self.schema(self.current_schema_id).ok_or_else(|| {
            Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("Schema id {} not found!", self.current_schema_id),
            )
        })
    }

    /// Get schema by id
    pub fn schema(&self, schema_id: i32) -> Option<&Schema> {
        self.schemas.iter().find(|s| s.schema_id == schema_id)
    }

    /// Current schema.
    pub fn current_snapshot(&self) -> Result<Option<&Snapshot>> {
        if self.current_snapshot_id == Some(EMPTY_SNAPSHOT_ID) || self.current_snapshot_id.is_none()
        {
            return Ok(None);
        }

        Ok(Some(
            self.snapshot(self.current_snapshot_id.unwrap())
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!(
                            "Snapshot id {} not found!",
                            self.current_snapshot_id.unwrap()
                        ),
                    )
                })?,
        ))
    }

    pub fn snapshot(&self, snapshot_id: i64) -> Option<&Snapshot> {
        if let Some(snapshots) = &self.snapshots {
            snapshots.iter().find(|s| s.snapshot_id == snapshot_id)
        } else {
            None
        }
    }

    /// Returns snapshot reference of branch
    pub fn snapshot_ref(&self, branch: &str) -> Option<&SnapshotReference> {
        self.refs.get(branch)
    }

    /// Set snapshot reference for branch
    pub fn set_snapshot_ref(&mut self, branch: &str, snap_ref: SnapshotReference) -> Result<()> {
        let snapshot = self
            .snapshots
            .as_ref()
            .and_then(|s| s.iter().find(|s| s.snapshot_id == snap_ref.snapshot_id))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("Snapshot id {} not found!", snap_ref.snapshot_id),
                )
            })?;
        self.refs
            .entry(branch.to_string())
            .and_modify(|s| {
                s.snapshot_id = snap_ref.snapshot_id;
                s.typ = snap_ref.typ;

                if let Some(min_snapshots_to_keep) = snap_ref.min_snapshots_to_keep {
                    s.min_snapshots_to_keep = Some(min_snapshots_to_keep);
                }

                if let Some(max_snapshot_age_ms) = snap_ref.max_snapshot_age_ms {
                    s.max_snapshot_age_ms = Some(max_snapshot_age_ms);
                }

                if let Some(max_ref_age_ms) = snap_ref.max_ref_age_ms {
                    s.max_ref_age_ms = Some(max_ref_age_ms);
                }
            })
            .or_insert_with(|| {
                SnapshotReference::new(snap_ref.snapshot_id, SnapshotReferenceType::Branch)
            });

        if branch == MAIN_BRANCH {
            self.current_snapshot_id = Some(snap_ref.snapshot_id);
            self.last_updated_ms = snapshot.timestamp_ms;
            self.last_sequence_number = snapshot.sequence_number;
            if let Some(snap_logs) = self.snapshot_log.as_mut() {
                snap_logs.push(snapshot.log());
            } else {
                self.snapshot_log = Some(vec![snapshot.log()]);
            }
        }
        Ok(())
    }

    pub(crate) fn add_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        if let Some(snapshots) = &mut self.snapshots {
            snapshots.push(snapshot);
        } else {
            self.snapshots = Some(vec![snapshot]);
        }

        Ok(())
    }
}

/// Table format version number.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TableFormatVersion {
    /// The V1 Table Format Version.
    V1 = 1,
    /// The V2 Table Format Version.
    V2 = 2,
}

impl TryFrom<u8> for TableFormatVersion {
    type Error = Error;

    fn try_from(value: u8) -> Result<TableFormatVersion> {
        match value {
            1 => Ok(TableFormatVersion::V1),
            2 => Ok(TableFormatVersion::V2),
            _ => Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("Unknown table format: {value}"),
            )),
        }
    }
}

impl Display for TableFormatVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableFormatVersion::V1 => f.write_str("1"),
            TableFormatVersion::V2 => f.write_str("2"),
        }
    }
}

#[cfg(test)]
mod test {
    use apache_avro::{schema, types::Value};
    use std::collections::HashMap;

    use crate::types::SnapshotSummaryBuilder;
    use crate::types::{Field, PrimitiveValue, Struct, StructValueBuilder};

    use super::{AnyValue, DataFile, StructValue};

    #[test]
    fn test_struct_to_avro() {
        let value = {
            let struct_value = {
                let struct_type = Struct::new(vec![
                    Field::optional(
                        1,
                        "a",
                        crate::types::Any::Primitive(crate::types::Primitive::Int),
                    )
                    .into(),
                    Field::required(
                        2,
                        "b",
                        crate::types::Any::Primitive(crate::types::Primitive::String),
                    )
                    .into(),
                ]);
                let mut builder = StructValueBuilder::new(struct_type.into());
                builder.add_field(1, None).unwrap();
                builder
                    .add_field(
                        2,
                        Some(AnyValue::Primitive(PrimitiveValue::String(
                            "hello".to_string(),
                        ))),
                    )
                    .unwrap();
                AnyValue::Struct(builder.build().unwrap())
            };

            let mut res = apache_avro::to_value(struct_value).unwrap();

            // Guarantee the order of fields order of field names for later compare.
            if let Value::Record(ref mut record) = res {
                record.sort_by(|a, b| a.0.cmp(&b.0));
            }

            res
        };

        let expect_value = {
            let raw_schema = r#"
                {
                    "type": "record",
                    "name": "test",
                    "fields": [
                        {"name": "a", "type": ["int","null"]},
                        {"name": "b", "type": "string"}
                    ]
                }
            "#;

            let schema = schema::Schema::parse_str(raw_schema).unwrap();

            let mut record = apache_avro::types::Record::new(&schema).unwrap();
            record.put("a", None::<String>);
            record.put("b", "hello");

            record.into()
        };

        assert_eq!(value, expect_value);
    }

    #[test]
    fn test_struct_field_id_lookup() {
        let struct_type1 = Struct::new(vec![
            Field::optional(
                1,
                "a",
                crate::types::Any::Primitive(crate::types::Primitive::Int),
            )
            .into(),
            Field::required(
                2,
                "b",
                crate::types::Any::Primitive(crate::types::Primitive::String),
            )
            .into(),
        ]);
        let struct_type2 = Struct::new(vec![
            Field::required(3, "c", crate::types::Any::Struct(struct_type1.into())).into(),
            Field::required(
                4,
                "d",
                crate::types::Any::Primitive(crate::types::Primitive::Int),
            )
            .into(),
        ]);
        assert_eq!(struct_type2.lookup_field(1).unwrap().name, "a");
        assert_eq!(struct_type2.lookup_field(2).unwrap().name, "b");
        assert_eq!(struct_type2.lookup_field(3).unwrap().name, "c");
        assert_eq!(struct_type2.lookup_field(4).unwrap().name, "d");
    }

    #[test]
    fn test_snapshot_summary() {
        let (data_file, pos_delete_file, eq_delete_file) = {
            let data_file = DataFile {
                content: super::DataContentType::Data,
                file_path: String::new(),
                file_format: super::DataFileFormat::Parquet,
                partition: StructValue::default(),
                record_count: 10,
                file_size_in_bytes: 100,
                column_sizes: None,
                value_counts: None,
                null_value_counts: None,
                nan_value_counts: None,
                distinct_counts: None,
                lower_bounds: None,
                upper_bounds: None,
                key_metadata: None,
                split_offsets: None,
                equality_ids: None,
                sort_order_id: None,
            };
            let pos_delete_file = {
                let mut data_file = data_file.clone();
                data_file.content = super::DataContentType::PositionDeletes;
                data_file
            };
            let eq_delete_file = {
                let mut data_file = data_file.clone();
                data_file.content = super::DataContentType::EqualityDeletes;
                data_file
            };
            (data_file, pos_delete_file, eq_delete_file)
        };

        // add data file
        let mut builder = SnapshotSummaryBuilder::new();
        builder.add(&data_file);
        let summary_1 = builder.merge(&HashMap::new(), false).unwrap();
        assert_eq!(
            summary_1.get(SnapshotSummaryBuilder::OPERATION).unwrap(),
            "append"
        );
        assert_eq!(
            summary_1
                .get(SnapshotSummaryBuilder::ADDED_DATA_FILES)
                .unwrap(),
            "1"
        );
        assert_eq!(
            summary_1
                .get(SnapshotSummaryBuilder::TOTAL_DATA_FILES)
                .unwrap(),
            "1"
        );
        assert_eq!(
            summary_1
                .get(SnapshotSummaryBuilder::ADDED_RECORDS)
                .unwrap(),
            "10"
        );
        assert_eq!(
            summary_1
                .get(SnapshotSummaryBuilder::TOTAL_RECORDS)
                .unwrap(),
            "10"
        );
        assert_eq!(
            summary_1
                .get(SnapshotSummaryBuilder::ADDED_FILES_SIZE)
                .unwrap(),
            "100"
        );
        assert_eq!(
            summary_1
                .get(SnapshotSummaryBuilder::TOTAL_FILES_SIZE)
                .unwrap(),
            "100"
        );

        // add position delete file
        // add eq delete file
        let mut builder = SnapshotSummaryBuilder::new();
        builder.add(&pos_delete_file);
        builder.add(&eq_delete_file);
        let summary_2 = builder.merge(&summary_1, false).unwrap();
        assert_eq!(
            summary_2.get(SnapshotSummaryBuilder::OPERATION).unwrap(),
            "delete"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::ADDED_DATA_FILES)
                .unwrap(),
            "0"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::TOTAL_DATA_FILES)
                .unwrap(),
            "1"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::ADDED_DELETE_FILES)
                .unwrap(),
            "2"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::ADDED_POSITION_DELETE_FILES)
                .unwrap(),
            "1"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::ADDED_EQUALITY_DELETE_FILES)
                .unwrap(),
            "1"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::TOTAL_DELETE_FILES)
                .unwrap(),
            "2"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::ADDED_RECORDS)
                .unwrap(),
            "0"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::TOTAL_RECORDS)
                .unwrap(),
            "10"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::ADDED_POSITION_DELETES)
                .unwrap(),
            "10"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::TOTAL_POSITION_DELETES)
                .unwrap(),
            "10"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::ADDED_EQUALITY_DELETES)
                .unwrap(),
            "10"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::TOTAL_EQUALITY_DELETES)
                .unwrap(),
            "10"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::ADDED_FILES_SIZE)
                .unwrap(),
            "200"
        );
        assert_eq!(
            summary_2
                .get(SnapshotSummaryBuilder::TOTAL_FILES_SIZE)
                .unwrap(),
            "300"
        );

        // add data file
        // add position delete file
        // add eq delete file
        let mut builder = SnapshotSummaryBuilder::new();
        builder.add(&data_file);
        builder.add(&pos_delete_file);
        builder.add(&eq_delete_file);
        let summary_3 = builder.merge(&summary_2, false).unwrap();
        assert_eq!(
            summary_3.get(SnapshotSummaryBuilder::OPERATION).unwrap(),
            "overwrite"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::ADDED_DATA_FILES)
                .unwrap(),
            "1"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::TOTAL_DATA_FILES)
                .unwrap(),
            "2"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::ADDED_DELETE_FILES)
                .unwrap(),
            "2"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::ADDED_POSITION_DELETE_FILES)
                .unwrap(),
            "1"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::ADDED_EQUALITY_DELETE_FILES)
                .unwrap(),
            "1"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::TOTAL_DELETE_FILES)
                .unwrap(),
            "4"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::ADDED_RECORDS)
                .unwrap(),
            "10"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::TOTAL_RECORDS)
                .unwrap(),
            "20"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::ADDED_POSITION_DELETES)
                .unwrap(),
            "10"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::TOTAL_POSITION_DELETES)
                .unwrap(),
            "20"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::ADDED_EQUALITY_DELETES)
                .unwrap(),
            "10"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::TOTAL_EQUALITY_DELETES)
                .unwrap(),
            "20"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::ADDED_FILES_SIZE)
                .unwrap(),
            "300"
        );
        assert_eq!(
            summary_3
                .get(SnapshotSummaryBuilder::TOTAL_FILES_SIZE)
                .unwrap(),
            "600"
        );
    }
}
