//! Schema and data types of iceberg.

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use uuid::Uuid;

pub mod avro;

/// All data types are either primitives or nested types, which are maps, lists, or structs.
#[derive(Debug, PartialEq, Clone)]
pub enum Any {
    /// A Primitive type
    Primitive(Primitive),
    /// A Struct type
    Struct(Struct),
    /// A List type.
    List(List),
    /// A Map type
    Map(Map),
}

/// All data values are either primitives or nested values, which are maps, lists, or structs.
#[derive(Debug, PartialEq, Clone)]
pub enum AnyValue {
    /// A Primitive type
    Primitive(PrimitiveValue),
    /// A Struct type is a tuple of typed values.
    ///
    /// struct value carries the value of a struct type, could be used as
    /// default value.
    ///
    /// struct value stores as a map from field id to field value.
    Struct(HashMap<i32, AnyValue>),
    /// A list type with a list of typed values.
    List(Vec<AnyValue>),
    /// A map is a collection of key-value pairs with a key type and a value type.
    ///
    /// map value carries the value of a map type, could be used as
    /// default value.
    Map {
        /// All keys in this map.
        keys: Vec<AnyValue>,
        /// All values in this map.
        values: Vec<AnyValue>,
    },
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

/// Primitive Values within a schema.
///
/// Used to represent the value of a primitive type, like as default value.
#[derive(Debug, PartialEq, Clone)]
pub enum PrimitiveValue {
    /// True or False
    Boolean(bool),
    /// 32-bit signed integer, Can promote to long
    Int(i32),
    /// 64-bit signed integer
    Long(i64),
    /// 32-bit IEEE 753 floating bit, Can promote to double
    Float(f32),
    /// 64-bit IEEE 753 floating bit.
    Double(f64),
    /// Fixed point decimal
    Decimal(Decimal),
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

/// A struct is a tuple of typed values.
///
/// - Each field in the tuple is named and has an integer id that is unique in the table schema.
/// - Each field can be either optional or required, meaning that values can (or cannot) be null.
/// - Fields may be any type.
/// - Fields may have an optional comment or doc string.
/// - Fields can have default values.
#[derive(Debug, PartialEq, Clone)]
pub struct Struct {
    /// Fields contained in this struct.
    pub fields: Vec<Field>,
}

/// A Field is the field of a struct.
#[derive(Debug, PartialEq, Clone)]
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

/// A list is a collection of values with some element type.
///
/// - The element field has an integer id that is unique in the table schema.
/// - Elements can be either optional or required.
/// - Element types may be any type.
#[derive(Debug, PartialEq, Clone)]
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
#[derive(Debug, PartialEq, Clone)]
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
    pub fields: Vec<Field>,
}
