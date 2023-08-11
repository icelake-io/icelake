//! in_memory module provides the definition of iceberg in-memory data types.

use std::hash::Hasher;
use std::sync::Arc;
use std::{collections::HashMap, str::FromStr};

use bitvec::vec::BitVec;
use chrono::DateTime;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::NaiveTime;
use chrono::Utc;
use opendal::Operator;
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use serde::ser::SerializeMap;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::hash::Hash;
use uuid::Uuid;

use crate::types::parse_manifest_list;
use crate::ErrorKind;
use crate::Result;
use crate::{Error, Table};

pub(crate) const UNASSIGNED_SEQ_NUM: i64 = -1;
const MAIN_BRANCH: &str = "main";
const EMPTY_SNAPSHOT_ID: i64 = -1;

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
#[derive(Debug, PartialEq, Clone, Eq, Hash)]
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
            PrimitiveValue::Decimal(value) => serializer.serialize_str(&value.to_string()),
            PrimitiveValue::Date(value) => serializer.serialize_str(&value.to_string()),
            PrimitiveValue::Time(value) => serializer.serialize_str(&value.to_string()),
            PrimitiveValue::Timestamp(value) => serializer.serialize_str(&value.to_string()),
            PrimitiveValue::Timestampz(value) => serializer.serialize_str(&value.to_string()),
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
#[derive(Default, Debug, PartialEq, Clone, Eq)]
pub struct Struct {
    /// Fields contained in this struct.
    fields: Vec<Field>,
    /// Map field id to index
    id_lookup: HashMap<i32, usize>,
}

impl Struct {
    /// Create a new struct.
    pub fn new(fields: Vec<Field>) -> Self {
        let mut id_lookup = HashMap::with_capacity(fields.len());
        fields.iter().enumerate().for_each(|(index, field)| {
            id_lookup.insert(field.id, index);
        });
        Self { fields, id_lookup }
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
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Lookup the field type according to the field id.
    pub fn lookup_type(&self, field_id: i32) -> Option<Any> {
        self.id_lookup
            .get(&field_id)
            .map(|&idx| self.fields[idx].field_type.clone())
    }
}

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
    fn required(id: i32, name: impl Into<String>, r#type: Any) -> Self {
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

    fn optional(id: i32, name: impl Into<String>, r#type: Any) -> Self {
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

impl StructValue {
    /// Create a iterator to read the field in order of (field_id, field_value, field_name).
    pub fn iter(&self) -> impl Iterator<Item = (i32, Option<&AnyValue>, &str)> {
        self.null_bitmap
            .iter()
            .zip(self.field_values.iter())
            .zip(self.type_info.fields().iter())
            .map(|((null, value), field)| {
                (
                    field.id,
                    if *null { None } else { Some(value) },
                    field.name.as_str(),
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
        for (_, value, key) in self.iter() {
            if let Some(value) = value {
                // NOTE: Here we use `Box::leak` to convert `&str` to `&'static str`. The safe is guaranteed by serializer.
                record.serialize_field(Box::leak(key.to_string().into_boxed_str()), value)?;
            } else {
                // `i32` is just as a placeholder, it will be ignored by serializer.
                record
                    .serialize_field(Box::leak(key.to_string().into_boxed_str()), &None::<i32>)?;
            }
        }
        record.end()
    }
}

impl Hash for StructValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for (id, value, name) in self.iter() {
            id.hash(state);
            value.hash(state);
            name.hash(state);
        }
    }
}

/// A builder to build a struct value. Buidler will guaranteed that the StructValue is valid for the Struct.
pub struct StructValueBuilder {
    fileds: HashMap<i32, Option<AnyValue>>,
    type_info: Arc<Struct>,
}

impl StructValueBuilder {
    /// Create a new builder.
    pub fn new(type_info: Arc<Struct>) -> Self {
        Self {
            fileds: HashMap::with_capacity(type_info.len()),
            type_info,
        }
    }

    /// Add a field to the struct value.
    pub fn add_field(&mut self, field_id: i32, field_value: Option<AnyValue>) -> Result<()> {
        // Check the field id is valid.
        self.type_info.lookup_type(field_id).ok_or(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("Field {} is not found", field_id),
        ))?;
        // TODO: Check the field type is consistent.
        // TODO: Check the duplication of field.

        self.fileds.insert(field_id, field_value);
        Ok(())
    }

    /// Build the struct value.
    pub fn build(mut self) -> Result<StructValue> {
        let mut field_values = Vec::with_capacity(self.fileds.len());
        let mut null_bitmap = BitVec::with_capacity(self.fileds.len());

        for field in self.type_info.fields.iter() {
            let field_value = self.fileds.remove(&field.id).ok_or(Error::new(
                ErrorKind::IcebergDataInvalid,
                format!("Field {} is required", field.name),
            ))?;
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
    pub fields: Vec<Field>,
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
    fn result_type(&self, input_type: &Any) -> Result<Any> {
        match self {
            Transform::Identity => Ok(input_type.clone()),
            _ => Err(Error::new(
                ErrorKind::IcebergFeatureUnsupported,
                format!("Transform {:?} not supported yet!", &self),
            )),
        }
    }
}

impl<'a> ToString for &'a Transform {
    fn to_string(&self) -> String {
        match self {
            Transform::Identity => "identity".to_string(),
            Transform::Year => "year".to_string(),
            Transform::Month => "month".to_string(),
            Transform::Day => "day".to_string(),
            Transform::Hour => "hour".to_string(),
            Transform::Void => "void".to_string(),
            Transform::Bucket(length) => format!("bucket[{}]", length),
            Transform::Truncate(width) => format!("truncate[{}]", width),
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
                .fields
                .iter()
                .find(|f| f.id == partition_field.source_column_id)
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
            fields.push(Field::optional(
                partition_field.partition_field_id,
                partition_field.name.as_str(),
                result_type,
            ));
        }

        Ok(Struct::new(fields))
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

impl ToString for SortDirection {
    fn to_string(&self) -> String {
        match self {
            SortDirection::ASC => "asc".to_string(),
            SortDirection::DESC => "desc".to_string(),
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

impl ToString for NullOrder {
    fn to_string(&self) -> String {
        match self {
            NullOrder::First => "nulls-first".to_string(),
            NullOrder::Last => "nulls-last".to_string(),
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
        Schema {
            schema_id: 1,
            identifier_field_ids: None,
            fields: vec![
                manifest_list::MANIFEST_PATH.clone(),
                manifest_list::MANIFEST_LENGTH.clone(),
                manifest_list::PARTITION_SPEC_ID.clone(),
                manifest_list::CONTENT.clone(),
                manifest_list::SEQUENCE_NUMBER.clone(),
                manifest_list::MIN_SEQUENCE_NUMBER.clone(),
                manifest_list::ADDED_SNAPSHOT_ID.clone(),
                manifest_list::ADDED_FILES_COUNT.clone(),
                manifest_list::EXISTING_FILES_COUNT.clone(),
                manifest_list::DELETED_FILES_COUNT.clone(),
                manifest_list::ADDED_ROWS_COUNT.clone(),
                manifest_list::EXISTING_ROWS_COUNT.clone(),
                manifest_list::DELETED_ROWS_COUNT.clone(),
                manifest_list::PARTITIONS.clone(),
                manifest_list::KEY_METADATA.clone(),
            ],
        }
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
    pub added_data_files_count: i32,
    /// field: 505
    ///
    /// Number of entries in the manifest that have status EXISTING (0),
    /// when null this is assumed to be non-zero
    pub existing_data_files_count: i32,
    /// field: 506
    ///
    /// Number of entries in the manifest that have status DELETED (2),
    /// when null this is assumed to be non-zero
    pub deleted_data_files_count: i32,
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
    pub partitions: Vec<FieldSummary>,
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
            "added_data_files_count",
            Any::Primitive(Primitive::Int),
        )
    });
    pub static EXISTING_FILES_COUNT: Lazy<Field> = Lazy::new(|| {
        Field::required(
            505,
            "existing_data_files_count",
            Any::Primitive(Primitive::Int),
        )
    });
    pub static DELETED_FILES_COUNT: Lazy<Field> = Lazy::new(|| {
        Field::required(
            506,
            "deleted_data_files_count",
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
            13,
            "partitions",
            Any::List(List {
                element_id: 13,
                element_required: false,
                element_type: Box::new(Any::Struct(
                    Struct::new(vec![
                        Field::required(0, "contains_null", Any::Primitive(Primitive::Boolean)),
                        Field::optional(1, "contains_nan", Any::Primitive(Primitive::Boolean)),
                        Field::optional(2, "lower_bound", Any::Primitive(Primitive::Binary)),
                        Field::optional(2, "upper_bound", Any::Primitive(Primitive::Binary)),
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
    ///
    /// FIXME: we should parse this field.
    // pub partition_spec: Option<PartitionSpec>,

    /// ID of the partition spec used to write the manifest as a string
    pub partition_spec_id: i32,
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
        Schema {
            schema_id: 0,
            identifier_field_ids: None,
            fields: vec![
                manifest_file::STATUS.clone(),
                manifest_file::SNAPSHOT_ID.clone(),
                manifest_file::SEQUENCE_NUMBER.clone(),
                manifest_file::FILE_SEQUENCE_NUMBER.clone(),
                Field::required(
                    manifest_file::DATA_FILE_ID,
                    manifest_file::DATA_FILE_NAME,
                    Any::Struct(
                        Struct::new(vec![
                            datafile::CONTENT.clone().with_required(),
                            datafile::FILE_PATH.clone(),
                            datafile::FILE_FORMAT.clone(),
                            DataFile::partition_field(partition_type),
                            datafile::RECORD_COUNT.clone(),
                            datafile::FILE_SIZE.clone(),
                            datafile::COLUMN_SIZES.clone(),
                            datafile::VALUE_COUNTS.clone(),
                            datafile::NULL_VALUE_COUNTS.clone(),
                            datafile::NAN_VALUE_COUNTS.clone(),
                            datafile::LOWER_BOUNDS.clone(),
                            datafile::UPPER_BOUNDS.clone(),
                            datafile::KEY_METADATA.clone(),
                            datafile::SPLIT_OFFSETS.clone(),
                            datafile::EQUALITY_IDS.clone(),
                            datafile::SORT_ORDER_ID.clone(),
                        ])
                        .into(),
                    ),
                ),
            ],
        }
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

impl ToString for ManifestContentType {
    fn to_string(&self) -> String {
        match self {
            ManifestContentType::Data => "data".to_string(),
            ManifestContentType::Deletes => "deletes".to_string(),
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

/// Data file carries data file path, partition tuple, metrics, …
#[derive(Debug, PartialEq, Clone)]
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
    pub column_sizes: Option<HashMap<i32, i64>>,
    /// field id: 109
    /// key field id: 119
    /// value field id: 120
    ///
    /// Map from column id to number of values in the column (including null
    /// and NaN values)
    pub value_counts: Option<HashMap<i32, i64>>,
    /// field id: 110
    /// key field id: 121
    /// value field id: 122
    ///
    /// Map from column id to number of null values in the column
    pub null_value_counts: Option<HashMap<i32, i64>>,
    /// field id: 137
    /// key field id: 138
    /// value field id: 139
    ///
    /// Map from column id to number of NaN values in the column
    pub nan_value_counts: Option<HashMap<i32, i64>>,
    /// field id: 111
    /// key field id: 123
    /// value field id: 124
    ///
    /// Map from column id to number of distinct values in the column;
    /// distinct counts must be derived using values in the file by counting
    /// or using sketches, but not using methods like merging existing
    /// distinct counts
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
    pub split_offsets: Vec<i64>,
    /// field id: 135
    /// element field id: 136
    ///
    /// Field ids used to determine row equality in equality delete files.
    /// Required when content is EqualityDeletes and should be null
    /// otherwise. Fields with ids listed in this column must be present
    /// in the delete file
    pub equality_ids: Vec<i32>,
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
    pub sort_order_id: Option<i32>,
}

// impl DataFile {
//     /// Set the partition for this data file.
//     pub fn set_partition(&mut self, partition: StructValue) {
//         self.partition = partition;
//     }
// }

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
    pub static BLOCK_SIZE: Lazy<Field> =
        Lazy::new(|| Field::required(105, "block_size_in_bytes", Any::Primitive(Primitive::Long)));
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
    pub static SPEC_ID: Lazy<Field> = Lazy::new(|| {
        Field::optional(141, "spec_id", Any::Primitive(Primitive::Int))
            .with_comment("Partition spec ID")
    });
}

impl DataFile {
    pub(crate) fn partition_field(partition_type: Struct) -> Field {
        Field::required(102, "partition", Any::Struct(partition_type.into()))
            .with_comment("Partition data tuple, schema based on the partition spec")
    }

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
            split_offsets: vec![],
            equality_ids: vec![],
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
    PostionDeletes = 1,
    /// value: 2
    EqualityDeletes = 2,
}

impl TryFrom<u8> for DataContentType {
    type Error = Error;

    fn try_from(v: u8) -> Result<DataContentType> {
        match v {
            0 => Ok(DataContentType::Data),
            1 => Ok(DataContentType::PostionDeletes),
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

impl ToString for DataFileFormat {
    fn to_string(&self) -> String {
        match self {
            DataFileFormat::Avro => "avro",
            DataFileFormat::Orc => "orc",
            DataFileFormat::Parquet => "parquet",
        }
        .to_string()
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
                .await?,
        )
    }

    pub(crate) fn log(&self) -> SnapshotLog {
        SnapshotLog {
            timestamp_ms: self.timestamp_ms,
            snapshot_id: self.snapshot_id,
        }
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
    /// Tag is used to reference to a specfic tag.
    Tag,
    /// Branch is used to reference to a specfic branch, like `master`.
    Branch,
}

impl ToString for SnapshotReferenceType {
    fn to_string(&self) -> String {
        match self {
            SnapshotReferenceType::Tag => "tag".to_string(),
            SnapshotReferenceType::Branch => "branch".to_string(),
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
        self.partition_specs
            .iter()
            .find(|p| p.spec_id == self.default_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("Partition spec id {} not found!", self.default_spec_id),
                )
            })
    }

    /// Current schema.
    pub fn current_schema(&self) -> Result<&Schema> {
        self.schemas
            .iter()
            .find(|s| s.schema_id == self.current_schema_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("Schema id {} not found!", self.current_schema_id),
                )
            })
    }

    /// Current schema.
    pub fn current_snapshot(&self) -> Result<Option<&Snapshot>> {
        if let (Some(snapshots), Some(snapshot_id)) = (&self.snapshots, self.current_snapshot_id) {
            if snapshot_id == EMPTY_SNAPSHOT_ID {
                return Ok(None);
            }
            Ok(Some(
                snapshots
                    .iter()
                    .find(|s| s.snapshot_id == snapshot_id)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::IcebergDataInvalid,
                            format!("Snapshot id {snapshot_id} not found!"),
                        )
                    })?,
            ))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn append_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        self.last_updated_ms = snapshot.timestamp_ms;
        self.last_sequence_number = snapshot.sequence_number;
        self.current_snapshot_id = Some(snapshot.snapshot_id);

        self.refs
            .entry(MAIN_BRANCH.to_string())
            .and_modify(|s| {
                s.snapshot_id = snapshot.snapshot_id;
                s.typ = SnapshotReferenceType::Branch;
            })
            .or_insert_with(|| {
                SnapshotReference::new(snapshot.snapshot_id, SnapshotReferenceType::Branch)
            });

        if let Some(snapshots) = &mut self.snapshots {
            self.snapshot_log
                .as_mut()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        "Snapshot logs is empty while snapshots is not!",
                    )
                })?
                .push(snapshot.log());
            snapshots.push(snapshot);
        } else {
            if self.snapshot_log.is_some() {
                return Err(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    "Snapshot logs is empty while snapshots is not!",
                ));
            }

            self.snapshot_log = Some(vec![snapshot.log()]);
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

impl ToString for TableFormatVersion {
    fn to_string(&self) -> String {
        match self {
            TableFormatVersion::V1 => "1".to_string(),
            TableFormatVersion::V2 => "2".to_string(),
        }
    }
}

#[cfg(test)]
mod test {
    use apache_avro::{schema, types::Value};

    use crate::types::{Field, PrimitiveValue, Struct, StructValueBuilder};

    use super::AnyValue;

    #[test]
    fn test_struct_to_avro() {
        let value = {
            let struct_value = {
                let struct_type = Struct::new(vec![
                    Field::optional(
                        1,
                        "a",
                        crate::types::Any::Primitive(crate::types::Primitive::Int),
                    ),
                    Field::required(
                        2,
                        "b",
                        crate::types::Any::Primitive(crate::types::Primitive::String),
                    ),
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

        println!("{:#?}", value);
        println!("{:#?}", expect_value);
        assert_eq!(value, expect_value);
    }
}
