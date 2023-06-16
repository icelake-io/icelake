/// All data types are either primitives or nested types, which are maps, lists, or structs.
#[derive(Debug, PartialEq, Eq)]
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

/// Primitive Types within a schema.
#[derive(Debug, PartialEq, Eq)]
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

/// A struct is a tuple of typed values.
///
/// - Each field in the tuple is named and has an integer id that is unique in the table schema.
/// - Each field can be either optional or required, meaning that values can (or cannot) be null.
/// - Fields may be any type.
/// - Fields may have an optional comment or doc string.
/// - Fields can have default values.
#[derive(Debug, PartialEq, Eq)]
pub struct Struct {
    pub fields: Vec<Field>,
}

/// A Field is the field of a struct.
#[derive(Debug, PartialEq, Eq)]
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
}

/// A list is a collection of values with some element type.
///
/// - The element field has an integer id that is unique in the table schema.
/// - Elements can be either optional or required.
/// - Element types may be any type.
#[derive(Debug, PartialEq, Eq)]
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
#[derive(Debug, PartialEq, Eq)]
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
#[derive(Debug)]
pub struct SchemaV2 {
    /// The unique id for this schema.
    pub id: i32,
    /// A schema can optionally track the set of primitive fields that
    /// identify rows in a table, using the property identifier-field-ids
    pub identifier_field_ids: Option<Vec<i32>>,
    /// types contained in this schema.
    pub types: Struct,
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

/// Data files are stored in manifests with a tuple of partition values
/// that are used in scans to filter out files that cannot contain records
///  that match the scan’s filter predicate.
///
/// Partition values for a data file must be the same for all records stored
/// in the data file. (Manifests store data files from any partition, as long
/// as the partition spec is the same for the data files.)
pub struct Partition {
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
pub struct SortOrder {
    /// The sort order id of this SortOrder
    pub id: i32,
    /// The order of the sort fields within the list defines the order in
    /// which the sort is applied to the data
    pub fields: Vec<SortField>,
}

/// Field of the specified sort order.
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
pub enum SortDirection {
    ASC,
    DESC,
}

/// A null order that describes the order of null values when sorted.
/// Can only be either nulls-first or nulls-last
pub enum NullOrder {
    NullsFirst,
    NullsLast,
}
