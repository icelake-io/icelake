/// All data types are either primitives or nested types, which are maps, lists, or structs.
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
pub struct Struct {
    pub fields: Vec<Field>,
}

/// A Field is the field of a struct.
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
