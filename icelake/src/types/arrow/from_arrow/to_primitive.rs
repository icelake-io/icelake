use crate::Result;
use crate::{types::PrimitiveValue, Error, ErrorKind};
use arrow::datatypes::i256;

/// Help to convert arrow primitive value to iceberg primitive value.
/// We implement this trait in the `ArrowPrimitiveType::Naive` type.
pub trait ToPrimitiveValue {
    /// In arrow, the data of the primitive value is represented by a `Native` type, it distinguishes by `DataType`.
    /// That's why we need to pass a extra `data_type`.
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue>;
}

impl ToPrimitiveValue for i8 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            // TODO: Is that right?
            arrow::datatypes::DataType::Int8 => Ok(PrimitiveValue::Int(self.into())),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert i8 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for i16 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            arrow::datatypes::DataType::Int16 => Ok(PrimitiveValue::Int(self.into())),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert i16 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for i32 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            arrow::datatypes::DataType::Int32 => Ok(PrimitiveValue::Int(self)),
            arrow::datatypes::DataType::Date32 => todo!(),
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth) => {
                todo!()
            }
            arrow::datatypes::DataType::Time32(_) => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert i32 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for i64 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            arrow::datatypes::DataType::Int64 => Ok(PrimitiveValue::Long(self)),
            arrow::datatypes::DataType::Date64 => todo!(),
            arrow::datatypes::DataType::Duration(_) => {
                todo!()
            }
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::DayTime) => {
                todo!()
            }
            arrow::datatypes::DataType::Time64(_) => todo!(),
            arrow::datatypes::DataType::Timestamp(_, None) => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert i64 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for i128 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            arrow::datatypes::DataType::Decimal128(_, _) => todo!(),
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano) => {
                todo!()
            }
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert i128 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for i256 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            arrow::datatypes::DataType::Decimal256(_, _) => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert i256 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for f32 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            arrow::datatypes::DataType::Float32 => Ok(PrimitiveValue::Float(self.into())),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert f32 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for f64 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            arrow::datatypes::DataType::Float64 => Ok(PrimitiveValue::Double(self.into())),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert f64 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for u8 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            arrow::datatypes::DataType::UInt8 => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert u8 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for u16 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            arrow::datatypes::DataType::UInt16 => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert u16 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for u32 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            arrow::datatypes::DataType::UInt32 => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert u32 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for u64 {
    fn to_primitive(self, data_type: arrow::datatypes::DataType) -> Result<PrimitiveValue> {
        match data_type {
            arrow::datatypes::DataType::UInt64 => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert u64 to {:?}", data_type),
            )),
        }
    }
}
