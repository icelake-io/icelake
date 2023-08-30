use crate::Result;
use crate::{types::PrimitiveValue, Error, ErrorKind};
use arrow_buffer::i256;
use arrow_schema::DataType;
use arrow_schema::{IntervalUnit, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

/// Help to convert arrow primitive value to iceberg primitive value.
/// We implement this trait in the `ArrowPrimitiveType::Naive` type.
pub trait ToPrimitiveValue {
    /// In arrow, the data of the primitive value is represented by a `Native` type, it distinguishes by `DataType`.
    /// That's why we need to pass a extra `data_type`.
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue>;
}

impl ToPrimitiveValue for i8 {
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            // TODO: Is that right?
            DataType::Int8 => Ok(PrimitiveValue::Int(self.into())),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert i8 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for i16 {
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            DataType::Int16 => Ok(PrimitiveValue::Int(self.into())),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert i16 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for i32 {
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            DataType::Int32 => Ok(PrimitiveValue::Int(self)),
            DataType::Date32 => Ok(PrimitiveValue::Date(
                NaiveDate::from_num_days_from_ce_opt(self).ok_or_else(|| {
                    Error::new(
                        ErrorKind::DataTypeUnsupported,
                        format!("Cannot convert i32 to {:?}: day out of range ", data_type),
                    )
                })?,
            )),
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => Ok(PrimitiveValue::Time(
                    NaiveTime::from_hms_milli_opt(0, 0, self as u32, 0).ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataTypeUnsupported,
                            format!(
                                "Cannot convert i32 to {:?}: second out of range ",
                                data_type
                            ),
                        )
                    })?,
                )),
                TimeUnit::Millisecond => Ok(PrimitiveValue::Time(
                    NaiveTime::from_hms_milli_opt(0, 0, 0, self as u32).ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataTypeUnsupported,
                            format!(
                                "Cannot convert i32 to {:?}: millisecond out of range ",
                                data_type
                            ),
                        )
                    })?,
                )),
                TimeUnit::Microsecond => Ok(PrimitiveValue::Time(
                    NaiveTime::from_hms_micro_opt(0, 0, 0, self as u32).ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataTypeUnsupported,
                            format!(
                                "Cannot convert i32 to {:?}: microsecond out of range ",
                                data_type
                            ),
                        )
                    })?,
                )),
                TimeUnit::Nanosecond => Ok(PrimitiveValue::Time(
                    NaiveTime::from_hms_nano_opt(0, 0, 0, self as u32).ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataTypeUnsupported,
                            format!(
                                "Cannot convert i32 to {:?}: nanosecond out of range ",
                                data_type
                            ),
                        )
                    })?,
                )),
            },
            DataType::Interval(IntervalUnit::YearMonth) => {
                todo!()
            }
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert i32 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for i64 {
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            DataType::Int64 => Ok(PrimitiveValue::Long(self)),
            DataType::Timestamp(unit, with_tz) => {
                let dt = match unit {
                    TimeUnit::Second => {
                        NaiveDateTime::from_timestamp_opt(self, 0).ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataTypeUnsupported,
                                format!(
                                    "Cannot convert i64 to {:?}: second out of range ",
                                    data_type
                                ),
                            )
                        })?
                    }
                    TimeUnit::Millisecond => NaiveDateTime::from_timestamp_millis(self)
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataTypeUnsupported,
                                format!(
                                    "Cannot convert i64 to {:?}: millisecond out of range ",
                                    data_type
                                ),
                            )
                        })?,
                    TimeUnit::Microsecond => NaiveDateTime::from_timestamp_micros(self)
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataTypeUnsupported,
                                format!(
                                    "Cannot convert i64 to {:?}: microsecond out of range ",
                                    data_type
                                ),
                            )
                        })?,
                    TimeUnit::Nanosecond => NaiveDateTime::from_timestamp_opt(
                        0,
                        self.try_into().map_err(|_| {
                            Error::new(
                                ErrorKind::DataTypeUnsupported,
                                format!(
                                    "Cannot convert i64 to {:?}: Nanosecond should not out of i32",
                                    data_type
                                ),
                            )
                        })?,
                    )
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::DataTypeUnsupported,
                            format!(
                                "Cannot convert i64 to {:?}: nanosecond out of range ",
                                data_type
                            ),
                        )
                    })?,
                };
                if with_tz.is_some() {
                    Ok(PrimitiveValue::Timestamp(dt))
                } else {
                    Ok(PrimitiveValue::Timestampz(Utc.from_utc_datetime(&dt)))
                }
            }
            DataType::Date64 => todo!(),
            DataType::Duration(_) => {
                todo!()
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                todo!()
            }
            DataType::Time64(_) => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert i64 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for i128 {
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            DataType::Decimal128(_, _) => todo!(),
            DataType::Interval(IntervalUnit::MonthDayNano) => {
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
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            DataType::Decimal256(_, _) => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert i256 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for f32 {
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            DataType::Float32 => Ok(PrimitiveValue::Float(self.into())),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert f32 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for f64 {
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            DataType::Float64 => Ok(PrimitiveValue::Double(self.into())),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert f64 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for u8 {
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            DataType::UInt8 => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert u8 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for u16 {
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            DataType::UInt16 => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert u16 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for u32 {
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            DataType::UInt32 => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert u32 to {:?}", data_type),
            )),
        }
    }
}

impl ToPrimitiveValue for u64 {
    fn to_primitive(self, data_type: &DataType) -> Result<PrimitiveValue> {
        match data_type {
            DataType::UInt64 => todo!(),
            _ => Err(Error::new(
                ErrorKind::DataTypeUnsupported,
                format!("Cannot convert u64 to {:?}", data_type),
            )),
        }
    }
}
