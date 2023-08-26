use super::TransformFunction;
use crate::{Error, Result};
use arrow::array::{
    Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::compute::binary;
use arrow::datatypes;
use arrow::datatypes::DataType;
use arrow::{
    array::{ArrayRef, Date32Array, Int32Array},
    compute::{month_dyn, year_dyn},
};
use chrono::Datelike;
use std::sync::Arc;

/// 1970-01-01 is base date in iceberg.
/// 719163 is the number of days from 0000-01-01 to 1970-01-01
const BASE_DAY_FROM_CE: i32 = 719163;
const DAY_PER_SECOND: f64 = 0.0000115741;
const HOUR_PER_SECOND: f64 = 1_f64 / 3600.0;

pub struct Year {}

impl TransformFunction for Year {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let array = year_dyn(&input).map_err(|err| {
            Error::new(
                crate::ErrorKind::ArrowError,
                format!("error in transformfunction: {}", err),
            )
        })?;
        Ok(Arc::<Int32Array>::new(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .unary(|v| v - 1970),
        ))
    }
}

pub struct Month {}

impl TransformFunction for Month {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let year_array = year_dyn(&input).map_err(|err| {
            Error::new(
                crate::ErrorKind::ArrowError,
                format!("error in transformfunction: {}", err),
            )
        })?;
        let year_array: Int32Array = year_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .unary(|v| 12 * (v - 1970));
        let month_array = month_dyn(&input).map_err(|err| {
            Error::new(
                crate::ErrorKind::ArrowError,
                format!("error in transformfunction: {}", err),
            )
        })?;
        Ok(Arc::<Int32Array>::new(
            binary(
                month_array.as_any().downcast_ref::<Int32Array>().unwrap(),
                year_array.as_any().downcast_ref::<Int32Array>().unwrap(),
                // Compute month from 1970-01-01, so minus 1 here.
                |a, b| a + b - 1,
            )
            .unwrap(),
        ))
    }
}

pub struct Day {}

impl TransformFunction for Day {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Int32Array = match input.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                datatypes::TimeUnit::Second => input
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 { (v as f64 * DAY_PER_SECOND) as i32 }),
                datatypes::TimeUnit::Millisecond => input
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 { (v as f64 / 1000.0 * DAY_PER_SECOND) as i32 }),
                datatypes::TimeUnit::Microsecond => input
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 { (v as f64 / 1000.0 / 1000.0 * DAY_PER_SECOND) as i32 }),
                datatypes::TimeUnit::Nanosecond => input
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        (v as f64 / 1000.0 / 1000.0 / 1000.0 * DAY_PER_SECOND) as i32
                    }),
            },
            DataType::Date32 => {
                input
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        datatypes::Date32Type::to_naive_date(v).num_days_from_ce()
                            - BASE_DAY_FROM_CE
                    })
            }
            _ => unreachable!(
                "Should not call transform in Day with type {:?}",
                input.data_type()
            ),
        };
        Ok(Arc::new(res))
    }
}

pub struct Hour {}

impl TransformFunction for Hour {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        let res: Int32Array = match input.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                datatypes::TimeUnit::Second => input
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        println!("second: {}", v);
                        (v as f64 * HOUR_PER_SECOND) as i32
                    }),
                datatypes::TimeUnit::Millisecond => input
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        println!("mill: {}", v);
                        (v as f64 * HOUR_PER_SECOND / 1000.0) as i32
                    }),
                datatypes::TimeUnit::Microsecond => input
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        println!("micro: {}", v);
                        (v as f64 * HOUR_PER_SECOND / 1000.0 / 1000.0) as i32
                    }),
                datatypes::TimeUnit::Nanosecond => input
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .unary(|v| -> i32 {
                        println!("nano: {}", v);
                        (v as f64 * HOUR_PER_SECOND / 1000.0 / 1000.0 / 1000.0) as i32
                    }),
            },
            _ => unreachable!(
                "Should not call transform in Day with type {:?}",
                input.data_type()
            ),
        };
        Ok(Arc::new(res))
    }
}
