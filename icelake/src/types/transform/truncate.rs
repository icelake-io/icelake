use std::sync::Arc;

use arrow_array::{
    ArrayRef, Decimal128Array, Int32Array, Int64Array, LargeStringArray, StringArray,
};
use arrow_schema::DataType;

use crate::Error;

use super::TransformFunction;

pub struct Truncate {
    width: i32,
}

impl Truncate {
    pub fn new(width: i32) -> Self {
        Self { width }
    }
}

impl TransformFunction for Truncate {
    fn transform(&self, input: ArrayRef) -> crate::Result<ArrayRef> {
        match input.data_type() {
            DataType::Int32 => {
                let width = self.width;
                let res: Int32Array = input
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .unary(|v| v - (((v % width) + width) % width));
                Ok(Arc::new(res))
            }
            DataType::Int64 => {
                let width = self.width as i64;
                let res: Int64Array = input
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .unary(|v| v - (((v % width) + width) % width));
                Ok(Arc::new(res))
            }
            DataType::Decimal128(precision, scale) => {
                let width = self.width as i128;
                let res: Decimal128Array = input
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .unary(|v| v - (((v % width) + width) % width))
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(|err| Error::new(crate::ErrorKind::ArrowError, format!("{}", err)))?;
                Ok(Arc::new(res))
            }
            DataType::Utf8 => {
                let len = self.width as usize;
                let res: StringArray = StringArray::from_iter(
                    input
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .map(|v| v.map(|v| &v[..len])),
                );
                Ok(Arc::new(res))
            }
            DataType::LargeUtf8 => {
                let len = self.width as usize;
                let res: LargeStringArray = LargeStringArray::from_iter(
                    input
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .unwrap()
                        .iter()
                        .map(|v| v.map(|v| &v[..len])),
                );
                Ok(Arc::new(res))
            }
            _ => unreachable!("Truncate transform only supports (int,long,decimal,string) types"),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{
        builder::PrimitiveBuilder, types::Decimal128Type, Decimal128Array, Int32Array, Int64Array,
    };

    use crate::types::TransformFunction;

    // Test case ref from: https://iceberg.apache.org/spec/#truncate-transform-details
    #[test]
    fn test_truncate() {
        // test truncate int
        let input = Arc::new(Int32Array::from(vec![1, -1]));
        let res = super::Truncate::new(10).transform(input).unwrap();
        assert_eq!(
            res.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            0
        );
        assert_eq!(
            res.as_any().downcast_ref::<Int32Array>().unwrap().value(1),
            -10
        );

        // test truncate long
        let input = Arc::new(Int64Array::from(vec![1, -1]));
        let res = super::Truncate::new(10).transform(input).unwrap();
        assert_eq!(
            res.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            0
        );
        assert_eq!(
            res.as_any().downcast_ref::<Int64Array>().unwrap().value(1),
            -10
        );

        // test decimal
        let mut buidler = PrimitiveBuilder::<Decimal128Type>::new()
            .with_precision_and_scale(20, 2)
            .unwrap();
        buidler.append_value(1065);
        let input = Arc::new(buidler.finish());
        let res = super::Truncate::new(50).transform(input).unwrap();
        assert_eq!(
            res.as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            1050
        );

        // test string
        let input = Arc::new(arrow_array::StringArray::from(vec!["iceberg"]));
        let res = super::Truncate::new(3).transform(input).unwrap();
        assert_eq!(
            res.as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap()
                .value(0),
            "ice"
        );

        // test large string
        let input = Arc::new(arrow_array::LargeStringArray::from(vec!["iceberg"]));
        let res = super::Truncate::new(3).transform(input).unwrap();
        assert_eq!(
            res.as_any()
                .downcast_ref::<arrow_array::LargeStringArray>()
                .unwrap()
                .value(0),
            "ice"
        );
    }
}
