use crate::types::TransformFunction;
use crate::Result;
use arrow_array::{new_null_array, ArrayRef};
use arrow_schema::DataType;

pub struct Void {}

impl TransformFunction for Void {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        Ok(new_null_array(&DataType::Int32, input.len()))
    }
}
