use crate::types::TransformFunction;
use crate::Result;
use arrow_array::ArrayRef;
pub struct Identity {}

impl TransformFunction for Identity {
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef> {
        Ok(input)
    }
}
