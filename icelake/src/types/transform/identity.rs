use crate::types::TransformFunction;
use arrow::array::ArrayRef;
pub struct Identity {}

impl TransformFunction for Identity {
    fn transform(&self, input: ArrayRef) -> ArrayRef {
        input
    }
}
