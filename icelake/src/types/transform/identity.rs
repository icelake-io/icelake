use crate::types::TransformFunction;

pub struct Identity {}

impl TransformFunction for Identity {
    fn transform(&self, input: arrow_array::ArrayRef) -> arrow_array::ArrayRef {
        input
    }
}
