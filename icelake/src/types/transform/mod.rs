use super::Transform;
use crate::Result;
use arrow::array::ArrayRef;
mod identity;

/// TransformFunction is a trait that defines the interface of a transform function.
pub trait TransformFunction {
    /// transform will take an input array and transform it into a new array.
    /// The implementation of this function will need to check and downcast the input to specific
    /// type.
    fn transform(&self, input: ArrayRef) -> Result<ArrayRef>;
}

/// BoxedTransformFunction is a boxed trait object of TransformFunction.
pub type BoxedTransformFunction = Box<dyn TransformFunction>;

/// Create a transform function from a Transform.
pub fn create_transform_function(transform: &Transform) -> Result<BoxedTransformFunction> {
    match transform {
        Transform::Identity => Ok(Box::new(identity::Identity {})),
        _ => Err(crate::error::Error::new(
            crate::ErrorKind::IcebergFeatureUnsupported,
            format!("Transform {:?} is not implemented", transform),
        )),
    }
}
