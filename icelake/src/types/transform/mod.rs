use super::Transform;

mod identity;

/// TransformFunction is a trait that defines the interface of a transform function.
pub trait TransformFunction {
    /// transform will take an input array and transform it into a new array.
    /// The implementation of this function will need to check and downcast the input to specific
    /// type.
    fn transform(&self, input: arrow_array::ArrayRef) -> arrow_array::ArrayRef;
}

/// BoxedTransformFunction is a boxed trait object of TransformFunction.
pub type BoxedTransformFunction = Box<dyn TransformFunction>;

/// Create a transform function from a Transform.
pub fn create_transform_function(transform: Transform) -> BoxedTransformFunction {
    match transform {
        Transform::Identity => Box::new(identity::Identity {}),
        _ => todo!(),
    }
}
