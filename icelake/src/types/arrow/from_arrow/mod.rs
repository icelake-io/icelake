/// `to_array` module provide the trait and method to convert arrow array into anyvalue array.
pub mod to_array;
pub use to_array::*;

pub(crate) mod to_primitive;

/// `types` module provide the trait and method to convert arrow type into `Any`.
pub mod types;
