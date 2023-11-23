//! parquet module provides the ability to read and write parquet data.

mod stream;
pub use stream::ParquetStream;
pub use stream::ParquetStreamBuilder;
