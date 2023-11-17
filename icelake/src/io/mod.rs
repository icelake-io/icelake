//! io module provides the ability to read and write data from various
//! sources.
#![warn(dead_code)]

mod appender;
pub use appender::*;
pub mod file_writer;
pub use file_writer::*;
pub mod functional_writer;
pub mod location_generator;
pub use functional_writer::*;

pub mod parquet;
mod scan;
pub mod writer_builder;
pub use scan::*;
