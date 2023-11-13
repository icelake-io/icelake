//! io module provides the ability to read and write data from various
//! sources.
#![warn(dead_code)]

pub mod append_only_writer;
mod appender;
pub mod file_writer;
pub mod location_generator;
pub mod parquet;
pub mod writer_builder;
pub use appender::*;
mod scan;
pub use scan::*;
