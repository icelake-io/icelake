//! File appender.
#[cfg(feature = "prometheus")]
pub mod prometheus;

mod rolling_writer;
pub use self::rolling_writer::*;
