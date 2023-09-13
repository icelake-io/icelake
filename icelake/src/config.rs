//! This module contains table configurations.

use crate::error::Result;
use crate::{Error, ErrorKind};
use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::Arc;

/// Reference to [`TableConfig`].
pub type TableConfigRef = Arc<TableConfig>;

/// Table configuration.
#[derive(PartialEq, Eq, Debug, Default)]
pub struct TableConfig {
    /// Parquet writer configuration.
    pub parquet_writer: ParquetWriterConfig,
    /// Datafile configuration
    pub datafile_writer: DataFileWriterConfig,
}

/// Data file writer configuration.
#[derive(PartialEq, Eq, Debug)]
pub struct DataFileWriterConfig {
    /// data file writer will keep row number of a data file as a multiple of this value.
    pub rows_per_file: usize,
    /// data file writer will keep file size of a data file bigger than this value.
    pub target_file_size_in_bytes: u64,
}

impl Default for DataFileWriterConfig {
    fn default() -> Self {
        Self {
            rows_per_file: 1000,
            target_file_size_in_bytes: 1024 * 1024,
        }
    }
}

/// Parquet writer configuration.
#[derive(PartialEq, Eq, Debug)]
pub struct ParquetWriterConfig {
    /// Enable bloom filter.
    pub enable_bloom_filter: bool,
    /// Set author of parquet file.
    pub created_by: Option<String>,
    /// Set compression level.
    pub compression: Compression,
    /// Set number of rows of each row group.
    pub max_row_group_size: usize,
    /// Set batch size of parquet column.
    pub write_batch_size: usize,
    /// Set data page size.
    pub data_page_size: usize,
}

impl TryFrom<&'_ HashMap<String, String>> for TableConfig {
    type Error = Error;

    fn try_from(value: &'_ HashMap<String, String>) -> Result<Self> {
        let mut config = TableConfig::default();

        value
            .get("iceberg.table.parquet_writer.enable_bloom_filter")
            .map(|v| v.parse::<bool>())
            .transpose()
            .map_err(|e| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    "Can't parse iceberg.parquet_writer.enable_bloom_filter.",
                )
                .set_source(e)
            })?
            .iter()
            .for_each(|v| config.parquet_writer.enable_bloom_filter = *v);

        value
            .get("iceberg.table.parquet_writer.created_by")
            .iter()
            .for_each(|v| config.parquet_writer.created_by = Some(v.as_str().to_string()));

        config.parquet_writer.set_compression(
            value.get("iceberg.table.parquet_writer.compression"),
            value
                .get("iceberg.table.parquet_writer.compression_level")
                .map(|v| v.parse::<u32>())
                .transpose()
                .map_err(|e| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        "Can't parse compression level value.",
                    )
                    .set_source(e)
                })?,
        )?;

        value
            .get("iceberg.table.parquet_writer.max_row_group_size")
            .map(|v| v.parse::<usize>())
            .transpose()
            .map_err(|e| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    "Can't parse iceberg.parquet_writer.max_row_group_size.",
                )
                .set_source(e)
            })?
            .iter()
            .for_each(|v| config.parquet_writer.max_row_group_size = *v);

        value
            .get("iceberg.table.parquet_writer.write_batch_size")
            .map(|v| v.parse::<usize>())
            .transpose()
            .map_err(|e| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    "Can't parse iceberg.parquet_writer.write_batch_size.",
                )
                .set_source(e)
            })?
            .iter()
            .for_each(|v| config.parquet_writer.write_batch_size = *v);

        value
            .get("iceberg.table.parquet_writer.data_page_size")
            .map(|v| v.parse::<usize>())
            .transpose()
            .map_err(|e| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    "Can't parse iceberg.parquet_writer.data_page_size.",
                )
                .set_source(e)
            })?
            .iter()
            .for_each(|v| config.parquet_writer.data_page_size = *v);

        value
            .get("iceberg.table.datafile.rows_per_file")
            .map(|v| v.parse::<usize>())
            .transpose()
            .map_err(|e| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    "Can't parse iceberg.datafile.rows_per_file.",
                )
                .set_source(e)
            })?
            .iter()
            .for_each(|v| config.datafile_writer.rows_per_file = *v);

        value
            .get("iceberg.table.datafile.target_file_size_in_bytes")
            .map(|v| v.parse::<u64>())
            .transpose()
            .map_err(|e| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    "Can't parse iceberg.datafile.target_file_size_in_bytes.",
                )
                .set_source(e)
            })?;

        Ok(config)
    }
}

impl ParquetWriterConfig {
    fn set_compression(
        &mut self,
        compress_str: Option<&String>,
        compression_level: Option<u32>,
    ) -> Result<()> {
        if let Some(compression_str) = compress_str {
            let compression = match compression_str.as_str().to_lowercase().as_str() {
                "snappy" => Compression::SNAPPY,
                "gzip" => {
                    let gzip_level = compression_level
                        .map(GzipLevel::try_new)
                        .transpose()
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::IcebergDataInvalid,
                                "Invalid gzip compression level.",
                            )
                            .set_source(e)
                        })?
                        .unwrap_or(GzipLevel::default());
                    Compression::GZIP(gzip_level)
                }
                "lzo" => Compression::LZO,
                "brotli" => {
                    let level = compression_level
                        .map(BrotliLevel::try_new)
                        .transpose()
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::IcebergDataInvalid,
                                "Invalid brotli compression level.",
                            )
                            .set_source(e)
                        })?
                        .unwrap_or(BrotliLevel::default());
                    Compression::BROTLI(level)
                }
                "lz4" => Compression::LZ4,
                "zstd" => {
                    let level = compression_level
                        .map(|v| v as i32)
                        .map(ZstdLevel::try_new)
                        .transpose()
                        .map_err(|e| {
                            Error::new(
                                ErrorKind::IcebergDataInvalid,
                                "Invalid zstd compression level.",
                            )
                            .set_source(e)
                        })?
                        .unwrap_or(ZstdLevel::default());
                    Compression::ZSTD(level)
                }
                "lz4_raw" => Compression::LZ4_RAW,
                s => {
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("Invalid compression method {s}"),
                    ))
                }
            };

            self.compression = compression;
            Ok(())
        } else {
            Ok(())
        }
    }
}

impl Default for ParquetWriterConfig {
    fn default() -> Self {
        let parquet_writer_properties = WriterProperties::default();
        Self {
            enable_bloom_filter: true,
            created_by: None,
            compression: Compression::SNAPPY,
            max_row_group_size: parquet_writer_properties.max_row_group_size(),
            write_batch_size: parquet_writer_properties.write_batch_size(),
            data_page_size: parquet_writer_properties.data_page_size_limit(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{ParquetWriterConfig, TableConfig};
    use parquet::basic::{Compression, GzipLevel};
    use std::collections::HashMap;

    #[test]
    fn test_parse_table_config_from_hashmap() {
        let expected_config = TableConfig {
            parquet_writer: ParquetWriterConfig {
                enable_bloom_filter: true,
                created_by: Some("labixiaoxin".to_string()),
                compression: Compression::GZIP(GzipLevel::try_new(8).unwrap()),
                max_row_group_size: 1024,
                write_batch_size: 8888,
                data_page_size: 7,
            },
            datafile_writer: Default::default(),
        };

        let config_map = HashMap::from([
            ("iceberg.table.parquet_writer.enable_bloom_filter", "true"),
            ("iceberg.table.parquet_writer.created_by", "labixiaoxin"),
            ("iceberg.table.parquet_writer.compression", "gzip"),
            ("iceberg.table.parquet_writer.compression_level", "8"),
            ("iceberg.table.parquet_writer.max_row_group_size", "1024"),
            ("iceberg.table.parquet_writer.write_batch_size", "8888"),
            ("iceberg.table.parquet_writer.data_page_size", "7"),
        ])
        .iter()
        .map(|e| (e.0.to_string(), e.1.to_string()))
        .collect();

        let parsed_config = TableConfig::try_from(&config_map).unwrap();

        assert_eq!(expected_config, parsed_config);
    }
}
