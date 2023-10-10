use std::{ops::Range, sync::Arc};

use crate::{
    types::{DataFile, StructValue},
    Error, ErrorKind, Result, Table,
};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, Schema as ArrowSchema};
use derive_builder::Builder;
use futures::{future::BoxFuture, stream::BoxStream, StreamExt};
use opendal::Operator;
use parquet::{
    arrow::{
        arrow_to_parquet_schema, async_reader::AsyncFileReader, ParquetRecordBatchStreamBuilder,
        ProjectionMask,
    },
    errors::ParquetError,
    file::{
        footer::{decode_footer, decode_metadata},
        metadata::ParquetMetaData,
        FOOTER_SIZE,
    },
};

pub struct FileOffset {
    path: String,
    row: u64,
}
#[derive(Builder)]
#[builder(pattern = "owned")]
#[builder(setter(prefix = "with"))]
pub struct TableScan {
    // Table operator
    op: Operator,
    snapshot_id: i64,
    #[builder(default)]
    start_from: Option<FileOffset>,
    #[builder(default)]
    column_names: Vec<String>,
    #[builder(default)]
    partition_value: Option<StructValue>,

    // Configurations
    #[builder(default = "1024")]
    batch_size: usize,
}

pub struct FileScan {
    op: Operator,
    data_file: DataFile,
    table_location: String,
    projection_mask: ProjectionMask,
    offset: Option<usize>,
    batch_size: usize,
}

pub type FileScanStream = BoxStream<'static, Result<FileScan>>;
pub type RecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

impl TableScan {
    pub async fn scan(&self, table: &Table) -> Result<FileScanStream> {
        let snapshot = table
            .current_table_metadata()
            .snapshot(self.snapshot_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Snapshot {} not found!", self.snapshot_id),
                )
            })?;

        let schema = snapshot
            .schema_id
            .and_then(|id| table.current_table_metadata().schema(id as i32))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Schema id not found for snapshot {}!", self.snapshot_id),
                )
            })?;

        let projection_mask = if !self.column_names.is_empty() {
            let arrow_schema = ArrowSchema::try_from(schema.clone())?;
            let column_idx = self
                .column_names
                .iter()
                .map(|c_name| arrow_schema.index_of(c_name))
                .collect::<std::result::Result<Vec<_>, ArrowError>>()
                .map_err(|e| Error::new(ErrorKind::ArrowError, format!("{}", e)))?;
            let parquet_schema = arrow_to_parquet_schema(&arrow_schema)?;
            ProjectionMask::roots(&parquet_schema, column_idx)
        } else {
            ProjectionMask::all()
        };

        let data_files = table.data_files_of_snapshot(snapshot).await?;

        let (start_file_idx, start_row) = match &self.start_from {
            Some(offset) => (
                data_files
                    .iter()
                    .position(|data_file| data_file.file_path == offset.path)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!(
                                "Data file path {} not found in table {}",
                                offset.path,
                                table.current_table_metadata().location,
                            ),
                        )
                    })?,
                offset.row,
            ),
            None => (0, 0),
        };

        let mut streams = Vec::with_capacity(data_files.len());

        let data_files = data_files.into_iter().skip(start_file_idx).filter(|f| {
            self.partition_value.is_none() || f.partition == *self.partition_value.as_ref().unwrap()
        });

        for (idx, data_file) in data_files.into_iter().skip(start_file_idx).enumerate() {
            let offset = if idx == 0 {
                Some(start_row as usize)
            } else {
                None
            };
            streams.push(Ok(FileScan {
                op: self.op.clone(),
                data_file,
                table_location: table.current_table_metadata().location.clone(),
                projection_mask: projection_mask.clone(),
                offset,
                batch_size: self.batch_size,
            }));
        }

        Ok(Box::pin(futures::stream::iter(streams)))
    }
}

impl FileScan {
    pub async fn scan(self) -> Result<RecordBatchStream> {
        let file_reader = ParquetFileReader {
            op: self.op.clone(),
            path: self
                .data_file
                .file_path
                .strip_prefix(&self.table_location)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!(
                            "Data file path {} is not a prefix of table location {}",
                            self.table_location, self.data_file.file_path,
                        ),
                    )
                })?
                .to_string(),
        };

        let mut stream = ParquetRecordBatchStreamBuilder::new(file_reader)
            .await?
            .with_batch_size(self.batch_size)
            .with_projection(self.projection_mask)
            .build()?
            .map(|res: std::result::Result<RecordBatch, ParquetError>| res.map_err(|e| e.into()));

        if let Some(mut offset) = self.offset {
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                if batch.num_rows() > offset {
                    let stream = futures::stream::iter(vec![Ok(
                        batch.slice(offset, batch.num_rows() - offset)
                    )])
                    .chain(stream);
                    return Ok(Box::pin(stream));
                } else {
                    offset -= batch.num_rows();
                }
            }

            Ok(Box::pin(futures::stream::empty()))
        } else {
            Ok(Box::pin(stream))
        }
    }

    pub fn path(&self) -> &str {
        &self.data_file.file_path
    }
}

struct ParquetFileReader {
    op: Operator,
    // Relative file path of operator root, it must be a parquet file.
    path: String,
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        Box::pin(async move {
            self.op
                .read_with(&self.path)
                .range(range.start as u64..range.end as u64)
                .await
                .map(|data| data.into())
                .map_err(|e| ParquetError::General(format!("{}", e)))
        })
    }

    /// Get the metadata of the parquet file.
    ///
    /// Inspired by https://docs.rs/parquet/latest/parquet/file/footer/fn.parse_metadata.html
    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async {
            let file_size = self
                .op
                .stat(&self.path)
                .await
                .map_err(|e| ParquetError::General(format!("{}", e)))?
                .content_length();

            if file_size < (FOOTER_SIZE as u64) {
                return Err(ParquetError::General(
                    "Invalid Parquet file. Size is smaller than footer".to_string(),
                ));
            }

            let mut footer: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
            {
                let footer_buffer = self
                    .op
                    .read_with(&self.path)
                    .range((file_size - (FOOTER_SIZE as u64))..file_size)
                    .await
                    .map_err(|e| ParquetError::General(format!("{}", e)))?;

                assert_eq!(footer_buffer.len(), FOOTER_SIZE);
                footer.copy_from_slice(&footer_buffer);
            }

            let metadata_len = decode_footer(&footer)?;
            let footer_metadata_len = FOOTER_SIZE + metadata_len;

            if footer_metadata_len > file_size as usize {
                return Err(ParquetError::General(format!(
                    "Invalid Parquet file. Reported metadata length of {} + {} byte footer, but file is only {} bytes",
                    metadata_len,
                    FOOTER_SIZE,
                    file_size
                )));
            }

            let start = file_size - footer_metadata_len as u64;
            let metadata_bytes = self
                .op
                .read_with(&self.path)
                .range(start..(start + metadata_len as u64))
                .await
                .map_err(|e| ParquetError::General(format!("{}", e)))?;
            Ok(Arc::new(decode_metadata(&metadata_bytes)?))
        })
    }
}
