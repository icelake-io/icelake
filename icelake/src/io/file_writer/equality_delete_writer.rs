//! This module provide `EqualityDeleteWriter`.
use std::sync::Arc;

use crate::{
    io::{RecordBatchWriter, RecordBatchWriterBuilder},
    types::{DataFileBuilder, COLUMN_ID_META_KEY},
    Error, ErrorKind, Result,
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

/// EqualityDeleteWriter is a writer that writes to a file in the equality delete format.
pub struct EqualityDeleteWriter<F: RecordBatchWriter> {
    inner_writer: F,
    equality_ids: Vec<usize>,
    col_id_idx: Vec<usize>,
}

/// Create a new `EqualityDeleteWriter`.
pub async fn new_eq_delete_writer<B: RecordBatchWriterBuilder>(
    arrow_schema: SchemaRef,
    equality_ids: Vec<usize>,
    writer_builder: B,
) -> Result<EqualityDeleteWriter<B::R>> {
    let mut col_id_idx = vec![];
    for &id in equality_ids.iter() {
        arrow_schema.fields().iter().enumerate().any(|(idx, f)| {
            if f.metadata()
                .get(COLUMN_ID_META_KEY)
                .unwrap()
                .parse::<usize>()
                .unwrap()
                == id
            {
                col_id_idx.push(idx);
                true
            } else {
                false
            }
        });
    }
    let delete_schema = Arc::new(arrow_schema.project(&col_id_idx).map_err(|err| {
        Error::new(
            ErrorKind::ArrowError,
            format!(
                "Failed to project schema with equality ids: {:?}, error: {}",
                equality_ids, err
            ),
        )
    })?);

    Ok(EqualityDeleteWriter {
        inner_writer: writer_builder.build(&delete_schema).await?,
        equality_ids,
        col_id_idx,
    })
}

impl<F: RecordBatchWriter> EqualityDeleteWriter<F> {
    /// Write a record batch.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_writer
            .write(batch.project(&self.col_id_idx).map_err(|err| {
                Error::new(
                    ErrorKind::ArrowError,
                    format!(
                        "Failed to project record batch with equality ids: {:?}, error: {}",
                        self.equality_ids, err
                    ),
                )
            })?)
            .await?;
        Ok(())
    }

    /// Complte the write and return the list of `DataFileBuilder` as result.
    pub async fn close(mut self) -> Result<Vec<DataFileBuilder>> {
        Ok(self
            .inner_writer
            .close()
            .await?
            .into_iter()
            .map(|builder| {
                builder
                    .with_content(crate::types::DataContentType::EqualityDeletes)
                    .with_equality_ids(self.equality_ids.iter().map(|&i| i as i32).collect())
            })
            .collect())
    }
}
