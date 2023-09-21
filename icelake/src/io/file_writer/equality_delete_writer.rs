//! This module provide `EqualityDeleteWriter`.
use crate::{
    types::{DataFileBuilder, COLUMN_ID_META_KEY},
    Error, ErrorKind, Result,
};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use opendal::Operator;

use crate::{config::TableConfigRef, io::location_generator::FileLocationGenerator};

use super::rolling_writer::RollingWriter;

/// EqualityDeleteWriter is a writer that writes to a file in the equality delete format.
pub struct EqualityDeleteWriter {
    inner_writer: RollingWriter,
    table_location: String,
    equality_ids: Vec<usize>,
    col_id_idx: Vec<usize>,
}

impl EqualityDeleteWriter {
    /// Create a new `EqualityDeleteWriter`.
    pub async fn try_new(
        operator: Operator,
        table_location: String,
        location_generator: Arc<FileLocationGenerator>,
        arrow_schema: SchemaRef,
        table_config: TableConfigRef,
        equality_ids: Vec<usize>,
    ) -> Result<Self> {
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
        let delete_schema = arrow_schema.project(&col_id_idx).map_err(|err| {
            Error::new(
                ErrorKind::ArrowError,
                format!(
                    "Failed to project schema with equality ids: {:?}, error: {}",
                    equality_ids, err
                ),
            )
        })?;
        Ok(Self {
            inner_writer: RollingWriter::try_new(
                operator,
                location_generator,
                delete_schema.into(),
                table_config,
            )
            .await?,
            table_location,
            equality_ids,
            col_id_idx,
        })
    }

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
    pub async fn close(self) -> Result<Vec<DataFileBuilder>> {
        Ok(self
            .inner_writer
            .close()
            .await?
            .into_iter()
            .map(|builder| {
                builder
                    .with_content(crate::types::DataContentType::EqualityDeletes)
                    .with_table_location(self.table_location.clone())
                    .with_equality_ids(self.equality_ids.iter().map(|&i| i as i32).collect())
            })
            .collect())
    }
}
