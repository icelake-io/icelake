//! This module provide `EqualityDeleteWriter`.
use std::sync::Arc;

use crate::{
    io::{
        FileWriteResult, FileWriter, FileWriterBuilder, IcebergWriteResult, IcebergWriter,
        IcebergWriterBuilder,
    },
    types::FieldProjector,
    Error, ErrorKind, Result,
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

#[derive(Clone)]
pub struct EqualityDeleteWriterBuilder<B: FileWriterBuilder> {
    inner: B,
    equality_ids: Vec<i32>,
}

impl<B: FileWriterBuilder> EqualityDeleteWriterBuilder<B> {
    pub fn new(inner: B, equality_ids: Vec<i32>) -> Self {
        Self {
            inner,
            equality_ids,
        }
    }
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriterBuilder for EqualityDeleteWriterBuilder<B> {
    type R = EqualityDeleteWriter<B>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        let (projector, fields) = FieldProjector::new(schema.fields(), &self.equality_ids)?;
        let delete_schema = Arc::new(arrow_schema::Schema::new(fields));
        Ok(EqualityDeleteWriter {
            inner_writer: self.inner.clone().build(&delete_schema).await?,
            writer_builder: self.inner,
            projector,
            delete_schema,
            equality_ids: self.equality_ids,
        })
    }
}

/// EqualityDeleteWriter is a writer that writes to a file in the equality delete format.
pub struct EqualityDeleteWriter<B: FileWriterBuilder> {
    writer_builder: B,
    inner_writer: B::R,
    projector: FieldProjector,
    delete_schema: SchemaRef,
    equality_ids: Vec<i32>,
}

#[async_trait::async_trait]
impl<B: FileWriterBuilder> IcebergWriter for EqualityDeleteWriter<B> {
    type R = <<B::R as FileWriter>::R as FileWriteResult>::R;
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = RecordBatch::try_new(
            self.delete_schema.clone(),
            self.projector.project(batch.columns()),
        )
        .map_err(|err| Error::new(ErrorKind::IcebergDataInvalid, format!("{err}")))?;
        self.inner_writer.write(&batch).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<Self::R> {
        let writer = std::mem::replace(
            &mut self.inner_writer,
            self.writer_builder
                .clone()
                .build(&self.delete_schema)
                .await?,
        );
        let mut res = writer.close().await?.to_iceberg_result();
        res.with_content(crate::types::DataContentType::EqualityDeletes);
        res.with_equality_ids(self.equality_ids.clone());
        Ok(res)
    }
}
