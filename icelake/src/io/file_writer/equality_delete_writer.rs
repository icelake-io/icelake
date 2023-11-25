//! This module provide `EqualityDeleteWriter`.
use std::sync::Arc;

use crate::{
    io::{IcebergWriteResult, IcebergWriter, IcebergWriterBuilder},
    types::FieldProjector,
    Error, ErrorKind, Result,
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

#[derive(Clone)]
pub struct EqualityDeleteWriterBuilder<B: IcebergWriterBuilder> {
    inner: B,
    equality_ids: Vec<i32>,
}

impl<B: IcebergWriterBuilder> EqualityDeleteWriterBuilder<B> {
    pub fn new(inner: B, equality_ids: Vec<i32>) -> Self {
        Self {
            inner,
            equality_ids,
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for EqualityDeleteWriterBuilder<B>
where
    B::R: IcebergWriter,
{
    type R = EqualityDeleteWriter<B::R>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        let (projector, fields) = FieldProjector::new(schema.fields(), &self.equality_ids)?;
        let delete_schema = Arc::new(arrow_schema::Schema::new(fields));
        Ok(EqualityDeleteWriter {
            inner_writer: self.inner.build(&delete_schema).await?,
            projector,
            delete_schema,
            equality_ids: self.equality_ids,
        })
    }
}

/// EqualityDeleteWriter is a writer that writes to a file in the equality delete format.
pub struct EqualityDeleteWriter<F: IcebergWriter> {
    inner_writer: F,
    projector: FieldProjector,
    delete_schema: SchemaRef,
    equality_ids: Vec<i32>,
}

#[async_trait::async_trait]
impl<F: IcebergWriter> IcebergWriter for EqualityDeleteWriter<F> {
    type R = F::R;
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = RecordBatch::try_new(
            self.delete_schema.clone(),
            self.projector.project(batch.columns()),
        )
        .map_err(|err| Error::new(ErrorKind::IcebergDataInvalid, format!("{err}")))?;
        self.inner_writer.write(batch).await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<F::R> {
        let mut res = self.inner_writer.flush().await?;
        res.with_content(crate::types::DataContentType::EqualityDeletes);
        res.with_equality_ids(self.equality_ids.clone());
        Ok(res)
    }
}
