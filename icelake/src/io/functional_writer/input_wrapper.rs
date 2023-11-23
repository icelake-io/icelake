use std::sync::Arc;

use crate::io::{IcebergWriter, IcebergWriterBuilder};
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::{Field, SchemaRef};

#[derive(Clone)]
pub struct UpsertWrapWriterBuilder<B: IcebergWriterBuilder> {
    inner: B,
}

impl<B: IcebergWriterBuilder> UpsertWrapWriterBuilder<B> {
    pub fn new(inner: B) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for UpsertWrapWriterBuilder<B>
where
    B::R: IcebergWriter,
{
    type R = UpsertWrapperWriter<B::R>;

    async fn build(self, schama: &SchemaRef) -> Result<Self::R> {
        Ok(UpsertWrapperWriter {
            inner: self.inner.build(schama).await?,
        })
    }
}

pub struct UpsertWrapperWriter<R: IcebergWriter> {
    inner: R,
}

impl<R: IcebergWriter> UpsertWrapperWriter<R> {
    pub async fn write(&mut self, op: impl Into<Vec<i32>>, input: RecordBatch) -> Result<()> {
        let mut fields = input.schema().fields().to_vec();
        fields.push(Arc::new(Field::new(
            "",
            arrow_schema::DataType::Int32,
            false,
        )));

        let schema = Arc::new(arrow_schema::Schema::new(fields));

        let mut columns = input.columns().to_vec();
        columns.push(Arc::new(arrow_array::Int32Array::from(op.into())));

        let batch = RecordBatch::try_new(schema, columns).unwrap();

        self.inner.write(batch).await
    }

    pub async fn close(&mut self) -> Result<R::R> {
        self.inner.close().await
    }
}
