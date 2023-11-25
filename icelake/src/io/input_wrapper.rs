use std::sync::Arc;

use crate::io::IcebergWriter;
use crate::types::DataFileBuilder;
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::Field;

use super::DeltaResult;

pub struct DeltaWriter {
    inner: Box<dyn IcebergWriter<R = DeltaResult<Vec<DataFileBuilder>>>>,
}

impl DeltaWriter {
    pub fn new(inner: impl IcebergWriter<R = DeltaResult<Vec<DataFileBuilder>>>) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }

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

    pub async fn close(&mut self) -> Result<DeltaResult<Vec<DataFileBuilder>>> {
        self.inner.flush().await
    }
}

pub struct RecordBatchWriter {
    inner: Box<dyn IcebergWriter<R = Vec<DataFileBuilder>>>,
}

impl RecordBatchWriter {
    pub fn new(inner: impl IcebergWriter<R = Vec<DataFileBuilder>>) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }

    pub async fn write(&mut self, input: RecordBatch) -> Result<()> {
        self.inner.write(input).await
    }

    pub async fn flush(&mut self) -> Result<Vec<DataFileBuilder>> {
        self.inner.flush().await
    }
}

#[cfg(test)]
mod test {
    fn check_send<T: Send>() {}

    #[test]
    fn guarantee_send() {
        check_send::<super::DeltaWriter>();
        check_send::<super::RecordBatchWriter>();
    }
}
