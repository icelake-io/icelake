use std::sync::Arc;

use super::IcebergWriter;
use crate::types::{DataFile, DataFileBuilderV2};
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::Field;
use itertools::Itertools;

use super::DeltaResult;

pub struct DeltaFile {
    pub data: Vec<DataFile>,
    pub pos_delete: Vec<DataFile>,
    pub eq_delete: Vec<DataFile>,
}

pub struct DeltaWriter {
    inner: Box<dyn IcebergWriter<R = DeltaResult<DataFileBuilderV2>>>,
}

impl DeltaWriter {
    pub fn new(inner: impl IcebergWriter<R = DeltaResult<DataFileBuilderV2>>) -> Self {
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

    pub async fn flush(&mut self) -> Result<Vec<DeltaFile>> {
        let builders = self.inner.flush().await?;
        let mut delta_files = Vec::with_capacity(builders.len());
        for res in builders {
            delta_files.push(DeltaFile {
                data: res
                    .data
                    .into_iter()
                    .map(|b| b.build().expect("build data file failed"))
                    .collect_vec(),
                pos_delete: res
                    .pos_delete
                    .into_iter()
                    .map(|b| b.build().expect("build pos delete file failed"))
                    .collect_vec(),
                eq_delete: res
                    .eq_delete
                    .into_iter()
                    .map(|b| b.build().expect("build eq delete file fail"))
                    .collect_vec(),
            })
        }
        Ok(delta_files)
    }
}

pub struct RecordBatchWriter {
    inner: Box<dyn IcebergWriter<R = DataFileBuilderV2>>,
}

impl RecordBatchWriter {
    pub fn new(inner: impl IcebergWriter<R = DataFileBuilderV2>) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }

    pub async fn write(&mut self, input: RecordBatch) -> Result<()> {
        self.inner.write(input).await
    }

    pub async fn flush(&mut self) -> Result<Vec<DataFile>> {
        Ok(self
            .inner
            .flush()
            .await?
            .into_iter()
            .map(|b| b.build().expect("build data file fail"))
            .collect_vec())
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
