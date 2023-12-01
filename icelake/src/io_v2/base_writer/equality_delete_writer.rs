//! This module provide `EqualityDeleteWriter`.
use std::sync::Arc;

use crate::{
    io_v2::{
        FileWriteResult, FileWriter, FileWriterBuilder, IcebergWriteResult, IcebergWriter,
        IcebergWriterBuilder,
    },
    types::FieldProjector,
    Error, ErrorKind, Result,
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use itertools::Itertools;

#[derive(Clone)]
pub struct EqualityDeleteWriterBuilder<B: FileWriterBuilder> {
    inner: B,
    equality_ids: Vec<usize>,
}

impl<B: FileWriterBuilder> EqualityDeleteWriterBuilder<B> {
    pub fn new(inner: B, equality_ids: Vec<usize>) -> Self {
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
    equality_ids: Vec<usize>,
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

    async fn flush(&mut self) -> Result<Vec<Self::R>> {
        let writer = std::mem::replace(
            &mut self.inner_writer,
            self.writer_builder
                .clone()
                .build(&self.delete_schema)
                .await?,
        );
        let res = writer
            .close()
            .await?
            .into_iter()
            .map(|res| {
                let mut res = res.to_iceberg_result();
                res.set_content(crate::types::DataContentType::EqualityDeletes)
                    .set_equality_ids(self.equality_ids.iter().map(|id| *id as i32).collect_vec())
                    .set_partition(None);
                res
            })
            .collect_vec();
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::io_v2::{
        test::{
            create_arrow_schema, create_batch, create_location_generator, create_operator,
            read_batch,
        },
        BaseFileWriterBuilder, EqualityDeleteWriterBuilder, IcebergWriter, IcebergWriterBuilder,
        ParquetWriterBuilder,
    };

    #[tokio::test]
    async fn test_equality_delete_writer() -> Result<(), anyhow::Error> {
        // create writer
        let op = create_operator();
        let location_generator = create_location_generator();
        let schema = create_arrow_schema(3);
        let parquet_writer_builder = ParquetWriterBuilder::new(
            op.clone(),
            0,
            Default::default(),
            "/".to_string(),
            Arc::new(location_generator),
        );
        let rolling_writer_builder = BaseFileWriterBuilder::new(None, parquet_writer_builder);
        let mut equality_delete_writer =
            EqualityDeleteWriterBuilder::new(rolling_writer_builder, vec![2, 3])
                .build(&schema)
                .await?;

        // write 1024 * 3 column in 1 write
        let to_write = create_batch(&schema, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]);
        equality_delete_writer.write(to_write).await?;

        // check output
        let res = equality_delete_writer.flush().await.unwrap();
        assert!(res.len() == 1);
        let data_file = res[0].build().unwrap();
        assert_eq!(data_file.equality_ids, Some(vec![2, 3]));
        let batch = read_batch(&op, &data_file.file_path).await;
        assert_eq!(
            batch,
            create_batch(
                &Arc::new(schema.project(&[1, 2]).unwrap()),
                vec![vec![4, 5, 6], vec![7, 8, 9]]
            )
        );

        Ok(())
    }
}
