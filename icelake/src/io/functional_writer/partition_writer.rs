//! task_writer module provide a task writer for writing data in a table.
//! table writer used directly by the compute engine.
use crate::error::Result;
use crate::io::IcebergWriteResult;
use crate::io::IcebergWriter;
use crate::io::IcebergWriterBuilder;
use crate::types::Any;
use crate::types::FieldProjector;
use crate::types::PartitionKey;
use crate::types::PartitionSpec;
use crate::types::PartitionSplitter;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

/// PartitionWriter can route the batch into different inner writer by partition key.
#[derive(Clone)]
pub struct FanoutPartitionedWriterBuilder<B: IcebergWriterBuilder> {
    inner: B,
    partition_type: Any,
    partition_spec: PartitionSpec,
}

impl<B: IcebergWriterBuilder> FanoutPartitionedWriterBuilder<B> {
    pub fn new(inner: B, partition_type: Any, partition_spec: PartitionSpec) -> Self {
        Self {
            inner,
            partition_type,
            partition_spec,
        }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for FanoutPartitionedWriterBuilder<B>
where
    B::R: IcebergWriter,
{
    type R = FanoutPartitionedWriter<B>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        let (projector, _) =
            FieldProjector::new(schema.fields(), &self.partition_spec.column_ids())?;
        Ok(FanoutPartitionedWriter {
            inner_writers: HashMap::new(),
            partition_splitter: PartitionSplitter::try_new(
                projector,
                &self.partition_spec,
                self.partition_type,
            )?,
            inner_buidler: self.inner,
            schema: schema.clone(),
        })
    }
}

/// Partition append only writer
pub struct FanoutPartitionedWriter<B: IcebergWriterBuilder>
where
    B::R: IcebergWriter,
{
    inner_writers: HashMap<PartitionKey, B::R>,
    partition_splitter: PartitionSplitter,
    inner_buidler: B,
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter for FanoutPartitionedWriter<B>
where
    B::R: IcebergWriter,
{
    type R = <<B as IcebergWriterBuilder>::R as IcebergWriter>::R;

    /// Write a record batch. The `DataFileWriter` will create a new file when the current row num is greater than `target_file_row_num`.
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let split_batch = self.partition_splitter.split_by_partition(&batch)?;

        for (row, batch) in split_batch.into_iter() {
            match self.inner_writers.entry(row) {
                Entry::Occupied(mut writer) => {
                    writer.get_mut().write(batch).await?;
                }
                Entry::Vacant(vacant) => {
                    let new_writer = self.inner_buidler.clone().build(&self.schema).await?;
                    vacant.insert(new_writer).write(batch).await?;
                }
            }
        }
        Ok(())
    }

    /// Complte the write and return the list of `DataFile` as result.
    async fn flush(&mut self) -> Result<Self::R> {
        let mut res_vec = Self::R::empty();
        let inner_writers = std::mem::take(&mut self.inner_writers);
        for (key, mut writer) in inner_writers.into_iter() {
            let partition_value = self.partition_splitter.convert_key_to_value(key)?;
            let mut res = writer.flush().await?;
            res.with_partition(Some(partition_value));
            res_vec.combine(res);
        }
        Ok(res_vec)
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use crate::{
        io::{
            test::{create_arrow_schema, create_batch, create_schema, TestWriterBuilder},
            FanoutPartitionedWriterBuilder, IcebergWriter, IcebergWriterBuilder,
        },
        types::{Any, PartitionField, PartitionSpec},
    };

    fn create_partition() -> PartitionSpec {
        PartitionSpec {
            spec_id: 1,
            fields: vec![PartitionField {
                source_column_id: 1,
                partition_field_id: 1,
                transform: crate::types::Transform::Identity,
                name: "col1".to_string(),
            }],
        }
    }

    #[tokio::test]
    async fn test_partition_writer() {
        let schema = create_schema(2);
        let arrow_schema = create_arrow_schema(2);
        let partition_spec = create_partition();
        let partition_type = Any::Struct(partition_spec.partition_type(&schema).unwrap().into());

        let to_write = create_batch(
            &arrow_schema,
            vec![
                vec![1, 2, 3, 1, 2, 3, 1, 2, 3],
                vec![1, 1, 1, 2, 2, 2, 3, 3, 3],
            ],
        );

        let builder = FanoutPartitionedWriterBuilder::new(
            TestWriterBuilder {},
            partition_type,
            partition_spec,
        );
        let mut writer = builder.build(&arrow_schema).await.unwrap();
        writer.write(to_write).await.unwrap();

        assert_eq!(writer.inner_writers.len(), 3);

        let expect1 = create_batch(&arrow_schema, vec![vec![1, 1, 1], vec![1, 2, 3]]);
        let expect2 = create_batch(&arrow_schema, vec![vec![2, 2, 2], vec![1, 2, 3]]);
        let expect3 = create_batch(&arrow_schema, vec![vec![3, 3, 3], vec![1, 2, 3]]);
        let actual_res = writer
            .inner_writers
            .values()
            .map(|writer| writer.res())
            .collect_vec();
        assert!(actual_res.contains(&expect1));
        assert!(actual_res.contains(&expect2));
        assert!(actual_res.contains(&expect3));
    }

    // # NOTE
    // The delta writer will put the op vec in the last column, this test case test that the partition will
    // ignore the last column.
    #[tokio::test]
    async fn test_partition_delta_writer() {
        let schema = create_schema(2);
        let arrow_schema = create_arrow_schema(3);
        let partition_spec = create_partition();
        let partition_type = Any::Struct(partition_spec.partition_type(&schema).unwrap().into());

        let builder = FanoutPartitionedWriterBuilder::new(
            TestWriterBuilder {},
            partition_type,
            partition_spec,
        );
        let mut writer = builder.build(&arrow_schema).await.unwrap();

        let to_write = create_batch(
            &arrow_schema,
            vec![
                vec![1, 2, 3, 1, 2, 3, 1, 2, 3],
                vec![1, 1, 1, 2, 2, 2, 3, 3, 3],
                vec![3, 2, 1, 1, 3, 2, 2, 1, 3],
            ],
        );
        writer.write(to_write).await.unwrap();
        assert_eq!(writer.inner_writers.len(), 3);
        let expect1 = create_batch(
            &arrow_schema,
            vec![vec![1, 1, 1], vec![1, 2, 3], vec![3, 1, 2]],
        );
        let expect2 = create_batch(
            &arrow_schema,
            vec![vec![2, 2, 2], vec![1, 2, 3], vec![2, 3, 1]],
        );
        let expect3 = create_batch(
            &arrow_schema,
            vec![vec![3, 3, 3], vec![1, 2, 3], vec![1, 2, 3]],
        );
        let actual_res = writer
            .inner_writers
            .values()
            .map(|writer| writer.res())
            .collect_vec();
        assert!(actual_res.contains(&expect1));
        assert!(actual_res.contains(&expect2));
        assert!(actual_res.contains(&expect3));
    }
}
