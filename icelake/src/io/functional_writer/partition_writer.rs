//! task_writer module provide a task writer for writing data in a table.
//! table writer used directly by the compute engine.
use crate::error::Result;
use crate::io::RecordBatchWriter;
use crate::io::RecordBatchWriterBuilder;
use crate::types::Any;
use crate::types::DataFileBuilder;
use crate::types::FieldProjector;
use crate::types::PartitionKey;
use crate::types::PartitionSpec;
use crate::types::PartitionSplitter;
use arrow_array::RecordBatch;
use arrow_schema::Field;
use arrow_schema::Fields;
use arrow_schema::SchemaRef;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

/// PartitionWriter can route the batch into different inner writer by partition key.
#[derive(Clone)]
pub struct PartitionedWriterBuilder<L: RecordBatchWriterBuilder> {
    inner: L,
    partition_type: Any,
    partition_spec: PartitionSpec,
    is_upsert: bool,
}

impl<L: RecordBatchWriterBuilder> PartitionedWriterBuilder<L> {
    pub fn new(
        inner: L,
        partition_type: Any,
        partition_spec: PartitionSpec,
        is_upsert: bool,
    ) -> Self {
        Self {
            inner,
            partition_type,
            partition_spec,
            is_upsert,
        }
    }
}

#[async_trait::async_trait]
impl<L: RecordBatchWriterBuilder> RecordBatchWriterBuilder for PartitionedWriterBuilder<L> {
    type R = PartitionedWriter<L>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        let (projector, _) = if !self.is_upsert {
            FieldProjector::new(schema.fields(), &self.partition_spec.column_ids())?
        } else {
            let mut new_fields = Vec::with_capacity(schema.fields().len() + 1);
            new_fields.push(Arc::new(Field::new(
                "",
                arrow_schema::DataType::Int32,
                false,
            )));
            new_fields.extend(schema.fields().iter().cloned());
            FieldProjector::new(
                &Fields::from_iter(new_fields.into_iter()),
                &self.partition_spec.column_ids(),
            )?
        };
        Ok(PartitionedWriter {
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
pub struct PartitionedWriter<L: RecordBatchWriterBuilder> {
    inner_writers: HashMap<PartitionKey, L::R>,
    partition_splitter: PartitionSplitter,
    inner_buidler: L,
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl<L: RecordBatchWriterBuilder> RecordBatchWriter for PartitionedWriter<L> {
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
    async fn close(&mut self) -> Result<Vec<DataFileBuilder>> {
        let mut res = vec![];
        let inner_writers = std::mem::take(&mut self.inner_writers);
        for (key, mut writer) in inner_writers.into_iter() {
            let data_file_builders = writer.close().await?;

            let partition_value = self.partition_splitter.convert_key_to_value(key)?;

            res.extend(data_file_builders.into_iter().map(|data_file_builder| {
                data_file_builder.with_partition_value(Some(partition_value.clone()))
            }));
        }
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::{
        io::{
            PartitionedWriterBuilder, RecordBatchWriter, RecordBatchWriterBuilder,
            TestWriterBuilder,
        },
        types::{Any, Field, PartitionField, PartitionSpec, Schema, Struct},
    };

    fn create_partition() -> (Schema, PartitionSpec) {
        let schema = Schema::new(
            1,
            None,
            Struct::new(vec![
                Field::required(
                    1,
                    "pk",
                    crate::types::Any::Primitive(crate::types::Primitive::Long),
                )
                .into(),
                Field::required(
                    2,
                    "data",
                    crate::types::Any::Primitive(crate::types::Primitive::Long),
                )
                .into(),
            ]),
        );
        let partition_spec = PartitionSpec {
            spec_id: 1,
            fields: vec![PartitionField {
                source_column_id: 1,
                partition_field_id: 1,
                transform: crate::types::Transform::Identity,
                name: "pk".to_string(),
            }],
        };
        (schema, partition_spec)
    }

    #[tokio::test]
    async fn test_partition_writer() {
        let (schema, partition_spec) = create_partition();
        let partition_type = Any::Struct(partition_spec.partition_type(&schema).unwrap().into());
        let arrow_schema: arrow_schema::SchemaRef = Arc::new(schema.try_into().unwrap());

        let data = [
            (1, 1),
            (2, 1),
            (3, 1),
            (1, 2),
            (2, 2),
            (3, 2),
            (1, 3),
            (2, 3),
            (3, 3),
        ];
        let col = Arc::new(arrow_array::Int64Array::from_iter_values(
            data.iter().map(|(pk, _)| *pk),
        )) as arrow_array::ArrayRef;
        let col2 = Arc::new(arrow_array::Int64Array::from_iter_values(
            data.iter().map(|(_, data)| *data),
        )) as arrow_array::ArrayRef;
        let to_write =
            arrow_array::RecordBatch::try_from_iter([("pk", col.clone()), ("data", col2.clone())])
                .unwrap();

        let builder = PartitionedWriterBuilder::new(
            TestWriterBuilder {},
            partition_type,
            partition_spec,
            false,
        );
        let mut writer = builder.build(&arrow_schema).await.unwrap();
        writer.write(to_write).await.unwrap();

        assert_eq!(writer.inner_writers.len(), 3);

        let expect1 = arrow_array::RecordBatch::try_from_iter([
            (
                "pk",
                Arc::new(arrow_array::Int64Array::from(vec![1, 1, 1])) as arrow_array::ArrayRef,
            ),
            (
                "data",
                Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let expect2 = arrow_array::RecordBatch::try_from_iter([
            (
                "pk",
                Arc::new(arrow_array::Int64Array::from(vec![2, 2, 2])) as arrow_array::ArrayRef,
            ),
            (
                "data",
                Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let expect3 = arrow_array::RecordBatch::try_from_iter([
            (
                "pk",
                Arc::new(arrow_array::Int64Array::from(vec![3, 3, 3])) as arrow_array::ArrayRef,
            ),
            (
                "data",
                Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
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
    // This test case test that partition writer should guarantee the order within one partition is hold after partition.
    #[tokio::test]
    async fn test_partition_upsert_writer() {
        let (schema, partition_spec) = create_partition();
        let partition_type = Any::Struct(partition_spec.partition_type(&schema).unwrap().into());
        let arrow_schema: arrow_schema::SchemaRef = Arc::new(schema.try_into().unwrap());

        let data = vec![
            (3, 1, 1),
            (2, 2, 1),
            (3, 3, 1),
            (2, 1, 2),
            (3, 2, 2),
            (1, 3, 2),
            (1, 1, 3),
            (1, 2, 3),
            (2, 3, 3),
        ];
        let op_col = Arc::new(arrow_array::Int32Array::from_iter_values(
            data.iter().map(|(op, _, _)| *op),
        )) as arrow_array::ArrayRef;
        let col = Arc::new(arrow_array::Int64Array::from_iter_values(
            data.iter().map(|(_, pk, _)| *pk),
        )) as arrow_array::ArrayRef;
        let col2 = Arc::new(arrow_array::Int64Array::from_iter_values(
            data.iter().map(|(_, _, data)| *data),
        )) as arrow_array::ArrayRef;
        let to_write = arrow_array::RecordBatch::try_from_iter([
            ("op", op_col.clone()),
            ("pk", col.clone()),
            ("data", col2.clone()),
        ])
        .unwrap();

        let builder = PartitionedWriterBuilder::new(
            TestWriterBuilder {},
            partition_type,
            partition_spec,
            true,
        );
        let mut writer = builder.build(&arrow_schema).await.unwrap();
        writer.write(to_write).await.unwrap();

        assert_eq!(writer.inner_writers.len(), 3);

        let expect1 = arrow_array::RecordBatch::try_from_iter([
            (
                "op",
                Arc::new(arrow_array::Int32Array::from(vec![3, 2, 1])) as arrow_array::ArrayRef,
            ),
            (
                "pk",
                Arc::new(arrow_array::Int64Array::from(vec![1, 1, 1])) as arrow_array::ArrayRef,
            ),
            (
                "data",
                Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let expect2 = arrow_array::RecordBatch::try_from_iter([
            (
                "op",
                Arc::new(arrow_array::Int32Array::from(vec![2, 3, 1])) as arrow_array::ArrayRef,
            ),
            (
                "pk",
                Arc::new(arrow_array::Int64Array::from(vec![2, 2, 2])) as arrow_array::ArrayRef,
            ),
            (
                "data",
                Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let expect3 = arrow_array::RecordBatch::try_from_iter([
            (
                "op",
                Arc::new(arrow_array::Int32Array::from(vec![3, 1, 2])) as arrow_array::ArrayRef,
            ),
            (
                "pk",
                Arc::new(arrow_array::Int64Array::from(vec![3, 3, 3])) as arrow_array::ArrayRef,
            ),
            (
                "data",
                Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
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
