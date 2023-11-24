//! A module provide `PositionDeleteWriter`.

use std::sync::Arc;

use crate::io::{
    Combinable, IcebergWriteResult, IcebergWriter, IcebergWriterBuilder, SortWriter,
    SortWriterBuilder,
};
use crate::types::{Any, Field, Primitive};
use crate::Result;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{FieldRef as ArrowFieldRef, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};

#[derive(Clone)]
pub struct PositionDeleteWriterBuilder<B: IcebergWriterBuilder> {
    inner: B,
    cache_num: usize,
}

impl<B: IcebergWriterBuilder> PositionDeleteWriterBuilder<B> {
    pub fn new(inner: B, cache_num: usize) -> Self {
        Self { inner, cache_num }
    }
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriterBuilder for PositionDeleteWriterBuilder<B>
where
    B::R: IcebergWriter,
{
    type R = PositionDeleteWriter<B>;

    async fn build(self, _schema: &ArrowSchemaRef) -> Result<Self::R> {
        let fields: Vec<ArrowFieldRef> = vec![
            Arc::new(
                Field::required(2147483546, "file_path", Any::Primitive(Primitive::String))
                    .try_into()?,
            ),
            Arc::new(
                Field::required(2147483545, "pos", Any::Primitive(Primitive::Long)).try_into()?,
            ),
        ];
        let schema = ArrowSchema::new(fields).into();
        Ok(PositionDeleteWriter {
            inner_writer: SortWriterBuilder::new(self.inner, vec![], self.cache_num)
                .build(&schema)
                .await?,
        })
    }
}

//Position deletes are required to be sorted by file and position,
pub struct PositionDeleteWriter<B: IcebergWriterBuilder>
where
    B::R: IcebergWriter,
{
    inner_writer: SortWriter<PositionDeleteInput, B>,
}

#[async_trait::async_trait]
impl<B: IcebergWriterBuilder> IcebergWriter<PositionDeleteInput> for PositionDeleteWriter<B>
where
    B::R: IcebergWriter,
{
    type R = <B::R as IcebergWriter>::R;
    async fn write(&mut self, input: PositionDeleteInput) -> Result<()> {
        self.inner_writer.write(input).await
    }

    async fn close(&mut self) -> Result<Self::R> {
        let mut res = self.inner_writer.close().await?;
        res.with_content(crate::types::DataContentType::PositionDeletes);
        Ok(res)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PositionDeleteInput {
    pub path: String,
    pub offset: i64,
}

impl Combinable for PositionDeleteInput {
    fn combine(vec: Vec<Self>) -> RecordBatch {
        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, false),
            arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false),
        ]);
        let columns = vec![
            Arc::new(arrow_array::StringArray::from(
                vec.iter().map(|i| i.path.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(arrow_array::Int64Array::from(
                vec.iter().map(|i| i.offset).collect::<Vec<_>>(),
            )) as ArrayRef,
        ];
        RecordBatch::try_new(Arc::new(schema), columns).unwrap()
    }

    fn size(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::io::test::{
        create_arrow_schema, create_location_generator, create_operator, read_batch,
    };
    use crate::io::IcebergWriterBuilder;
    use crate::io::{
        BaseFileWriterBuilder, IcebergWriter, ParquetWriterBuilder, PositionDeleteInput,
        PositionDeleteWriterBuilder,
    };

    #[tokio::test]
    async fn test_position_delete_writer() {
        let op = create_operator();
        let location_generator = create_location_generator();
        let parquet_writer_builder = ParquetWriterBuilder::new(op.clone(), 0, Default::default());
        let mut delete_writer = PositionDeleteWriterBuilder::new(
            BaseFileWriterBuilder::new(Arc::new(location_generator), None, parquet_writer_builder),
            100,
        )
        .build(&create_arrow_schema(2))
        .await
        .unwrap();

        let path_col = vec![
            "file1", "file1", "file1", "file2", "file3", "file1", "file2",
        ];
        let offset_col = vec![3, 2, 2, 10, 30, 1, 20];

        for (path, offset) in path_col.iter().zip(offset_col.iter()) {
            delete_writer
                .write(PositionDeleteInput {
                    path: path.to_string(),
                    offset: *offset,
                })
                .await
                .unwrap()
        }

        let data_file_builder = delete_writer.close().await.unwrap();
        assert_eq!(data_file_builder.len(), 1);
        let data_file = data_file_builder
            .into_iter()
            .next()
            .unwrap()
            .with_partition(Default::default())
            .build()
            .unwrap();

        let batch = read_batch(&op, &data_file.file_path).await;

        // generate expect
        let mut expect = path_col
            .into_iter()
            .zip(offset_col.into_iter())
            .collect_vec();
        expect.sort();
        let path_col = expect.iter().map(|(path, _)| *path).collect_vec();
        let offset_col = expect.iter().map(|(_, offset)| *offset).collect_vec();

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap(),
            &arrow_array::StringArray::from(path_col)
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap(),
            &arrow_array::Int64Array::from(offset_col)
        );
    }
}
