//! This module provide the `EqualityDeltaWriter`.

// /// The close result return by DeltaWriter.
// pub struct DeltaWriterResult {
//     /// DataFileBuilder for written data.
//     pub data: Vec<DataFile>,
//     /// DataFileBuilder for position delete.
//     pub pos_delete: Vec<DataFile>,
//     /// DataFileBuilder for equality delete.
//     pub eq_delete: Vec<DataFile>,
// }

use crate::io::PositionDeleteInput;
use crate::{
    io::{IcebergWriteResult, IcebergWriter, IcebergWriterBuilder},
    Result,
};
use arrow_array::{builder::BooleanBuilder, Int32Array, RecordBatch};
use arrow_ord::partition::partition;
use arrow_row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_schema::SchemaRef;
use arrow_select::filter;
use itertools::Itertools;
use std::collections::HashMap;

use crate::{io::SingletonWriter, types::FieldProjector, Error, ErrorKind};

pub const INSERT_OP: i32 = 1;
pub const DELETE_OP: i32 = 2;

///`EqualityDeltaWriter` is same as: https://github.com/apache/iceberg/blob/2e1ec5fde9e6fecfbc0883465a585a1dacb58c05/core/src/main/java/org/apache/iceberg/io/BaseTaskWriter.java#L108
///
///EqualityDeltaWriter will guarantee that there is only one row with the unique columns value written. When
///insert a row with the same unique columns value, it will delete the previous row.
///
///NOTE:
///This write is not as same with `upsert`. If the row with same unique columns is not written in
///this writer, it will not delete it.
#[derive(Clone)]
pub struct EqualityDeltaWriterBuilder<
    DB: IcebergWriterBuilder,
    PDB: IcebergWriterBuilder,
    EDB: IcebergWriterBuilder,
> where
    DB::R: SingletonWriter + IcebergWriter,
    PDB::R: IcebergWriter<PositionDeleteInput>,
{
    data_file_writer_builder: DB,
    sorted_cache_writer_builder: PDB,
    equality_delete_writer_builder: EDB,
    unique_column_ids: Vec<i32>,
}

impl<DB: IcebergWriterBuilder, PDB: IcebergWriterBuilder, EDB: IcebergWriterBuilder>
    EqualityDeltaWriterBuilder<DB, PDB, EDB>
where
    DB::R: SingletonWriter + IcebergWriter,
    PDB::R: IcebergWriter<PositionDeleteInput>,
    EDB::R: IcebergWriter,
{
    pub fn new(
        data_file_writer_builder: DB,
        sorted_cache_writer_builder: PDB,
        equality_delete_writer_builder: EDB,
        unique_column_ids: Vec<i32>,
    ) -> Self {
        Self {
            data_file_writer_builder,
            sorted_cache_writer_builder,
            equality_delete_writer_builder,
            unique_column_ids,
        }
    }
}

#[async_trait::async_trait]
impl<DB: IcebergWriterBuilder, PDB: IcebergWriterBuilder, EDB: IcebergWriterBuilder>
    IcebergWriterBuilder for EqualityDeltaWriterBuilder<DB, PDB, EDB>
where
    DB::R: SingletonWriter + IcebergWriter,
    PDB::R: IcebergWriter<PositionDeleteInput>,
    EDB::R: IcebergWriter,
{
    type R = EqualityDeltaWriter<DB::R, PDB::R, EDB::R>;

    async fn build(self, schema: &SchemaRef) -> Result<Self::R> {
        let data_file_writer = self.data_file_writer_builder.build(schema).await?;
        let position_delete_writer = self.sorted_cache_writer_builder.build(schema).await?;
        let equality_delete_writer = self.equality_delete_writer_builder.build(schema).await?;
        EqualityDeltaWriter::try_new(
            data_file_writer,
            position_delete_writer,
            equality_delete_writer,
            self.unique_column_ids,
            schema,
        )
    }
}

pub struct EqualityDeltaWriter<
    D: SingletonWriter + IcebergWriter,
    PD: IcebergWriter<PositionDeleteInput>,
    ED: IcebergWriter,
> {
    data_file_writer: D,
    position_delete_writer: PD,
    equality_delete_writer: ED,
    inserted_rows: HashMap<OwnedRow, PositionDeleteInput>,
    row_converter: RowConverter,
    projector: FieldProjector,
}

impl<
        D: SingletonWriter + IcebergWriter,
        PD: IcebergWriter<PositionDeleteInput>,
        ED: IcebergWriter,
    > EqualityDeltaWriter<D, PD, ED>
{
    pub fn try_new(
        data_file_writer: D,
        position_delete_writer: PD,
        equality_delete_writer: ED,
        unique_column_ids: Vec<i32>,
        schema: &SchemaRef,
    ) -> Result<Self> {
        let (projector, unique_col_fields) =
            FieldProjector::new(schema.fields(), &unique_column_ids)?;

        let row_converter = RowConverter::new(
            unique_col_fields
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )
        .map_err(|err| {
            Error::new(
                ErrorKind::ArrowError,
                format!("Failed to create row converter, error: {}", err),
            )
        })?;
        Ok(Self {
            data_file_writer,
            position_delete_writer,
            equality_delete_writer,
            inserted_rows: HashMap::new(),
            row_converter,
            projector,
        })
    }

    /// Write the batch.
    /// 1. If a row with the same unique column is not written, then insert it.
    /// 2. If a row with the same unique column is written, then delete the previous row and insert the new row.
    async fn insert(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let current_file_path = self.data_file_writer.current_file();
        let current_file_offset = self.data_file_writer.current_row_num();
        for (idx, row) in rows.iter().enumerate() {
            let previous_input = self.inserted_rows.insert(
                row.owned(),
                PositionDeleteInput {
                    path: current_file_path.clone(),
                    offset: (current_file_offset + idx) as i64,
                },
            );
            if let Some(previous_input) = previous_input {
                self.position_delete_writer.write(previous_input).await?;
            }
        }

        self.data_file_writer.write(batch).await?;

        Ok(())
    }

    /// Delete the batch.
    pub async fn delete(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = self.extract_unique_column(&batch)?;
        let mut delete_row = BooleanBuilder::new();
        for row in rows.iter() {
            if let Some(previous_input) = self.inserted_rows.remove(&row.owned()) {
                self.position_delete_writer.write(previous_input).await?;
                delete_row.append_value(false);
            } else {
                delete_row.append_value(true);
            }
        }
        let delete_batch =
            filter::filter_record_batch(&batch, &delete_row.finish()).map_err(|err| {
                Error::new(
                    ErrorKind::ArrowError,
                    format!("Failed to filter record batch, error: {}", err),
                )
            })?;
        self.equality_delete_writer.write(delete_batch).await?;
        Ok(())
    }

    fn extract_unique_column(&mut self, batch: &RecordBatch) -> Result<Rows> {
        self.row_converter
            .convert_columns(&self.projector.project(batch.columns()))
            .map_err(|err| {
                Error::new(
                    ErrorKind::ArrowError,
                    format!("Failed to convert columns, error: {}", err),
                )
            })
    }
}

#[async_trait::async_trait]
impl<
        D: SingletonWriter + IcebergWriter,
        PD: IcebergWriter<PositionDeleteInput, R = D::R>,
        ED: IcebergWriter<R = D::R>,
    > IcebergWriter for EqualityDeltaWriter<D, PD, ED>
{
    type R = DeltaResult<D::R>;

    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        // check the last column is int32 array.
        let ops = batch
            .column(batch.num_columns() - 1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or(Error::new(ErrorKind::IcebergDataInvalid, ""))?;
        // partition the ops.
        let partitions =
            partition(&[batch.column(batch.num_columns() - 1).clone()]).map_err(|err| {
                Error::new(
                    ErrorKind::ArrowError,
                    format!("Failed to partition ops, error: {}", err),
                )
            })?;
        for range in partitions.ranges() {
            let batch = batch
                .project(&(0..batch.num_columns() - 1).collect_vec())
                .unwrap()
                .slice(range.start, range.end - range.start);
            match ops.value(range.start) {
                // Insert
                INSERT_OP => self.insert(batch).await?,
                // Delete
                DELETE_OP => self.delete(batch).await?,
                op => {
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("Invalid ops: {op}"),
                    ))
                }
            }
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<Self::R> {
        let data = self.data_file_writer.flush().await?;
        let eq_delete = self.equality_delete_writer.flush().await?;
        let pos_delete = self.position_delete_writer.flush().await?;
        self.inserted_rows.clear();
        Ok(DeltaResult {
            data,
            pos_delete,
            eq_delete,
        })
    }
}

#[derive(Default)]
pub struct DeltaResult<D: IcebergWriteResult> {
    pub data: D,
    pub pos_delete: D,
    pub eq_delete: D,
}

impl<D: IcebergWriteResult> IcebergWriteResult for DeltaResult<D> {
    fn combine(&mut self, other: Self) {
        self.data.combine(other.data);
        self.pos_delete.combine(other.pos_delete);
        self.eq_delete.combine(other.eq_delete)
    }

    fn with_file_path(&mut self, file_name: String) -> &mut Self {
        self.data.with_file_path(file_name.clone());
        self.pos_delete.with_file_path(file_name.clone());
        self.eq_delete.with_file_path(file_name);
        self
    }

    fn with_content(&mut self, content: crate::types::DataContentType) -> &mut Self {
        self.data.with_content(content);
        self.pos_delete.with_content(content);
        self.eq_delete.with_content(content);
        self
    }

    fn with_equality_ids(&mut self, equality_ids: Vec<i32>) -> &mut Self {
        self.data.with_equality_ids(equality_ids.clone());
        self.pos_delete.with_equality_ids(equality_ids.clone());
        self.eq_delete.with_equality_ids(equality_ids);
        self
    }

    fn with_partition(&mut self, partition_value: Option<crate::types::StructValue>) -> &mut Self {
        self.data.with_partition(partition_value.clone());
        self.pos_delete.with_partition(partition_value.clone());
        self.eq_delete.with_partition(partition_value);
        self
    }

    fn flush(&mut self) -> Self {
        Self {
            data: self.data.flush(),
            pos_delete: self.pos_delete.flush(),
            eq_delete: self.eq_delete.flush(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::Array;

    use crate::io::{
        test::{create_arrow_schema, create_batch, create_location_generator, create_operator},
        BaseFileWriterBuilder, DataFileWriterBuilder, EqualityDeleteWriterBuilder,
        EqualityDeltaWriterBuilder, IcebergWriteResult, IcebergWriter, IcebergWriterBuilder,
        ParquetWriterBuilder, PositionDeleteWriterBuilder,
    };

    #[tokio::test]
    async fn test_delta_writer() {
        // create writer
        let op = create_operator();
        let location_generator = create_location_generator();
        let arrow_schema = create_arrow_schema(3);
        let simple_builder = BaseFileWriterBuilder::new(
            Arc::new(location_generator),
            None,
            ParquetWriterBuilder::new(op.clone(), 0, Default::default()),
        );
        let pos_delete_writer_builder =
            PositionDeleteWriterBuilder::new(simple_builder.clone(), 100);
        let data_writer_buidler = DataFileWriterBuilder::new(simple_builder.clone());
        let equality_delete_writer_builder =
            EqualityDeleteWriterBuilder::new(simple_builder, vec![1, 2]);
        let mut delta_writer = EqualityDeltaWriterBuilder::new(
            data_writer_buidler,
            pos_delete_writer_builder,
            equality_delete_writer_builder,
            vec![1, 2],
        )
        .build(&arrow_schema)
        .await
        .unwrap();

        // test insert
        let to_insert = create_batch(
            &arrow_schema,
            vec![
                vec![1, 2, 3, 1, 2, 3, 1, 2, 3],
                vec![1, 1, 1, 2, 2, 2, 3, 3, 3],
                vec![1, 2, 3, 1, 2, 3, 1, 2, 3],
            ],
        );
        delta_writer.insert(to_insert.clone()).await.unwrap();
        let mut res = delta_writer.flush().await.unwrap();
        assert_eq!(res.data.len(), 1);
        assert_eq!(res.pos_delete.len(), 0);
        assert_eq!(res.eq_delete.len(), 0);
        res.with_partition(None);
        let res = crate::io::test::read_batch(&op, &res.data[0].build().unwrap().file_path).await;
        assert_eq!(res, to_insert);

        // test upsert
        let to_insert = create_batch(
            &arrow_schema,
            vec![vec![10, 11], vec![10, 11], vec![10, 11]],
        );
        let to_delete = create_batch(&arrow_schema, vec![vec![11, 1], vec![11, 1], vec![11, 1]]);
        delta_writer.insert(to_insert.clone()).await.unwrap();
        delta_writer.delete(to_delete.clone()).await.unwrap();
        let mut delta_res = delta_writer.flush().await.unwrap();
        assert_eq!(delta_res.data.len(), 1);
        assert_eq!(delta_res.pos_delete.len(), 1);
        assert_eq!(delta_res.eq_delete.len(), 1);
        delta_res.with_partition(Default::default());
        let data = delta_res.data[0].build().unwrap();
        let pos_delete = delta_res.pos_delete[0].build().unwrap();
        let eq_delete = delta_res.eq_delete[0].build().unwrap();
        let data_batch = crate::io::test::read_batch(&op, &data.file_path).await;
        let pos_delete_batch = crate::io::test::read_batch(&op, &pos_delete.file_path).await;
        let eq_delete_batch = crate::io::test::read_batch(&op, &eq_delete.file_path).await;
        // check data
        assert_eq!(data_batch, to_insert);
        // check position delete
        let paths = pos_delete_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths.value(0), data.file_path);
        assert_eq!(
            eq_delete_batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        // check equality delete
        assert_eq!(
            eq_delete_batch,
            create_batch(&create_arrow_schema(2), vec![vec![1], vec![1]])
        );
    }
}
