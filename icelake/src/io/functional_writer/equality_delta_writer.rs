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
    is_pos_delete_with_row: bool,
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
        is_pos_delete_with_row: bool,
    ) -> Self {
        Self {
            data_file_writer_builder,
            sorted_cache_writer_builder,
            equality_delete_writer_builder,
            unique_column_ids,
            is_pos_delete_with_row,
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
            self.is_pos_delete_with_row,
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
    _is_pos_delete_with_row: bool,
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
        is_pos_delete_with_row: bool,
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
            _is_pos_delete_with_row: is_pos_delete_with_row,
        })
    }

    /// Write the batch.
    /// 1. If a row with the same unique column is not written, then insert it.
    /// 2. If a row with the same unique column is written, then delete the previous row and insert the new row.
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
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
        PD: IcebergWriter<PositionDeleteInput>,
        ED: IcebergWriter<R = PD::R>,
    > IcebergWriter for EqualityDeltaWriter<D, PD, ED>
{
    type R = DeltaResult<D::R, PD::R>;

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
                INSERT_OP => self.write(batch).await?,
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

    async fn close(&mut self) -> Result<Self::R> {
        let data_res = self.data_file_writer.close().await?;
        let mut delete_res = self.equality_delete_writer.close().await?;
        delete_res.combine(self.position_delete_writer.close().await?);
        Ok(DeltaResult {
            data: data_res,
            delete: delete_res,
        })
    }
}

#[derive(Default)]
pub struct DeltaResult<D: IcebergWriteResult, DE: IcebergWriteResult> {
    pub data: D,
    pub delete: DE,
}

impl<D: IcebergWriteResult, DE: IcebergWriteResult> IcebergWriteResult for DeltaResult<D, DE> {
    fn combine(&mut self, other: Self) {
        self.data.combine(other.data);
        self.delete.combine(other.delete);
    }

    fn with_file_path(&mut self, file_name: String) -> &mut Self {
        self.data.with_file_path(file_name.clone());
        self.delete.with_file_path(file_name);
        self
    }

    fn with_content(&mut self, content: crate::types::DataContentType) -> &mut Self {
        self.data.with_content(content);
        self.delete.with_content(content);
        self
    }

    fn with_equality_ids(&mut self, equality_ids: Vec<i32>) -> &mut Self {
        self.data.with_equality_ids(equality_ids.clone());
        self.delete.with_equality_ids(equality_ids);
        self
    }

    fn with_partition(&mut self, partition_value: Option<crate::types::StructValue>) -> &mut Self {
        self.data.with_partition(partition_value.clone());
        self.delete.with_partition(partition_value);
        self
    }

    fn flush(&mut self) -> Self {
        Self {
            data: self.data.flush(),
            delete: self.delete.flush(),
        }
    }
}
