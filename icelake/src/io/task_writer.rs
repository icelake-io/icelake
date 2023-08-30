//! task_writer module provide a task writer for writing data in a table.
//! table writer used directly by the compute engine.

use super::data_file_writer::DataFileWriter;
use super::location_generator;
use crate::config::TableConfigRef;
use crate::error::Result;
use crate::io::location_generator::DataFileLocationGenerator;
use crate::types::BoxedTransformFunction;
use crate::types::PartitionSpec;
use crate::types::{create_transform_function, DataFile, TableMetadata};
use crate::types::{struct_to_anyvalue_array_with_type, Any, AnyValue};
use crate::Error;
use crate::ErrorKind;
use arrow_array::ArrayRef;
use arrow_array::RecordBatch;
use arrow_array::{Array, BooleanArray, StructArray};
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Fields;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use arrow_select::filter::filter_record_batch;
use opendal::Operator;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
/// `TaskWriter` used to write data for a table.
///
/// If it find that the table metadata has partition spec, it will create a
/// partitioned task writer. The partition task writer will split the data according
/// the partition key and write them using different data file writer.
///
/// If the table metadata has no partition spec, it will create a unpartitioned
/// task writer. The unpartitioned task writer will write all data using a single
/// data file writer.
pub enum TaskWriter {
    /// Unpartitioned task writer
    Unpartitioned(UnpartitionedWriter),
    /// Partitioned task writer
    Partitioned(PartitionedWriter),
}

impl TaskWriter {
    /// Create a new `TaskWriter`.
    pub async fn try_new(
        table_metadata: TableMetadata,
        operator: Operator,
        partition_id: usize,
        task_id: usize,
        suffix: Option<String>,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        let current_schema = table_metadata
            .schemas
            .clone()
            .into_iter()
            .find(|schema| schema.schema_id == table_metadata.current_schema_id)
            .ok_or_else(|| {
                crate::error::Error::new(
                    crate::ErrorKind::IcebergDataInvalid,
                    "Can't find current schema",
                )
            })?;

        let arrow_schema = Arc::new(current_schema.clone().try_into().map_err(|e| {
            crate::error::Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                format!("Can't convert iceberg schema to arrow schema: {}", e),
            )
        })?);

        let partition_spec = table_metadata
            .partition_specs
            .get(table_metadata.default_spec_id as usize)
            .ok_or(crate::error::Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                "Can't find default partition spec",
            ))?;

        let location_generator = location_generator::DataFileLocationGenerator::try_new(
            &table_metadata,
            partition_id,
            task_id,
            suffix,
        )?;

        if partition_spec.is_unpartitioned() {
            Ok(Self::Unpartitioned(
                UnpartitionedWriter::try_new(
                    arrow_schema,
                    table_metadata.location,
                    location_generator,
                    operator,
                    table_config,
                )
                .await?,
            ))
        } else {
            let partition_type =
                Any::Struct(partition_spec.partition_type(&current_schema)?.into());
            Ok(Self::Partitioned(PartitionedWriter::new(
                arrow_schema,
                table_metadata.location,
                location_generator,
                partition_spec,
                partition_type,
                operator,
                table_config,
            )?))
        }
    }

    /// Write a record batch.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            TaskWriter::Unpartitioned(writer) => writer.write(batch).await,
            TaskWriter::Partitioned(writer) => writer.write(batch).await,
        }
    }

    /// Close the writer and return the data files.
    pub async fn close(self) -> Result<Vec<DataFile>> {
        match self {
            TaskWriter::Unpartitioned(writer) => writer.close().await,
            TaskWriter::Partitioned(writer) => writer.close().await,
        }
    }
}

/// Unpartitioned task writer
pub struct UnpartitionedWriter {
    data_file_writer: DataFileWriter,
}

impl UnpartitionedWriter {
    /// Create a new `TaskWriter`.
    pub async fn try_new(
        schema: ArrowSchemaRef,
        table_location: String,
        location_generator: DataFileLocationGenerator,
        operator: Operator,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        Ok(Self {
            data_file_writer: DataFileWriter::try_new(
                operator,
                table_location,
                location_generator.into(),
                schema,
                table_config,
            )
            .await?,
        })
    }

    /// Write a record batch using data file writer.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.data_file_writer.write(batch.clone()).await
    }

    /// Complete the write and return the data files.
    /// It didn't mean the write take effect in table.
    /// To make the write take effect, you should commit the data file using transaction api.
    ///
    /// # Note
    ///
    /// For unpartitioned table, the key of the result map is default partition key.
    pub async fn close(self) -> Result<Vec<DataFile>> {
        self.data_file_writer.close().await
    }
}

/// Partition task writer
pub struct PartitionedWriter {
    schema: ArrowSchemaRef,
    operator: Operator,
    table_location: String,
    location_generator: Arc<DataFileLocationGenerator>,
    /// Paritition fields used to compute:
    /// - Vec<usize>: index vector of the source column
    /// - String: partition field name
    /// - BoxedTransformFunction: transform function
    partition_fields: Vec<(Vec<usize>, String, BoxedTransformFunction)>,
    /// Partition value type
    partition_type: Any,
    table_config: TableConfigRef,
    groups: HashMap<OwnedRow, PartitionGroup>,
}

/// Each partition group corresponds to partition value.
struct PartitionGroup {
    /// The parition_array have only one element which it's the partition value.
    pub partition_array: StructArray,
    /// Row with same partition value will be written to the same data file writer.
    pub writer: DataFileWriter,
}

impl PartitionedWriter {
    /// Fetch the column index vector of the column id (We store it in Field of arrow as dict id).
    /// e.g.
    /// struct<struct<x:1,y:2>,z:3>
    /// for source column id 2,
    /// you will get the source column index vector [1,0]
    fn fetch_column_index(fields: &Fields, index_vec: &mut Vec<usize>, col_id: i64) {
        for (pos, field) in fields.iter().enumerate() {
            let id: i64 = field
                .metadata()
                .get("column_id")
                .expect("column_id must be set")
                .parse()
                .expect("column_id must can be parse as i64");
            if col_id == id {
                index_vec.push(pos);
                return;
            }
            if let DataType::Struct(inner) = field.data_type() {
                Self::fetch_column_index(inner, index_vec, col_id);
                if !index_vec.is_empty() {
                    index_vec.push(pos);
                    return;
                }
            }
        }
    }

    fn get_column_by_index_vec(batch: &RecordBatch, index_vec: &[usize]) -> ArrayRef {
        let mut rev_iterator = index_vec.iter().rev();
        let mut array = batch.column(*rev_iterator.next().unwrap()).clone();
        for idx in rev_iterator {
            array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(*idx)
                .clone();
        }
        array
    }

    /// Create a new `PartitionedWriter`.
    pub fn new(
        schema: ArrowSchemaRef,
        table_location: String,
        location_generator: DataFileLocationGenerator,
        partition_spec: &PartitionSpec,
        partition_type: Any,
        operator: Operator,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        let partition_fields = partition_spec
            .fields
            .iter()
            .map(|field| {
                let transform = create_transform_function(&field.transform)?;
                let mut index_vec = vec![];
                Self::fetch_column_index(
                    schema.fields(),
                    &mut index_vec,
                    field.source_column_id as i64,
                );
                if index_vec.is_empty() {
                    return Err(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("Can't find source column id: {}", field.source_column_id),
                    ));
                }
                Ok((index_vec, field.name.clone(), transform))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            operator,
            table_location,
            location_generator: location_generator.into(),
            partition_fields,
            partition_type,
            table_config,
            groups: HashMap::new(),
        })
    }

    /// This function do two things:
    /// 1. Partition the batch by partition spec.
    /// 2. Create the partition group if not exist.
    ///
    /// # TODO
    /// Mix up two thing in one function may be not a good idea.
    /// The reason do that is we need to use the partition info in step 1 when we create the
    /// partition group. To avoid data copy, we do that in one function. We may need to refactor
    /// this function in the future. Also the name of this function may not be a good choice.
    async fn split_by_partition(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<HashMap<OwnedRow, RecordBatch>> {
        let value_array = Arc::new(StructArray::from(
            self.partition_fields
                .iter()
                .map(|(index_vec, field_name, transform)| {
                    let array = Self::get_column_by_index_vec(batch, index_vec);
                    let array = transform.transform(array)?;
                    let field = Arc::new(Field::new(
                        field_name.clone(),
                        array.data_type().clone(),
                        true,
                    ));
                    Ok((field, array))
                })
                .collect::<Result<Vec<_>>>()?,
        ));

        let mut row_converter = RowConverter::new(vec![SortField::new(
            value_array.data_type().clone(),
        )])
        .map_err(|e| crate::error::Error::new(crate::ErrorKind::ArrowError, format!("{}", e)))?;
        let rows = row_converter
            .convert_columns(&[value_array.clone()])
            .map_err(|e| {
                crate::error::Error::new(crate::ErrorKind::ArrowError, format!("{}", e))
            })?;

        // Group the batch by row value.
        let mut group_ids = HashMap::new();
        rows.into_iter().enumerate().for_each(|(row_id, row)| {
            group_ids.entry(row.owned()).or_insert(vec![]).push(row_id);
        });

        // Create the partition group if not exist.
        for (row, row_ids) in group_ids.iter() {
            let row_id = row_ids[0];
            if let Entry::Vacant(entry) = self.groups.entry(row.clone()) {
                let partition_array = value_array.slice(row_id, 1);
                let writer = DataFileWriter::try_new(
                    self.operator.clone(),
                    self.table_location.clone(),
                    self.location_generator.clone(),
                    self.schema.clone(),
                    self.table_config.clone(),
                )
                .await?;
                entry.insert(PartitionGroup {
                    partition_array,
                    writer,
                });
            }
        }

        // Partition the batch with same partition partition_values
        let mut partition_batches = HashMap::new();
        for (row, row_ids) in group_ids.into_iter() {
            // generate the bool filter array from column_ids
            let filter_array: BooleanArray = {
                let mut filter = vec![false; batch.num_rows()];
                row_ids.into_iter().for_each(|row_id| {
                    filter[row_id] = true;
                });
                filter.into()
            };

            // filter the RecordBatch
            partition_batches.insert(
                row.clone(),
                filter_record_batch(batch, &filter_array)
                    .expect("We should guarantee the filter array is valid"),
            );
        }

        Ok(partition_batches)
    }

    /// Write a record batch using data file writer.
    /// It will split the batch by partition spec and write the batch to different data file writer.
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let split_batch = self.split_by_partition(batch).await?;

        for (partition_values, batch) in split_batch.into_iter() {
            self.groups
                .get_mut(&partition_values)
                .expect(
                    "self.split_by_partition should create the new partition group if not exists",
                )
                .writer
                .write(batch)
                .await?;
        }

        Ok(())
    }

    /// Complete the write and return the data files.
    pub async fn close(self) -> Result<Vec<DataFile>> {
        let mut res = vec![];
        for (
            _,
            PartitionGroup {
                partition_array,
                writer,
            },
        ) in self.groups.into_iter()
        {
            // Convert the partition array to partition value.
            let value = {
                // # NOTE
                // We should guarantee that partition array type is consistent with partition_type.
                // Other wise, struct_to_anyvalue_array_with_type will return the error.
                let mut array = struct_to_anyvalue_array_with_type(
                    &partition_array,
                    self.partition_type.clone(),
                )?;
                // We guarantee the partition array only has one value.
                assert!(array.len() == 1);
                let value = array
                    .pop()
                    .unwrap()
                    .expect("Partition Value is alway valid");
                // cast to StructValue
                if let AnyValue::Struct(v) = value {
                    v
                } else {
                    unreachable!("Partition value should be struct value")
                }
            };
            let mut data_files = writer.close().await?;
            // Update the partition value in data file.
            data_files.iter_mut().for_each(|data_file| {
                data_file.partition = value.clone();
            });
            res.extend(data_files);
        }
        Ok(res)
    }
}
