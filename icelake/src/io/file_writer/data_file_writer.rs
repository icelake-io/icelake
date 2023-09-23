//! A module provide `DataFileWriter`.
use std::sync::Arc;

use crate::config::TableConfigRef;
use crate::io::location_generator::FileLocationGenerator;
use crate::types::DataFileBuilder;
use crate::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use opendal::Operator;

use super::rolling_writer::RollingWriter;

/// A writer capable of splitting incoming data into multiple files within one spec/partition based on the target file size.
/// When complete, it will return a list of `DataFile`.
///
/// # NOTE
/// This writer will not gurantee the writen data is within one spec/partition. It is the caller's responsibility to make sure the data is within one spec/partition.
pub struct DataFileWriter {
    inner_writer: RollingWriter,
}

impl DataFileWriter {
    /// Create a new `DataFileWriter`.
    pub async fn try_new(
        operator: Operator,
        table_location: String,
        location_generator: Arc<FileLocationGenerator>,
        arrow_schema: SchemaRef,
        table_config: TableConfigRef,
    ) -> Result<Self> {
        Ok(Self {
            inner_writer: RollingWriter::try_new(
                operator,
                table_location,
                location_generator,
                arrow_schema,
                table_config,
            )
            .await?,
        })
    }

    /// Write a record batch.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner_writer.write(batch).await?;
        Ok(())
    }

    /// Complte the write and return the list of `DataFileBuilder` as result.
    pub async fn close(self) -> Result<Vec<DataFileBuilder>> {
        Ok(self
            .inner_writer
            .close()
            .await?
            .into_iter()
            .map(|builder| builder.with_content(crate::types::DataContentType::Data))
            .collect())
    }

    /// Return the current file name.
    pub fn current_file(&self) -> String {
        self.inner_writer.current_file()
    }

    /// Return the current row number.
    pub fn current_row(&self) -> usize {
        self.inner_writer.current_row()
    }
}

#[cfg(test)]
mod test {
    use std::{env, fs, sync::Arc};

    use arrow_array::RecordBatch;
    use arrow_array::{ArrayRef, Int64Array};
    use bytes::Bytes;
    use opendal::{services::Memory, Operator};

    use anyhow::Result;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use crate::config::TableConfig;
    use crate::io::file_writer::data_file_writer;
    use crate::io::location_generator::FileLocationGenerator;
    use crate::types::parse_table_metadata;

    #[tokio::test]
    async fn tets_data_file_writer() -> Result<()> {
        let mut builder = Memory::default();
        builder.root("/tmp/table");
        let op = Operator::new(builder)?.finish();

        let location_generator = {
            let mut metadata = {
                let path = format!(
                    "{}/../testdata/simple_table/metadata/v1.metadata.json",
                    env!("CARGO_MANIFEST_DIR")
                );

                let bs = fs::read(path).expect("read_file must succeed");

                parse_table_metadata(&bs).expect("parse_table_metadata v1 must succeed")
            };
            metadata.location = "/tmp/table".to_string();

            FileLocationGenerator::try_new_for_data_file(&metadata, 0, 0, None)?
        };

        let data = (0..1024 * 1024).collect::<Vec<_>>();
        let col = Arc::new(Int64Array::from_iter_values(data)) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col.clone())]).unwrap();

        let mut writer = data_file_writer::DataFileWriter::try_new(
            op.clone(),
            "/tmp/table".to_string(),
            location_generator.into(),
            to_write.schema(),
            Arc::new(TableConfig::default()),
        )
        .await?;

        writer.write(to_write.clone()).await?;
        writer.write(to_write.clone()).await?;
        writer.write(to_write.clone()).await?;
        let data_files = writer
            .close()
            .await?
            .into_iter()
            .map(|f| f.build())
            .collect::<Vec<_>>();

        let mut row_num = 0;
        for data_file in data_files {
            let res = op
                .read(data_file.file_path.strip_prefix("/tmp/table").unwrap())
                .await?;
            let res = Bytes::from(res);
            let reader = ParquetRecordBatchReaderBuilder::try_new(res)
                .unwrap()
                .build()
                .unwrap();
            reader.into_iter().for_each(|batch| {
                row_num += batch.unwrap().num_rows();
            });
        }
        assert_eq!(row_num, 1024 * 1024 * 3);

        Ok(())
    }
}
