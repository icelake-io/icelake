//! location_generator is used to generate a related file location for
//! the writer.

use std::str::FromStr;
use std::sync::atomic::AtomicUsize;

use crate::types::{DataFileFormat, TableMetadata};
use crate::{Error, ErrorKind, Result};
use uuid::Uuid;

/// TODO: move to table_properties.rs
const DEFAULT_FILE_FORMAT: &str = "write.format.default";
const DEFAULT_FILE_FORMAT_DEFAULT: &str = "parquet";
const WRITE_DATA_LOCATION: &str = "write.data.path";
const WRITE_FOLDER_STORAGE_LOCATION: &str = "write.folder-storage.path";

/// FileLocationGenerator will generate a file location for the writer.
pub struct FileLocationGenerator {
    file_count: AtomicUsize,
    partition_id: usize,
    task_id: usize,
    operation_id: String,
    file_format: DataFileFormat,
    suffix: Option<String>,
    is_delete: bool,
    /// The data file relpath related to the base of table location.
    data_rel_location: String,
}

impl FileLocationGenerator {
    /// Create a file location generator for data file.
    pub fn try_new_for_data_file(
        table_metatdata: &TableMetadata,
        partition_id: usize,
        task_id: usize,
        suffix: Option<String>,
    ) -> Result<Self> {
        Self::try_new(table_metatdata,partition_id,task_id,suffix,false)
    }
    
    /// Create a file location generator for delete file.
    pub fn try_new_for_delete_file(
        table_metatdata: &TableMetadata,
        partition_id: usize,
        task_id: usize,
        suffix: Option<String>,
    ) -> Result<Self> {
        Self::try_new(table_metatdata,partition_id,task_id,suffix,true)
    }
    
    fn try_new(
        table_metatdata: &TableMetadata,
        partition_id: usize,
        task_id: usize,
        suffix: Option<String>,
        is_delete: bool,
    ) -> Result<Self> {
        let operation_id = Uuid::new_v4().to_string();

        let file_format = DataFileFormat::from_str(
            &table_metatdata
                .properties
                .as_ref()
                .and_then(|prop| prop.get(DEFAULT_FILE_FORMAT))
                .cloned()
                .unwrap_or(DEFAULT_FILE_FORMAT_DEFAULT.to_string()),
        )
        .unwrap_or(DataFileFormat::Parquet);

        let data_rel_location = {
            let base_location = &table_metatdata.location;
            let data_location = table_metatdata.properties.as_ref().and_then(|prop| {
                prop.get(WRITE_DATA_LOCATION)
                    .or(prop.get(WRITE_FOLDER_STORAGE_LOCATION))
                    .cloned()
            });
            if let Some(data_location) = data_location {
                data_location
                    .strip_prefix(base_location)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::IcebergDataInvalid,
                            format!(
                                "data location {} is not a subpath of table location {}",
                                data_location, base_location
                            ),
                        )
                    })?
                    .to_string()
            } else {
                "data".to_string()
            }
        };

        Ok(Self {
            file_count: AtomicUsize::new(0),
            partition_id,
            task_id,
            operation_id,
            file_format,
            suffix,
            data_rel_location,
            is_delete,
        })
    }

    /// Generate a related file location for the writer.
    ///
    /// # TODO
    ///
    /// - Only support to generate file name for unpartitioned write.
    /// - May need a way(e.g LocationProvider) to generate custom file name in future. It's useful in some case: <https://github.com/apache/iceberg/issues/1911>.
    pub fn generate_name(&self) -> String {
        let suffix = if let Some(suffix) = &self.suffix {
            format!("-{}", suffix)
        } else {
            "".to_string()
        };

        let mut file_name = format!(
            "{:05}-{}-{}-{:05}{}",
            self.partition_id,
            self.task_id,
            self.operation_id,
            self.file_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            suffix,
        );

        if self.is_delete {
            file_name.push_str("-delete");
        }

        let extension = self.file_format.to_string();
        let file_name = if file_name.to_ascii_lowercase().ends_with(&extension) {
            file_name
        } else {
            format!("{}.{}", file_name, extension)
        };

        format!("{}/{}", self.data_rel_location, file_name)
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, env, fs};

    use anyhow::Result;

    use crate::{
        io::location_generator::{
            FileLocationGenerator, WRITE_DATA_LOCATION, WRITE_FOLDER_STORAGE_LOCATION,
        },
        types::parse_table_metadata,
    };

    #[tokio::test]
    async fn test_location_generator_default() -> Result<()> {
        let metadata = {
            let path = format!(
                "{}/../testdata/simple_table/metadata/v1.metadata.json",
                env!("CARGO_MANIFEST_DIR")
            );

            let bs = fs::read(path).expect("read_file must succeed");

            parse_table_metadata(&bs).expect("parse_table_metadata v1 must succeed")
        };

        let generator = FileLocationGenerator::try_new_for_data_file(&metadata, 0, 0, None)?;
        let name = generator.generate_name();
        assert!(name.starts_with("data/"));

        Ok(())
    }

    #[tokio::test]
    async fn test_location_generator_with_write_data_location() -> Result<()> {
        let mut metadata = {
            let path = format!(
                "{}/../testdata/simple_table/metadata/v1.metadata.json",
                env!("CARGO_MANIFEST_DIR")
            );

            let bs = fs::read(path).expect("read_file must succeed");

            parse_table_metadata(&bs).expect("parse_table_metadata v1 must succeed")
        };

        // Mock metadata to test
        metadata.location = "/tmp/table".to_string();
        let mock_properties = {
            let mut map = HashMap::new();
            map.insert(
                WRITE_DATA_LOCATION.to_string(),
                "/tmp/table/mock".to_string(),
            );
            map.insert(
                WRITE_FOLDER_STORAGE_LOCATION.to_string(),
                "/tmp/table/mock_storage".to_string(),
            );
            map
        };
        metadata.properties = Some(mock_properties);

        let generator = FileLocationGenerator::try_new_for_data_file(&metadata, 0, 0, None)?;
        let name = generator.generate_name();
        assert!(name.starts_with("/mock"));
        Ok(())
    }

    #[tokio::test]
    async fn test_location_generator_with_write_folder_storage_location() -> Result<()> {
        let mut metadata = {
            let path = format!(
                "{}/../testdata/simple_table/metadata/v1.metadata.json",
                env!("CARGO_MANIFEST_DIR")
            );

            let bs = fs::read(path).expect("read_file must succeed");

            parse_table_metadata(&bs).expect("parse_table_metadata v1 must succeed")
        };

        // Mock metadata to test
        metadata.location = "/tmp/table".to_string();
        let mock_properties = {
            let mut map = HashMap::new();
            map.insert(
                WRITE_FOLDER_STORAGE_LOCATION.to_string(),
                "/tmp/table/mock_storage".to_string(),
            );
            map
        };
        metadata.properties = Some(mock_properties);

        let generator = FileLocationGenerator::try_new_for_data_file(&metadata, 0, 0, None)?;
        let name = generator.generate_name();
        assert!(name.starts_with("/mock_storage"));
        Ok(())
    }
}
