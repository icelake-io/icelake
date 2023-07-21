use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;

use crate::error::Result;
use futures::StreamExt;
use opendal::layers::LoggingLayer;
use opendal::services::Fs;
use opendal::Operator;

use crate::io::task_writer::TaskWriter;
use crate::types::DataFile;
use crate::{types, Error};

const META_PATH: &str = "metadata";

/// Table is the main entry point for the IceLake.
pub struct Table {
    op: Operator,

    table_metadata: HashMap<i64, types::TableMetadata>,

    /// `0` means the version is not loaded yet.
    ///
    /// We use table's `last-updated-ms` to represent the version.
    current_version: i64,
    current_location: Option<String>,

    task_id: AtomicUsize,
}

impl Table {
    /// Create a new table via the given operator.
    pub fn new(op: Operator) -> Self {
        Self {
            op,

            table_metadata: HashMap::new(),

            current_version: 0,
            current_location: None,
            task_id: AtomicUsize::new(0),
        }
    }

    /// Load metadata and manifest from storage.
    async fn load(&mut self) -> Result<()> {
        let path = if self.is_version_hint_exist().await? {
            let version_hint = self.read_version_hint().await?;
            format!("metadata/v{}.metadata.json", version_hint)
        } else {
            let files = self.list_table_metadata_paths().await?;

            files.into_iter().last().ok_or(Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                "no table metadata found",
            ))?
        };

        let metadata = self.read_table_metadata(&path).await?;
        // TODO: check if the metadata is out of date.
        if metadata.last_updated_ms == 0 {
            return Err(Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                "Timestamp when the table was last updated is invalid",
            ));
        }
        self.current_version = metadata.last_updated_ms;
        self.current_location = Some(metadata.location.clone());
        self.table_metadata
            .insert(metadata.last_updated_ms, metadata);

        Ok(())
    }

    /// Open an iceberg table by uri
    pub async fn open(uri: &str) -> Result<Table> {
        // Todo(xudong): inferring storage types by uri
        let mut builder = Fs::default();
        builder.root(uri);

        let op = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();

        let mut table = Table::new(op);
        table.load().await?;
        Ok(table)
    }

    /// Open an iceberg table by operator
    pub async fn open_with_op(op: Operator) -> Result<Table> {
        let mut table = Table::new(op);
        table.load().await?;
        Ok(table)
    }

    /// Fetch current table metadata.
    pub fn current_table_metadata(&self) -> &types::TableMetadata {
        assert!(
            self.current_version != 0,
            "table current version must be valid"
        );

        self.table_metadata
            .get(&self.current_version)
            .expect("table metadata of current version must be exist")
    }

    /// # TODO
    ///
    /// we will have better API to play with snapshots and partitions.
    ///
    /// Currently, we just return all data files of the current version.
    pub async fn current_data_files(&self) -> Result<Vec<types::DataFile>> {
        assert!(
            self.current_version != 0,
            "table current version must be valid"
        );

        let meta = self
            .table_metadata
            .get(&self.current_version)
            .expect("table metadata of current version must be exist");

        let current_snapshot_id = meta.current_snapshot_id.ok_or(Error::new(
            crate::ErrorKind::IcebergDataInvalid,
            "current snapshot id is empty",
        ))?;
        let current_snapshot = meta
            .snapshots
            .as_ref()
            .ok_or(Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                "snapshots is empty",
            ))?
            .iter()
            .find(|v| v.snapshot_id == current_snapshot_id)
            .ok_or(Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                format!("snapshot with id {} is not found", current_snapshot_id),
            ))?;

        let manifest_list_path = self.rel_path(&current_snapshot.manifest_list)?;
        let manifest_list_content = self.op.read(&manifest_list_path).await?;
        let manifest_list = types::parse_manifest_list(&manifest_list_content)?;

        let mut data_files: Vec<DataFile> = Vec::new();
        for manifest_list_entry in manifest_list.entries {
            let manifest_path = self.rel_path(&manifest_list_entry.manifest_path)?;
            let manifest_content = self.op.read(&manifest_path).await?;
            let manifest = types::parse_manifest_file(&manifest_content)?;
            data_files.extend(manifest.entries.into_iter().map(|v| v.data_file));
        }

        Ok(data_files)
    }

    /// Get the relpath related to the base of table location.
    pub fn rel_path(&self, path: &str) -> Result<String> {
        let location = self.current_location.as_ref().ok_or(Error::new(
            crate::ErrorKind::IcebergDataInvalid,
            "table location is empty, maybe it's not loaded?",
        ))?;

        path.strip_prefix(location)
            .ok_or(Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                format!(
                    "path {} is not starts with table location {}",
                    path, location
                ),
            ))
            .map(|v| v.to_string())
    }

    /// Check if version hint file exist.
    async fn is_version_hint_exist(&self) -> Result<bool> {
        self.op
            .is_exist("metadata/version-hint.text")
            .await
            .map_err(|e| {
                Error::new(
                    crate::ErrorKind::IcebergDataInvalid,
                    format!("check if version hint exist failed: {}", e),
                )
            })
    }

    /// Read version hint of table.
    async fn read_version_hint(&self) -> Result<i32> {
        let content = self.op.read("metadata/version-hint.text").await?;
        let version_hint = String::from_utf8(content).map_err(|err| {
            Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                format!("Fail to covert version_hint from utf8 to string: {}", err),
            )
        })?;

        version_hint.parse().map_err(|e| {
            Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                format!("parse version hint failed: {}", e),
            )
        })
    }

    /// Read table metadata of the given version.
    async fn read_table_metadata(&self, path: &str) -> Result<types::TableMetadata> {
        let content = self.op.read(path).await?;

        let metadata = types::parse_table_metadata(&content)?;

        Ok(metadata)
    }

    /// List all paths of table metadata files.
    ///
    /// The returned paths are sorted by name.
    ///
    /// TODO: we can imporve this by only fetch the latest metadata.
    async fn list_table_metadata_paths(&self) -> Result<Vec<String>> {
        let mut lister = self.op.list("metadata/").await.map_err(|err| {
            Error::new(
                crate::ErrorKind::Unexpected,
                format!("list metadata failed: {}", err),
            )
        })?;

        let mut paths = vec![];

        while let Some(entry) = lister.next().await {
            let entry = entry.map_err(|err| {
                Error::new(
                    crate::ErrorKind::Unexpected,
                    format!("list metadata entry failed: {}", err),
                )
            })?;

            // Only push into paths if the entry is a metadata file.
            if entry.path().ends_with(".metadata.json") {
                paths.push(entry.path().to_string());
            }
        }

        // Make the returned paths sorted by name.
        paths.sort();

        Ok(paths)
    }

    /// Return a task writer used to write data into table.
    pub async fn task_writer(&self) -> Result<TaskWriter> {
        let task_id = self
            .task_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let task_writer = TaskWriter::try_new(
            self.current_table_metadata().clone(),
            self.op.clone(),
            0,
            task_id,
            None,
        )
        .await?;
        Ok(task_writer)
    }

    /// Returns path of metadata file relative to the table root path.
    #[inline]
    pub fn metadata_file_path(filename: impl Into<String>) -> String {
        format!("{}/{}", META_PATH, filename.into())
    }

    pub(crate) fn operator(&self) -> Operator {
        self.op.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use opendal::layers::LoggingLayer;
    use opendal::services::Fs;

    use super::*;

    #[tokio::test]
    async fn test_table_version_hint() -> Result<()> {
        let path = format!(
            "{}/testdata/simple_table",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let mut builder = Fs::default();
        builder.root(&path);

        let op = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();

        let table = Table::new(op);

        let version_hint = table.read_version_hint().await?;

        assert_eq!(version_hint, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_table_read_table_metadata() -> Result<()> {
        let path = format!(
            "{}/testdata/simple_table",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let mut builder = Fs::default();
        builder.root(&path);

        let op = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();

        let table = Table::new(op);

        let table_v1 = table
            .read_table_metadata("metadata/v1.metadata.json")
            .await?;

        assert_eq!(table_v1.format_version, types::TableFormatVersion::V1);
        assert_eq!(table_v1.last_updated_ms, 1686911664577);

        let table_v2 = table
            .read_table_metadata("metadata/v2.metadata.json")
            .await?;
        assert_eq!(table_v2.format_version, types::TableFormatVersion::V1);
        assert_eq!(table_v2.last_updated_ms, 1686911671713);

        Ok(())
    }

    #[tokio::test]
    async fn test_table_load() -> Result<()> {
        let path = format!(
            "{}/testdata/simple_table",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let mut builder = Fs::default();
        builder.root(&path);

        let op = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();

        let mut table = Table::new(op);
        table.load().await?;

        let table_metadata = table.current_table_metadata();
        assert_eq!(table_metadata.format_version, types::TableFormatVersion::V1);
        assert_eq!(table_metadata.last_updated_ms, 1686911671713);

        Ok(())
    }

    #[tokio::test]
    async fn test_table_load_without_version_hint() -> Result<()> {
        let path = format!(
            "{}/testdata/no_hint_table",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let mut builder = Fs::default();
        builder.root(&path);

        let op = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();

        let mut table = Table::new(op);
        table.load().await?;

        let table_metadata = table.current_table_metadata();
        assert_eq!(table_metadata.format_version, types::TableFormatVersion::V1);
        assert_eq!(table_metadata.last_updated_ms, 1672981042425);
        assert_eq!(
            table_metadata.location,
            "s3://testbucket/iceberg_data/iceberg_ctl/iceberg_db/iceberg_tbl"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_table_current_data_files() -> Result<()> {
        let path = format!(
            "{}/testdata/simple_table",
            env::current_dir()
                .expect("current_dir must exist")
                .to_string_lossy()
        );

        let mut builder = Fs::default();
        builder.root(&path);

        let op = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .finish();

        let mut table = Table::new(op);
        table.load().await?;

        let data_files = table.current_data_files().await?;
        assert_eq!(data_files.len(), 3);
        assert_eq!(data_files[0].file_path, "/opt/bitnami/spark/warehouse/db/table/data/00000-0-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");
        assert_eq!(data_files[1].file_path, "/opt/bitnami/spark/warehouse/db/table/data/00001-1-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");
        assert_eq!(data_files[2].file_path, "/opt/bitnami/spark/warehouse/db/table/data/00002-2-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");

        Ok(())
    }
}
