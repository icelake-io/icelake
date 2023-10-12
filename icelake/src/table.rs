use std::collections::HashMap;
use std::fmt::Display;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::catalog::CatalogRef;
use crate::error::Result;
use crate::io::writer_builder::{new_writer_builder, WriterBuilder};
use crate::io::{EmptyLayer, TableScanBuilder};
use opendal::Operator;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::config::{TableConfig, TableConfigRef};
use crate::types::{Any, DataFile, PartitionSplitter, Schema, Snapshot, TableMetadata};
use crate::{types, Error, ErrorKind};

pub(crate) const META_ROOT_PATH: &str = "metadata";
pub(crate) const METADATA_FILE_EXTENSION: &str = ".metadata.json";
pub(crate) const VERSION_HINT_FILENAME: &str = "version-hint.text";
pub(crate) const VERSIONED_TABLE_METADATA_FILE_PATTERN: &str = r"v([0-9]+).metadata.json";

/// Namespace of tables
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Namespace {
    /// Levels in namespace.
    pub levels: Vec<String>,
}

impl Namespace {
    /// Creates namespace
    pub fn new(levels: impl IntoIterator<Item = impl ToString>) -> Self {
        Self {
            levels: levels.into_iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.levels.join("."))
    }
}

/// Full qualified name of table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableIdentifier {
    /// Namespace
    pub namespace: Namespace,
    /// Table name
    pub name: String,
}

impl TableIdentifier {
    /// Creates a full qualified table identifier from a list of names.
    pub fn new(names: impl IntoIterator<Item = impl ToString>) -> Result<Self> {
        let mut names: Vec<String> = names.into_iter().map(|s| s.to_string()).collect();
        if names.is_empty() {
            return Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                "Table identifier can't be empty!",
            ));
        }

        let table_name = names.pop().unwrap();
        let ns = Namespace { levels: names };

        Ok(Self {
            namespace: ns,
            name: table_name,
        })
    }
}

impl Display for TableIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

/// Table is the main entry point for the IceLake.
pub struct Table {
    op: Operator,
    table_name: TableIdentifier,

    table_metadata: HashMap<String, types::TableMetadata>,

    /// `None` means the version is not loaded yet.
    ///
    /// We use table's current metadata file location to represent the version.
    current_version: Option<String>,
    current_location: String,
    /// It's different from `current_version` in that it's the `v[version number]` in metadata file.
    current_table_version: i64,

    task_id: AtomicUsize,

    table_config: TableConfigRef,

    // Catalog res
    catalog: CatalogRef,
}

/// Table builder
pub struct TableBuilder {
    op: Operator,
    name: TableIdentifier,
    catalog: CatalogRef,
    metadatas: HashMap<String, TableMetadata>,
    current_version: Option<String>,
    current_table_version: Option<i64>,
    table_config: Option<TableConfigRef>,
}

impl TableBuilder {
    /// Add metadata
    pub fn add_metadata(mut self, location: String, metadata: TableMetadata) -> Self {
        self.metadatas.insert(location, metadata);
        self
    }

    /// Set config.
    pub fn with_config(mut self, config: TableConfigRef) -> Self {
        self.table_config = Some(config);
        self
    }

    /// Set current version.
    pub fn with_current_version(mut self, location: String) -> Self {
        self.current_version = Some(location);
        self
    }

    /// Set current table version.
    pub fn with_current_table_version(mut self, version: i64) -> Self {
        self.current_table_version = Some(version);
        self
    }

    /// Build table
    pub fn build(self) -> Result<Table> {
        let table_metadata: HashMap<String, TableMetadata> = self.metadatas;

        let current_version = match self.current_version {
            Some(v) => v,
            None => table_metadata
                .iter()
                .max_by_key(|v| v.1.last_sequence_number)
                .map(|v| v.0.clone())
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        "Table metadata must not be empty",
                    )
                })?,
        };

        let current_location = table_metadata
            .get(&current_version)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!(
                        "Table metadata of version {} must be exist",
                        current_version
                    ),
                )
            })?
            .location
            .clone();

        Ok(Table {
            op: self.op,
            table_name: self.name,
            table_metadata,
            current_version: Some(current_version),
            current_location,
            current_table_version: self.current_table_version.unwrap_or(0),
            task_id: AtomicUsize::new(0),
            table_config: self
                .table_config
                .unwrap_or_else(|| Arc::new(TableConfig::default())),
            catalog: self.catalog,
        })
    }
}

impl Table {
    /// Creates a table from catalog.
    pub fn builder_from_catalog(
        op: Operator,
        catalog: CatalogRef,
        metadata: TableMetadata,
        metadata_location: String,
        table_name: TableIdentifier,
    ) -> TableBuilder {
        let mut metadatas = HashMap::new();
        metadatas.insert(metadata_location, metadata);
        TableBuilder {
            op,
            name: table_name,
            catalog,
            metadatas,
            current_version: None,
            current_table_version: None,
            table_config: None,
        }
    }

    /// Returns table name.
    pub fn table_name(&self) -> &TableIdentifier {
        &self.table_name
    }

    /// Returns catalog that manages this table.
    pub(crate) fn catalog(&self) -> CatalogRef {
        self.catalog.clone()
    }

    /// Fetch current table metadata.
    pub fn current_table_metadata(&self) -> &types::TableMetadata {
        let current_version = self.current_version.as_ref().expect("table must be loaded");

        self.table_metadata
            .get(current_version)
            .expect("table metadata of current version must be exist")
    }

    /// Fetch current table metadata location
    pub fn current_metadata_location(&self) -> &str {
        self.current_version.as_ref().expect("table must be loaded")
    }

    pub(crate) fn current_table_version(&self) -> i64 {
        self.current_table_version
    }

    /// # TODO
    ///
    /// we will have better API to play with snapshots and partitions.
    ///
    /// Currently, we just return all data files of the current version.
    pub async fn current_data_files(&self) -> Result<Vec<types::DataFile>> {
        let meta = self.current_table_metadata();
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
            .ok_or_else(|| {
                Error::new(
                    crate::ErrorKind::IcebergDataInvalid,
                    format!("snapshot with id {} is not found", current_snapshot_id),
                )
            })?;

        self.data_files_of_snapshot(current_snapshot).await
    }

    pub async fn data_files_of_snapshot(
        &self,
        snapshot: &Snapshot,
    ) -> Result<Vec<types::DataFile>> {
        let manifest_list_path = self.rel_path(&snapshot.manifest_list)?;
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
        let location = &self.current_location;

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

    /// Return current partition type.
    pub fn current_partition_type(&self) -> Result<Any> {
        let current_partition_spec = self.current_table_metadata().current_partition_spec()?;
        let current_schema = self.current_table_metadata().current_schema()?;
        Ok(Any::Struct(Arc::new(
            current_partition_spec.partition_type(current_schema)?,
        )))
    }

    pub fn partition_type_of(&self, partition_spec_id: i32, schema: &Schema) -> Result<Any> {
        let partition_spec = self
            .current_table_metadata()
            .partition_spec(partition_spec_id)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Partition spec id {} not found!", partition_spec_id),
                )
            })?;

        Ok(Any::Struct(Arc::new(
            partition_spec.partition_type(schema)?,
        )))
    }

    /// Return a PartitionSplitter used to split data files into partitions.
    /// None - Current partition is unpartitioned.
    pub fn partition_splitter(&self) -> Result<Option<PartitionSplitter>> {
        let current_partition_spec = self.current_table_metadata().current_partition_spec()?;
        if current_partition_spec.is_unpartitioned() {
            return Ok(None);
        }

        let current_schema = self.current_table_metadata().current_schema()?;
        let arrow_schema = Arc::new(current_schema.clone().try_into().map_err(|e| {
            crate::error::Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                format!("Can't convert iceberg schema to arrow schema: {}", e),
            )
        })?);

        let partition_type = Any::Struct(
            current_partition_spec
                .partition_type(current_schema)?
                .into(),
        );
        Ok(Some(PartitionSplitter::try_new(
            current_partition_spec,
            &arrow_schema,
            partition_type,
        )?))
    }

    /// Return `WriterBuilder` used to create kinds of writer.
    pub async fn writer_builder(&self) -> Result<WriterBuilder<EmptyLayer>> {
        let task_id = self
            .task_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        new_writer_builder(
            self.current_table_metadata().clone(),
            self.op.clone(),
            task_id,
            self.table_config.clone(),
        )
        .await
    }

    /// Returns path of metadata file relative to the table root path.
    #[inline]
    pub fn metadata_path(filename: impl Into<String>) -> String {
        format!("{}/{}", META_ROOT_PATH, filename.into())
    }

    /// Returns the metadata file path.
    pub fn metadata_file_path(metadata_version: i64) -> String {
        Table::metadata_path(format!("v{metadata_version}{METADATA_FILE_EXTENSION}"))
    }

    /// Returns absolute path in operator.
    pub fn absolution_path(op: &Operator, relation_location: &str) -> String {
        let op_info = op.info();
        format!(
            "{}://{}/{}/{}",
            op_info.scheme().into_static(),
            op_info.name(),
            op_info.root(),
            relation_location
        )
    }

    /// Returns the relative path to operator.
    pub fn relative_path(op: &Operator, absolute_path: &str) -> Result<String> {
        let url = Url::parse(absolute_path)?;
        let op_info = op.info();

        // TODO: We should check schema here, but how to guarantee schema compatible such as s3, s3a

        if url.host_str() != Some(op_info.name()) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "Host in {:?} not match with operator info {}",
                    url.host_str(),
                    op_info.name()
                ),
            ));
        }

        url.path()
            .strip_prefix(op_info.root())
            .ok_or_else(|| {
                Error::new(
                    crate::ErrorKind::IcebergDataInvalid,
                    format!(
                        "path {} is not starts with operator root {}",
                        absolute_path,
                        op_info.root()
                    ),
                )
            })
            .map(|s| s.to_string())
    }

    pub(crate) fn operator(&self) -> Operator {
        self.op.clone()
    }

    pub fn new_scan_builder(&self) -> TableScanBuilder {
        TableScanBuilder::default()
            .with_op(self.operator())
            .with_snapshot_id(self.current_table_metadata().current_snapshot_id.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use crate::catalog::IcebergStorageCatalog;

    use super::*;

    #[tokio::test]
    async fn test_table_version_hint() -> Result<()> {
        let path = format!("{}/../testdata/simple_table", env!("CARGO_MANIFEST_DIR"));

        let table = IcebergStorageCatalog::load_table(&path).await?;

        let version_hint = table.current_table_version();

        assert_eq!(version_hint, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_table_load() -> Result<()> {
        let path = format!("{}/../testdata/simple_table", env!("CARGO_MANIFEST_DIR"));

        let table = IcebergStorageCatalog::load_table(&path).await?;

        let table_metadata = table.current_table_metadata();
        assert_eq!(table_metadata.format_version, types::TableFormatVersion::V1);
        assert_eq!(table_metadata.last_updated_ms, 1686911671713);

        Ok(())
    }

    #[tokio::test]
    async fn test_table_load_without_version_hint() -> Result<()> {
        let path = format!("{}/../testdata/no_hint_table", env!("CARGO_MANIFEST_DIR"));

        let table = IcebergStorageCatalog::load_table(&path).await?;

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
        let path = format!("{}/../testdata/simple_table", env!("CARGO_MANIFEST_DIR"));

        let table = IcebergStorageCatalog::load_table(&path).await?;

        let data_files = table.current_data_files().await?;
        assert_eq!(data_files.len(), 3);
        assert_eq!(data_files[0].file_path, "/opt/bitnami/spark/warehouse/db/table/data/00000-0-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");
        assert_eq!(data_files[1].file_path, "/opt/bitnami/spark/warehouse/db/table/data/00001-1-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");
        assert_eq!(data_files[2].file_path, "/opt/bitnami/spark/warehouse/db/table/data/00002-2-b8982382-f016-467a-84e4-5e6bbe0ff19a-00001.parquet");

        Ok(())
    }
}
