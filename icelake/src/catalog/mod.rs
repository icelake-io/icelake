//! This module defines catalog api for icelake.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;

use enum_display::EnumDisplay;
use uuid::Uuid;

use crate::config::{TableConfig, TableConfigRef};
use crate::error::Result;
use crate::table::{Namespace, TableIdentifier};
use crate::types::{
    PartitionField, PartitionSpec, Schema, Snapshot, SnapshotReference, SnapshotReferenceType,
    SortOrder, TableMetadata,
};
use crate::Table;

mod rest;
pub use rest::*;
mod storage;
use crate::error::{Error, ErrorKind};
pub use storage::*;
mod io;
pub use io::*;
mod layer;
#[cfg(feature = "prometheus")]
pub mod prometheus;
pub use layer::*;

/// Reference to catalog.
pub type CatalogRef = Arc<dyn Catalog>;

/// Catalog definition.
#[async_trait]
pub trait Catalog: Send + Sync {
    /// Return catalog's name.
    fn name(&self) -> &str;

    /// List tables under namespace.
    async fn list_tables(self: Arc<Self>, _ns: &Namespace) -> Result<Vec<TableIdentifier>> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            format!("list_tables is not supported by {}", self.name()),
        ))
    }

    /// Creates a table.
    async fn create_table(
        self: Arc<Self>,
        _table_name: &TableIdentifier,
        _schema: &Schema,
        _spec: &PartitionSpec,
        _location: &str,
        _props: HashMap<String, String>,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            format!("create_table is not supported by {}", self.name()),
        ))
    }

    /// Check table exists.
    async fn table_exists(self: Arc<Self>, _table_name: &TableIdentifier) -> Result<bool> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            format!("table_exists is not supported by {}", self.name()),
        ))
    }

    /// Drop table.
    async fn drop_table(
        self: Arc<Self>,
        _table_name: &TableIdentifier,
        _purge: bool,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            format!("drop_table is not supported by {}", self.name()),
        ))
    }

    /// Rename table.
    async fn rename_table(
        self: Arc<Self>,
        _from: &TableIdentifier,
        _to: &TableIdentifier,
    ) -> Result<()> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            format!("rename_table is not supported by {}", self.name()),
        ))
    }

    /// Load table.
    async fn load_table(self: Arc<Self>, _table_name: &TableIdentifier) -> Result<Table> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            format!("load_table is not supported by {}", self.name()),
        ))
    }

    /// Invalidate table.
    async fn invalidate_table(self: Arc<Self>, _table_name: &TableIdentifier) -> Result<()> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            format!("invalidate_table is not supported by {}", self.name()),
        ))
    }

    /// Register a table using metadata file location.
    async fn register_table(
        self: Arc<Self>,
        _table_name: &TableIdentifier,
        _metadata_file_location: &str,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            format!("register_table is not supported by {}", self.name()),
        ))
    }

    /// Update table.
    async fn update_table(self: Arc<Self>, _udpate_table: &UpdateTable) -> Result<Table> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            format!("update_table is not supported by {}", self.name()),
        ))
    }
}

/// Update table requirments
#[derive(Debug, Clone, EnumDisplay)]
pub enum UpdateRquirement {
    /// Requirest table exists.
    AssertTableDoesNotExist,
    /// Requirest current table's uuid .
    AssertTableUUID(Uuid),
    /// Requirest current table branch's snapshot id.
    AssertRefSnapshotID {
        /// Branch name
        name: String,
        /// Snapshot id
        snapshot_id: i64,
    },
    /// Requirest current table's last assigned field id.
    AssertLastAssignedFieldId {
        /// Last assigned field id.
        last_assigned_field_id: i32,
    },
    /// Requirest current table's schema id.
    AssertCurrentSchemaID {
        /// Schema id
        schema_id: i32,
    },
    /// Requirest current table's last assigned partition id.
    AssertLastAssignedPartitionId {
        /// Partition id.
        last_assigned_partition_id: i32,
    },
    /// Requirest current table's default spec assigned partition id.
    AssertDefaultSpecID {
        /// Spec id
        spec_id: i32,
    },
    /// Requirest current table's default spec sort order id.
    AssertDefaultSortOrderID {
        /// Sort order id.
        sort_order_id: i32,
    },
}

impl UpdateRquirement {
    fn check(&self, table_metadata: &TableMetadata) -> bool {
        match self {
            UpdateRquirement::AssertTableDoesNotExist => false,
            UpdateRquirement::AssertTableUUID(uuid) => {
                table_metadata.table_uuid == uuid.to_string()
            }
            _ => todo!(),
        }
    }
}

/// Metadata updates.
#[derive(Debug, Clone, EnumDisplay)]
pub enum MetadataUpdate {
    /// Assign uuid.
    AssignUuid(Uuid),
    /// Upgrade format version.
    UpgradeFormatVersion(i32),
    /// Add schema
    AddSchema {
        /// New schema
        schema: Schema,
        /// Last column id
        last_column_id: i32,
    },
    /// Set current schema id.
    SetCurrentSchema {
        /// Schema id.
        schema_id: i32,
    },
    /// Add partition spec
    AddPartitionSpec {
        /// Spec id
        spec_id: i32,
        /// Partiton fields
        fields: Vec<PartitionField>,
    },
    /// Set default partiton spec.
    SetDefaultPartitonSpec {
        /// Partiton spec id
        spec_id: i32,
    },
    /// Add sort order.
    AddSortOrder {
        /// Sort order
        sort_order: SortOrder,
    },
    /// Set defaut sort order
    SetDefaultSortOrder {
        /// Sort order id
        sort_order_id: i32,
    },
    /// Add snapshot
    AddSnapshot {
        /// Snapshot
        snapshot: Snapshot,
    },
    /// Remove snapshot
    RemoveSnapshot {
        /// Snapshot id
        snapshot_id: i64,
    },
    /// Remove snapshot ref
    RemoveSnapshotRef {
        /// Ref name.
        ref_name: String,
    },
    /// Update snapshot reference.
    SetSnapshotRef {
        /// Branch name
        ref_name: String,
        /// Snapshot shot id.
        snapshot_id: i64,
        /// Type
        typ: SnapshotReferenceType,
        /// Number of snapshots to keep.
        min_snapshots_to_keep: Option<i32>,
        /// Max snapshot ages
        max_snapshot_ages: Option<i64>,
        /// Max ref ages
        max_ref_ages: Option<i64>,
    },
    /// Update table properties.
    SetProperties {
        /// Table properties.
        props: HashMap<String, String>,
    },
    /// Remove table properties.
    RemoveProperties {
        /// Keys to remove.
        removed: HashSet<String>,
    },
    /// Set table location
    SetLocation {
        /// Table Location
        location: String,
    },
}

impl MetadataUpdate {
    fn apply(&self, metadata: &mut TableMetadata) -> Result<()> {
        match self {
            MetadataUpdate::AddSnapshot { snapshot } => metadata.add_snapshot(snapshot.clone()),
            MetadataUpdate::SetSnapshotRef {
                ref_name,
                snapshot_id,
                typ,
                min_snapshots_to_keep,
                max_snapshot_ages,
                max_ref_ages,
            } => metadata.set_snapshot_ref(
                ref_name.as_str(),
                SnapshotReference {
                    snapshot_id: *snapshot_id,
                    typ: *typ,
                    min_snapshots_to_keep: *min_snapshots_to_keep,
                    max_snapshot_age_ms: *max_snapshot_ages,
                    max_ref_age_ms: *max_ref_ages,
                },
            ),
            other => Err(Error::new(
                ErrorKind::IcebergFeatureUnsupported,
                format!("update {other} is not supported"),
            )),
        }
    }
}

/// Update table request.
pub struct UpdateTable {
    table_name: TableIdentifier,
    requirements: Vec<UpdateRquirement>,
    updates: Vec<MetadataUpdate>,
}

/// Update table builder.
pub struct UpdateTableBuilder(UpdateTable);

impl UpdateTable {
    /// Creates a update table builder.
    pub fn builder(table_name: TableIdentifier) -> UpdateTableBuilder {
        UpdateTableBuilder(UpdateTable {
            table_name,
            requirements: vec![],
            updates: vec![],
        })
    }
}

impl UpdateTableBuilder {
    /// Add requirements.
    pub fn add_requirements(
        &mut self,
        requirements: impl IntoIterator<Item = UpdateRquirement>,
    ) -> &mut Self {
        self.0.requirements.extend(requirements);
        self
    }

    /// Add updates.
    pub fn add_updates(&mut self, updates: impl IntoIterator<Item = MetadataUpdate>) -> &mut Self {
        self.0.updates.extend(updates);
        self
    }

    /// Build.
    pub fn build(self) -> UpdateTable {
        self.0
    }
}

/// Catalog type: rest, storage.
pub const CATALOG_TYPE: &str = "iceberg.catalog.type";
/// Catalog name
pub const CATALOG_NAME: &str = "iceberg.catalog.name";
pub(crate) const CATALOG_CONFIG_PREFIX: &str = "iceberg.catalog.";
const TABLE_IO_PREFIX: &str = "iceberg.table.io.";

/// Base catalog config.
#[derive(Debug, Default)]
pub struct BaseCatalogConfig {
    /// Name of catalog.
    pub name: String,
    /// Table io configs.
    pub table_io_configs: HashMap<String, String>,
    /// Table config.
    pub table_config: TableConfigRef,
}

/// Load catalog from configuration.
///
/// The following two configurations must be provides:
///
/// - [`CATALOG_TYPE`]: Type of catalog, must be one of `storage`, `rest`.
/// - [`CATALOG_NAME`]: Name of catalog.
///
/// ## Catalog specifig configuration.
///
/// Catalog specific configurations are prefixed with `iceberg.catalog.<catalog name>`.
/// For example, if catalog name is `demo`, then all catalog specific configuration keys must be prefixed with `iceberg.catalog.demo.`.:
///
/// ### Storage catalog
///
/// Currently the only required configuration is `iceberg.catalog.demo.warehouse`, which is the root path of warehouse.
///
/// ### Rest catalog
///
/// Currently the only required configuration is `iceberg.catalog.demo.uri`, which is the uri of rest catalog server.
///
/// ## IO Configuration
///
/// All configurations for table io are prefixed with `iceberg.table.io.`.
/// For example if underlying storage is s3, then we can use the following configurations can be provided:
///
/// - `iceberg.table.io.region`: Region of s3.
/// - `iceberg.table.io.endpoint`: Endpoint of s3.
///
/// ## Table reader/writer configuration.
///
/// User can control the behavior of table reader/writer by providing configurations, see [`TableConfig`] for details.
///
/// # Examples
///
/// ## Rest catalog
///
/// ```text
/// iceberg.catalog.name=demo # required
/// iceberg.catalog.type=rest # required
///
/// iceberg.catalog.demo.uri = http://localhost:9090 # required
///
/// ## Configurations for s3
/// iceberg.table.io.region = us-east-1
/// iceberg.table.io.endpoint = http://minio:9000
/// iceberg.table.io.bucket = icebergdata
/// iceberg.table.io.root = demo
/// iceberg.table.io.access_key_id=admin
/// iceberg.table.io.secret_access_key=password
///
/// ## Configurations for table reader/writer, following are optional.
/// iceberg.table.parquet_writer.enable_bloom_filter = true
/// ```
///
/// ## Storage catalog
///
/// ```text
/// iceberg.catalog.name=demo # required
/// iceberg.catalog.type=storage # required
///
/// iceberg.catalog.demo.warehouse = s3://icebergdata/demo # required
///
/// # Configuration for s3
/// iceberg.table.io.region=us-east-1
/// iceberg.table.io.endpoint=http://localhost:8181
/// iceberg.table.io.bucket = icebergdata
/// iceberg.table.io.root = demo
/// iceberg.table.io.access_key_id = admin
/// iceberg.table.io.secret_access_key = password
///
/// ## Configurations for table reader/writer, following are optional.
/// iceberg.table.parquet_writer.enable_bloom_filter = true
/// ```
pub async fn load_catalog(configs: &HashMap<String, String>) -> Result<CatalogRef> {
    log::info!("Loading catalog from configs: {:?}", configs);
    let catalog_type = configs.get(CATALOG_TYPE).ok_or_else(|| {
        Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("{CATALOG_TYPE} is not set"),
        )
    })?;

    let catalog_name = configs.get(CATALOG_NAME).ok_or_else(|| {
        Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("{CATALOG_NAME} is not set"),
        )
    })?;

    let table_io_configs = configs
        .iter()
        .filter(|(k, _v)| k.starts_with(TABLE_IO_PREFIX))
        .map(|(k, v)| (k[TABLE_IO_PREFIX.len()..].to_string(), v.to_string()))
        .collect();

    let table_config = Arc::new(TableConfig::try_from(configs)?);

    let base_catalog_config = BaseCatalogConfig {
        name: catalog_name.to_string(),
        table_io_configs,
        table_config,
    };

    log::info!("Parsed base catalog config: {:?}", base_catalog_config);

    match catalog_type.as_str() {
        "storage" => Ok(Arc::new(
            StorageCatalog::from_config(base_catalog_config, configs).await?,
        )),
        "rest" => Ok(Arc::new(
            RestCatalog::new(base_catalog_config, configs).await?,
        )),
        _ => Err(Error::new(
            ErrorKind::IcebergDataInvalid,
            format!("Unsupported catalog type: {catalog_type}"),
        )),
    }
}
