//! This module defines catalog api for icelake.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use enum_display::EnumDisplay;
use opendal::services::S3;
use opendal::Operator;
use uuid::Uuid;

use crate::error::Result;
use crate::table::{Namespace, TableIdentifier};
use crate::types::{
    PartitionField, PartitionSpec, Schema, Snapshot, SnapshotReferenceType, SortOrder,
    TableMetadata,
};
use crate::Table;

mod rest;
pub use rest::*;
mod file;
use crate::error::{Error, ErrorKind};
pub use file::*;

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
        snapshot_id: Option<i64>,
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
            MetadataUpdate::AddSnapshot { snapshot } => metadata.append_snapshot(snapshot.clone()),
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

/// Args for creating opendal operator
pub enum OperatorArgs {
    /// Creating s3 operator    
    S3 {
        /// Root path
        root: String,
        /// Bucket
        bucket: String,
        /// Endpoint
        endpoint: Option<String>,
        /// Region
        region: Option<String>,
        /// Access key
        access_key: Option<String>,
        /// Access secret
        access_secret: Option<String>,
    },
    /// Creating local file operator
    Fs {
        /// root
        root: String,
    },
}

impl TryFrom<&OperatorArgs> for Operator {
    type Error = Error;

    fn try_from(args: &OperatorArgs) -> Result<Self> {
        match args {
            OperatorArgs::S3 {
                root,
                bucket,
                endpoint,
                region,
                access_key,
                access_secret,
            } => {
                let mut s3 = S3::default();
                s3.root(root).bucket(bucket);

                if let Some(region) = region {
                    s3.region(region);
                }

                if let Some(endpoint) = endpoint {
                    s3.endpoint(endpoint);
                }

                if let Some(access_key) = access_key {
                    s3.access_key_id(access_key);
                }

                if let Some(access_secret) = access_secret {
                    s3.secret_access_key(access_secret);
                }

                Ok(Operator::new(s3)?.finish())
            }
            OperatorArgs::Fs { root } => {
                let mut fs = opendal::services::Fs::default();
                fs.root(root);
                Ok(Operator::new(fs)?.finish())
            }
        }
    }
}

impl OperatorArgs {
    /// Creates a subdir of current operator args.
    pub fn sub_dir(&self, sub_dir: &str) -> Self {
        match self {
            OperatorArgs::S3 {
                root,
                bucket,
                endpoint,
                region,
                access_key,
                access_secret,
            } => OperatorArgs::S3 {
                root: format!("{root}/{sub_dir}"),
                bucket: bucket.clone(),
                endpoint: endpoint.clone(),
                region: region.clone(),
                access_key: access_key.clone(),
                access_secret: access_secret.clone(),
            },
            OperatorArgs::Fs { root } => OperatorArgs::Fs {
                root: format!("{root}/{sub_dir}"),
            },
        }
    }

    /// Returns root path of current operator args.
    pub fn root(&self) -> String {
        match self {
            OperatorArgs::S3 { root, bucket, .. } => format!("s3://{bucket}/{root}"),
            OperatorArgs::Fs { root } => format!("file://{root}"),
        }
    }
}
