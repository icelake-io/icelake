//! This module defines catalog api for icelake.

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use enum_display::EnumDisplay;
use uuid::Uuid;

use crate::error::Result;
use crate::table::{Namespace, TableIdentifier};
use crate::types::{
    PartitionField, PartitionSpec, Schema, Snapshot, SnapshotReferenceType, SortOrder,
};
use crate::Table;

mod rest;
pub use rest::*;

/// Catalog definition.
#[async_trait]
pub trait Catalog {
    /// Return catalog's name.
    fn name(&self) -> &str;

    /// List tables under namespace.
    async fn list_tables(&self, ns: &Namespace) -> Result<Vec<TableIdentifier>>;

    /// Creates a table.
    async fn create_table(
        &self,
        table_name: &TableIdentifier,
        schema: &Schema,
        spec: &PartitionSpec,
        location: &str,
        props: HashMap<String, String>,
    ) -> Result<Table>;

    /// Check table exists.
    async fn table_exists(&self, table_name: &TableIdentifier) -> Result<bool>;

    /// Drop table.
    async fn drop_table(&self, table_name: &TableIdentifier, purge: bool) -> Result<()>;

    /// Rename table.
    async fn rename_table(&self, from: &TableIdentifier, to: &TableIdentifier) -> Result<()>;

    /// Load table.
    async fn load_table(&self, table_name: &TableIdentifier) -> Result<Table>;

    /// Invalidate table.
    async fn invalidate_table(&self, table_name: &TableIdentifier) -> Result<()>;

    /// Register a table using metadata file location.
    async fn register_table(
        &self,
        table_name: &TableIdentifier,
        metadata_file_location: &str,
    ) -> Result<Table>;

    /// Update table.
    async fn update_table(&self, udpate_table: &UpdateTable) -> Result<Table>;
}

/// Update table requirments
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

/// Update table request.
pub struct UpdateTable {
    table_name: TableIdentifier,
    requirements: Vec<UpdateRquirement>,
    updates: Vec<MetadataUpdate>,
}
