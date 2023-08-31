//! Rest catalog implementation.
//!

use std::collections::HashMap;

use async_trait::async_trait;

use crate::{
    table::{Namespace, TableIdentifier},
    types::{PartitionSpec, Schema},
    Table,
};

use super::{Catalog, UpdateTable};
use crate::error::Result;

/// Rest catalog implementation
pub struct RestCatalog {}

#[async_trait]
impl Catalog for RestCatalog {
    /// Return catalog's name.
    fn name(&self) -> &str {
        todo!()
    }

    /// List tables under namespace.
    async fn list_tables(&self, _ns: &Namespace) -> Result<Vec<TableIdentifier>> {
        todo!()
    }

    /// Creates a table.
    async fn create_table(
        &self,
        _table_name: &TableIdentifier,
        _schema: &Schema,
        _spec: &PartitionSpec,
        _location: &str,
        _props: HashMap<String, String>,
    ) -> Result<Table> {
        todo!()
    }

    /// Check table exists.
    async fn table_exists(&self, _table_name: &TableIdentifier) -> Result<bool> {
        todo!()
    }

    /// Drop table.
    async fn drop_table(&self, _table_name: &TableIdentifier, _purge: bool) -> Result<()> {
        todo!()
    }

    /// Rename table.
    async fn rename_table(&self, _from: &TableIdentifier, _to: &TableIdentifier) -> Result<()> {
        todo!()
    }

    /// Load table.
    async fn load_table(&self, _table_name: &TableIdentifier) -> Result<Table> {
        todo!()
    }

    /// Invalidate table.
    async fn invalidate_table(&self, _table_name: &TableIdentifier) -> Result<()> {
        todo!()
    }

    /// Register a table using metadata file location.
    async fn register_table(
        &self,
        _table_name: &TableIdentifier,
        _metadata_file_location: &str,
    ) -> Result<Table> {
        todo!()
    }

    /// Update table.
    async fn update_table(&self, _udpate_table: &UpdateTable) -> Result<Table> {
        todo!()
    }
}
