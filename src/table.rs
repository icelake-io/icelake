use std::collections::HashMap;

use anyhow::anyhow;
use anyhow::Result;
use opendal::layers::LoggingLayer;
use opendal::services::Fs;
use opendal::Operator;

use crate::types;

/// Table is the main entry point for the IceLake.
pub struct Table {
    op: Operator,

    /// `0` means the version is not loaded yet.
    current_version: i32,
    table_metadata: HashMap<i32, types::TableMetadata>,
}

impl Table {
    /// Create a new table via the given operator.
    pub fn new(op: Operator) -> Self {
        Self {
            op,
            current_version: 0,
            table_metadata: HashMap::new(),
        }
    }

    /// Load metadata and manifest from storage.
    pub async fn load(&mut self) -> Result<()> {
        let version_hint = self.read_version_hint().await?;
        if self.current_version == version_hint {
            return Ok(());
        }

        let metadata = self.read_table_metadata(version_hint).await?;
        self.table_metadata.insert(version_hint, metadata);
        self.current_version = version_hint;

        Ok(())
    }

    /// Fetch current table metadata.
    pub fn current_table_metadata(&self) -> Result<&types::TableMetadata> {
        if self.current_version == 0 {
            return Err(anyhow!("table metadata not loaded yet"));
        }

        self.table_metadata
            .get(&self.current_version)
            .ok_or_else(|| anyhow!("table metadata not found"))
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
}

impl Table {
    /// Read version hint of table.
    async fn read_version_hint(&self) -> Result<i32> {
        let content = self.op.read("metadata/version-hint.text").await?;
        let version_hint = String::from_utf8(content)?;

        version_hint
            .parse()
            .map_err(|e| anyhow!("parse version hint failed: {}", e))
    }

    /// Read table metadata of the given version.
    async fn read_table_metadata(&self, version: i32) -> Result<types::TableMetadata> {
        let content = self
            .op
            .read(&format!("metadata/v{}.metadata.json", version))
            .await?;

        let metadata = types::parse_table_metadata(&content)?;

        Ok(metadata)
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use opendal::{layers::LoggingLayer, services::Fs};

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

        let table_v1 = table.read_table_metadata(1).await?;

        assert_eq!(table_v1.format_version, types::TableFormatVersion::V1);
        assert_eq!(table_v1.last_updated_ms, 1686911664577);

        let table_v2 = table.read_table_metadata(2).await?;
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

        let table_metadata = table.current_table_metadata()?;
        assert_eq!(table_metadata.format_version, types::TableFormatVersion::V1);
        assert_eq!(table_metadata.last_updated_ms, 1686911671713);

        Ok(())
    }
}
