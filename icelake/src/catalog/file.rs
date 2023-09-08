//! This module contains file system catalog for icelake.

use std::sync::Arc;

use async_trait::async_trait;

use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use opendal::{EntryMode, Metakey, Operator};
use regex::Regex;
use uuid::Uuid;

use crate::{
    types::{self, serialize_table_meta, TableMetadata},
    Error, Namespace, Table, TableIdentifier, METADATA_FILE_EXTENSION, META_ROOT_PATH,
    VERSIONED_TABLE_METADATA_FILE_PATTERN, VERSION_HINT_FILENAME,
};

use super::{Catalog, OperatorArgs, UpdateTable};
use crate::error::Result;

/// File system catalog.
pub struct StorageCatalog {
    name: String,
    root_uri: String,
    op_args: OperatorArgs,
    op: Operator,
}

#[async_trait]
impl Catalog for StorageCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    async fn list_tables(self: Arc<Self>, ns: &Namespace) -> Result<Vec<TableIdentifier>> {
        let ns_path = ns.to_path()?;
        let mut ds = self.op.list(&ns_path).await?;
        let mut tables = vec![];
        while let Some(de) = ds.try_next().await? {
            let table_name = de.name();
            let table_identifier = TableIdentifier {
                namespace: ns.clone(),
                name: table_name.to_string(),
            };
            if self.is_table_dir(&table_identifier).await? {
                tables.push(table_identifier);
            }
        }

        Ok(tables)
    }

    async fn load_table(self: Arc<Self>, table: &TableIdentifier) -> Result<Table> {
        let table_path = table.to_path()?;
        let (cur_table_version, path) = if self.is_version_hint_exist(&table_path).await? {
            let version_hint = self.read_version_hint(&table_path).await?;
            (
                version_hint,
                format!("{table_path}/metadata/v{}.metadata.json", version_hint),
            )
        } else {
            let files = self.list_table_metadata_paths(&table_path).await?;

            let path = files.into_iter().last().ok_or(Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                "no table metadata found",
            ))?;

            let version_hint = {
                let re = Regex::new(VERSIONED_TABLE_METADATA_FILE_PATTERN)?;
                if re.is_match(path.as_str()) {
                    let (_, [version]) = re
                        .captures_iter(path.as_str())
                        .map(|c| c.extract())
                        .next()
                        .ok_or_else(|| {
                            Error::new(
                                crate::ErrorKind::IcebergDataInvalid,
                                format!("Invalid metadata file name {path}"),
                            )
                        })?;
                    version.parse()?
                } else {
                    // This is an ugly workaround to fix ut
                    log::error!("Hadoop table metadata filename doesn't not match pattern!");
                    0
                }
            };

            (version_hint, path)
        };

        let metadata = self.read_table_metadata(&path).await?;
        let table_op = self.table_operator(&table_path)?;

        Ok(
            Table::builder_from_catalog(table_op, self.clone(), metadata, table.clone())
                .with_current_table_version(cur_table_version as i64)
                .build()?,
        )
    }

    async fn update_table(self: Arc<Self>, table_update: &UpdateTable) -> Result<Table> {
        let table = self.clone().load_table(&table_update.table_name).await?;

        let mut metadata = table.current_table_metadata().clone();

        for requirement in &table_update.requirements {
            if !requirement.check(&metadata) {
                return Err(Error::new(
                    crate::ErrorKind::IcebergDataInvalid,
                    format!(
                        "Update table {} failed because requirement {requirement} not met!",
                        &table_update.table_name
                    ),
                ));
            }
        }

        for update in &table_update.updates {
            update.apply(&mut metadata)?;
        }

        self.commit_table(
            table_update.table_name.to_path()?.as_str(),
            table.current_table_version() + 1,
            metadata,
        )
        .await?;

        self.load_table(&table_update.table_name).await
    }
}

impl StorageCatalog {
    /// Load table from path.
    pub async fn load_table(path: &str) -> Result<Table> {
        if let Some((warehouse_path, table_name)) = path.rsplit_once('/') {
            Self::load_table_in(warehouse_path, table_name).await
        } else {
            Err(Error::new(
                crate::ErrorKind::IcebergDataInvalid,
                format!("Invalid path for table: [{path}]"),
            ))
        }
    }

    /// Load table in warehouse.
    pub async fn load_table_in(warehouse_path: &str, table_name: &str) -> Result<Table> {
        let op_args = Table::create_operator_args(warehouse_path)?;
        let op = Operator::try_from(&op_args)?;

        let fs_catalog = Arc::new(StorageCatalog {
            name: "fs catalog".to_string(),
            root_uri: warehouse_path.to_string(),
            op_args,
            op,
        });

        fs_catalog
            .load_table(&TableIdentifier::new(vec![table_name])?)
            .await
    }

    /// Create filesystem catalog from operator args.
    pub async fn open(op_args: OperatorArgs) -> Result<StorageCatalog> {
        let op = Operator::try_from(&op_args)?;

        Ok(StorageCatalog {
            name: "fs catalog".to_string(),
            root_uri: op_args.url()?,
            op_args,
            op,
        })
    }

    async fn is_table_dir(&self, table_name: &TableIdentifier) -> Result<bool> {
        let table_metadata_dir = format!("{}/{}", table_name.to_path()?, META_ROOT_PATH);
        if !self.op.is_exist(&table_metadata_dir).await? {
            return Ok(false);
        }

        let mut ds = self.op.list(&table_metadata_dir).await?;
        while let Some(de) = ds.try_next().await? {
            let meta = self.op.metadata(&de, Metakey::Mode).await?;

            match meta.mode() {
                EntryMode::FILE if de.name().ends_with(METADATA_FILE_EXTENSION) => {
                    return Ok(true);
                }
                _ => {}
            }
        }

        Ok(false)
    }

    /// Read version hint of table.
    ///
    /// `table_path`: relative path of table dir under warehouse root.
    async fn read_version_hint(&self, table_path: &str) -> Result<i32> {
        let content = self
            .op
            .read(format!("{table_path}/metadata/version-hint.text").as_str())
            .await?;
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
    ///
    /// `table_path`: relative path of table dir under warehouse root.
    async fn list_table_metadata_paths(&self, table_path: &str) -> Result<Vec<String>> {
        let mut lister = self
            .op
            .list(format!("{table_path}/metadata/").as_str())
            .await
            .map_err(|err| {
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

    /// Check if version hint file exist.
    ///
    /// `table_path`: relative path of table dir under warehouse root.
    async fn is_version_hint_exist(&self, table_path: &str) -> Result<bool> {
        self.op
            .is_exist(format!("{table_path}/metadata/version-hint.text").as_str())
            .await
            .map_err(|e| {
                Error::new(
                    crate::ErrorKind::IcebergDataInvalid,
                    format!("check if version hint exist failed: {}", e),
                )
            })
    }

    async fn commit_table(
        &self,
        table_path: &str,
        next_version: i64,
        next_metadata: TableMetadata,
    ) -> Result<()> {
        let tmp_metadata_file_path = format!(
            "{table_path}/metadata/{}{METADATA_FILE_EXTENSION}",
            Uuid::new_v4()
        );

        let final_metadata_file_path =
            format!("{table_path}/metadata/v{next_version}{METADATA_FILE_EXTENSION}");

        log::debug!("Writing to temporary metadata file path: {tmp_metadata_file_path}");
        self.op
            .write(
                &tmp_metadata_file_path,
                serialize_table_meta(next_metadata)?,
            )
            .await?;

        log::debug!("Renaming temporary metadata file path [{tmp_metadata_file_path}] to final metadata file path [{final_metadata_file_path}]");
        Self::rename(&self.op, &tmp_metadata_file_path, &final_metadata_file_path).await?;
        self.write_metadata_version_hint(next_version, table_path)
            .await?;

        Ok(())
    }

    async fn write_metadata_version_hint(&self, version: i64, table_path: &str) -> Result<()> {
        let tmp_version_hint_path =
            format!("{table_path}/metadata/{}-version-hint.temp", Uuid::new_v4());
        self.op
            .write(&tmp_version_hint_path, format!("{version}"))
            .await?;

        let final_version_hint_path = format!("{table_path}/metadata/{VERSION_HINT_FILENAME}");

        self.op.delete(final_version_hint_path.as_str()).await?;
        log::debug!("Renaming temporary version hint file path [{tmp_version_hint_path}] to final metadata file path [{final_version_hint_path}]");
        Self::rename(&self.op, &tmp_version_hint_path, &final_version_hint_path).await?;

        Ok(())
    }

    fn table_metadata_path(&self, table_path: &str, metadata_filename: &str) -> String {
        format!("{table_path}/metadata/{metadata_filename}")
    }

    async fn rename(op: &Operator, src_path: &str, dest_path: &str) -> Result<()> {
        let info = op.info();
        if info.can_rename() {
            Ok(op.rename(src_path, dest_path).await?)
        } else {
            op.copy(src_path, dest_path).await?;
            op.delete(src_path).await?;
            Ok(())
        }
    }

    fn table_operator(&self, table_path: &str) -> Result<Operator> {
        Operator::try_from(&self.op_args.sub_dir(table_path)?)
    }
}

impl Namespace {
    fn to_path(&self) -> Result<String> {
        Ok(self.levels.iter().join("/").to_string())
    }
}

impl TableIdentifier {
    fn to_path(&self) -> Result<String> {
        Ok(self
            .namespace
            .levels
            .iter()
            .chain([&self.name])
            .join("/")
            .to_string())
    }
}
