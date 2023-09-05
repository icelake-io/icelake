//! Rest catalog implementation.
//!

use std::collections::HashMap;

use async_trait::async_trait;
use iceberg_rest_api::{
    apis::{
        catalog_api_api::list_tables, configuration::Configuration,
        configuration_api_api::get_config,
    },
    models::TableIdentifier as RestTableIdentifier,
};
use reqwest::ClientBuilder;

use crate::{
    table::{Namespace, TableIdentifier},
    types::{PartitionSpec, Schema},
    Error, ErrorKind, Table,
};

use super::{Catalog, UpdateTable};
use crate::error::Result;

/// Configuration for rest catalog.
#[derive(Debug, Default)]
pub struct RestCatalogConfig {
    uri: String,
    prefix: Option<String>,
    warehouse: Option<String>,
}

/// Rest catalog implementation
pub struct RestCatalog {
    name: String,
    config: RestCatalogConfig,
    // rest client config
    rest_client: Configuration,
}

#[async_trait]
impl Catalog for RestCatalog {
    /// Return catalog's name.
    fn name(&self) -> &str {
        &self.name
    }

    /// List tables under namespace.
    async fn list_tables(&self, ns: &Namespace) -> Result<Vec<TableIdentifier>> {
        let resp = list_tables(
            &self.rest_client,
            self.config.prefix.as_deref(),
            &ns.encode_in_url()?,
        )
        .await?;
        Ok(resp
            .identifiers
            .unwrap_or_default()
            .iter()
            .map(TableIdentifier::from)
            .collect())
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
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            "Creating table in rest client is not implemented yet!",
        ))
    }

    /// Check table exists.
    async fn table_exists(&self, _table_name: &TableIdentifier) -> Result<bool> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            "Table exists in rest client is not implemented yet!",
        ))
    }

    /// Drop table.
    async fn drop_table(&self, _table_name: &TableIdentifier, _purge: bool) -> Result<()> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            "Drop table in rest client is not implemented yet!",
        ))
    }

    /// Rename table.
    async fn rename_table(&self, _from: &TableIdentifier, _to: &TableIdentifier) -> Result<()> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            "Rename table in rest client is not implemented yet!",
        ))
    }

    /// Load table.
    async fn load_table(&self, _table_name: &TableIdentifier) -> Result<Table> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            "Load table in rest client is not implemented yet!",
        ))
    }

    /// Invalidate table.
    async fn invalidate_table(&self, _table_name: &TableIdentifier) -> Result<()> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            "Invalidate table in rest client is not implemented yet!",
        ))
    }

    /// Register a table using metadata file location.
    async fn register_table(
        &self,
        _table_name: &TableIdentifier,
        _metadata_file_location: &str,
    ) -> Result<Table> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            "Register table in rest client is not implemented yet!",
        ))
    }

    /// Update table.
    async fn update_table(&self, _udpate_table: &UpdateTable) -> Result<Table> {
        Err(Error::new(
            ErrorKind::IcebergFeatureUnsupported,
            "Update table in rest client is not implemented yet!",
        ))
    }
}

impl RestCatalog {
    /// Creates rest catalog.
    pub async fn new(name: impl AsRef<str>, config: HashMap<String, String>) -> Result<Self> {
        let catalog_config = RestCatalog::init_config_from_server(config).await?;
        let rest_client = RestCatalog::create_rest_client(&catalog_config)?;

        Ok(Self {
            name: name.as_ref().to_string(),
            config: catalog_config,
            rest_client,
        })
    }

    fn create_rest_client(config: &RestCatalogConfig) -> Result<Configuration> {
        let mut client = Configuration {
            base_path: config.uri.to_string(),
            ..Default::default()
        };

        let req = { ClientBuilder::new().build()? };

        client.client = req;

        Ok(client)
    }

    async fn init_config_from_server(config: HashMap<String, String>) -> Result<RestCatalogConfig> {
        log::info!("Creating rest catalog with user config: {config:?}");
        let rest_catalog_config = RestCatalogConfig::try_from(&config)?;
        let rest_client = RestCatalog::create_rest_client(&rest_catalog_config)?;

        let mut server_config =
            get_config(&rest_client, rest_catalog_config.warehouse.as_deref()).await?;

        log::info!("Catalog config from rest catalog server: {server_config:?}");
        server_config.defaults.extend(config);
        server_config.defaults.extend(server_config.overrides);

        let ret = RestCatalogConfig::try_from(&server_config.defaults)?;

        log::info!("Result rest catalog config after merging with catalog server config: {ret:?}");
        Ok(ret)
    }
}

impl TryFrom<&HashMap<String, String>> for RestCatalogConfig {
    type Error = Error;

    fn try_from(value: &HashMap<String, String>) -> Result<RestCatalogConfig> {
        let mut config = RestCatalogConfig {
            uri: value
                .get("uri")
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::IcebergDataInvalid,
                        "uri is missing for rest catalog.",
                    )
                })?
                .clone(),
            ..Default::default()
        };

        if let Some(warehouse) = value.get("warehouse") {
            config.warehouse = Some(warehouse.clone());
        }

        if let Some(prefix) = value.get("prefix") {
            config.prefix = Some(prefix.clone());
        }

        Ok(config)
    }
}

impl From<&RestTableIdentifier> for TableIdentifier {
    fn from(value: &RestTableIdentifier) -> Self {
        Self {
            namespace: Namespace {
                levels: value.namespace.clone(),
            },
            name: value.name.clone(),
        }
    }
}

impl Namespace {
    /// Returns url encoded format.
    pub fn encode_in_url(&self) -> Result<String> {
        if self.levels.is_empty() {
            return Err(Error::new(
                ErrorKind::IcebergDataInvalid,
                "Can't encode empty namespace in url!",
            ));
        }

        Ok(self.levels.join("\u{1F}").to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::table::Namespace;

    #[test]
    fn test_namespace_encode() {
        let ns = Namespace {
            levels: vec!["a".to_string(), "b".to_string()],
        };

        assert_eq!("a\u{1F}b", ns.encode_in_url().unwrap());
    }
}
