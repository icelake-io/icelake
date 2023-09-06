//! Rest catalog implementation.
//!

use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::{Client, ClientBuilder, Request, StatusCode};
use serde::de::DeserializeOwned;
use urlencoding::encode;

use crate::{
    catalog::rest::_models::CatalogConfig,
    table::{Namespace, TableIdentifier},
    types::{PartitionSpec, Schema, TableMetadata},
    Error, ErrorKind, Table,
};

use self::_models::{ListTablesResponse, LoadTableResult};

use super::{Catalog, UpdateTable};
use crate::error::Result;

const PATH_V1: &str = "v1";

/// Configuration for rest catalog.
#[derive(Debug, Default)]
pub struct RestCatalogConfig {
    uri: String,
    warehouse: Option<String>,
}

/// Rest catalog implementation
pub struct RestCatalog {
    name: String,
    config: RestCatalogConfig,
    endpoints: Endpoint,
    // rest client config
    rest_client: Client,
}

#[async_trait]
impl Catalog for RestCatalog {
    /// Return catalog's name.
    fn name(&self) -> &str {
        &self.name
    }

    /// List tables under namespace.
    async fn list_tables(&self, ns: &Namespace) -> Result<Vec<TableIdentifier>> {
        let request = self.rest_client.get(self.endpoints.tables(ns)?).build()?;
        Ok(self
            .execute_request::<ListTablesResponse>(request, |status| match status {
                StatusCode::NOT_FOUND => Some(Error::new(
                    ErrorKind::IcebergDataInvalid,
                    format!("Namespace {ns} not found!"),
                )),
                _ => None,
            })
            .await?
            .identifiers
            .into_iter()
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
    async fn load_table(&self, table_name: &TableIdentifier) -> Result<Table> {
        let resp = self
            .execute_request::<LoadTableResult>(
                self.rest_client
                    .get(self.endpoints.table(table_name)?)
                    .build()?,
                |status| match status {
                    StatusCode::NOT_FOUND => Some(Error::new(
                        ErrorKind::IcebergDataInvalid,
                        format!("Talbe {table_name} not found!"),
                    )),
                    _ => None,
                },
            )
            .await?;

        let metadata_location = resp.metadata_location.ok_or_else(|| {
            Error::new(
                ErrorKind::IcebergFeatureUnsupported,
                "Loading uncommitted table is not supported!",
            )
        })?;

        log::info!("Table metadata location of {table_name} is {metadata_location}");

        let table_metadata = TableMetadata::try_from(resp.metadata)?;
        Ok(Table::read_only_table(table_metadata, &metadata_location))
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
        let endpoints = Endpoint::new(catalog_config.uri.clone());
        let rest_client = RestCatalog::create_rest_client(&catalog_config)?;

        Ok(Self {
            name: name.as_ref().to_string(),
            config: catalog_config,
            rest_client,
            endpoints,
        })
    }

    async fn execute_request<T: DeserializeOwned>(
        &self,
        request: Request,
        error_handler: impl FnOnce(StatusCode) -> Option<Error>,
    ) -> Result<T> {
        log::debug!("Executing request: {request:?}");

        let resp = self.rest_client.execute(request).await?;

        match resp.status() {
            StatusCode::OK => {
                let text = resp.text().await?;
                log::debug!("Response text is: {text}");
                Ok(serde_json::from_slice::<T>(text.as_bytes())?)
            }
            other => {
                if let Some(error) = error_handler(other) {
                    Err(error)
                } else {
                    let text = resp.text().await?;
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        format!(
                            "Faile to execute http request, status code: {other}, message: {text}"
                        ),
                    ))
                }
            }
        }
    }

    fn create_rest_client(_config: &RestCatalogConfig) -> Result<Client> {
        Ok(ClientBuilder::new().build()?)
    }

    async fn init_config_from_server(config: HashMap<String, String>) -> Result<RestCatalogConfig> {
        log::info!("Creating rest catalog with user config: {config:?}");
        let rest_catalog_config = RestCatalogConfig::try_from(&config)?;
        let endpoint = Endpoint::new(rest_catalog_config.uri.clone());
        let rest_client = RestCatalog::create_rest_client(&rest_catalog_config)?;

        let resp = rest_client
            .execute(rest_client.get(endpoint.config()).build()?)
            .await?;

        match resp.status() {
            StatusCode::OK => {
                let mut server_config = resp.json::<CatalogConfig>().await?;
                log::info!("Catalog config from rest catalog server: {server_config:?}");
                server_config.defaults.extend(config);
                server_config.defaults.extend(server_config.overrides);

                let ret = RestCatalogConfig::try_from(&server_config.defaults)?;

                log::info!(
                    "Result rest catalog config after merging with catalog server config: {ret:?}"
                );
                Ok(ret)
            }
            _ => Err(Error::new(ErrorKind::Unexpected, resp.text().await?)),
        }
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

        Ok(config)
    }
}

// TODO: Support prefix
struct Endpoint {
    base: String,
}

impl Endpoint {
    fn new(base: String) -> Self {
        Self { base }
    }
    fn config(&self) -> String {
        [&self.base, PATH_V1, "config"].join("/")
    }

    fn tables(&self, ns: &Namespace) -> Result<String> {
        Ok([
            &self.base,
            PATH_V1,
            "namespaces",
            &ns.encode_in_url()?,
            "tables",
        ]
        .join("/")
        .to_string())
    }

    fn table(&self, table: &TableIdentifier) -> Result<String> {
        Ok([
            &self.base,
            PATH_V1,
            "namespaces",
            &table.namespace.encode_in_url()?,
            "tables",
            encode(&table.name).as_ref(),
        ]
        .join("/")
        .to_string())
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

        Ok(encode(&self.levels.join("\u{1F}")).to_string())
    }
}

mod _models {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    use crate::{table, types::TableMetadataSerDe};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub(super) struct TableIdentifier {
        pub(super) namespace: Vec<String>,
        pub(super) name: String,
    }

    impl From<TableIdentifier> for table::TableIdentifier {
        fn from(value: TableIdentifier) -> Self {
            Self {
                namespace: table::Namespace::new(value.namespace),
                name: value.name,
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub(super) struct ListTablesResponse {
        pub(super) identifiers: Vec<TableIdentifier>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub(super) struct CatalogConfig {
        pub(super) overrides: HashMap<String, String>,
        pub(super) defaults: HashMap<String, String>,
    }

    #[derive(Serialize, Deserialize)]
    pub(super) struct LoadTableResult {
        /// May be null if the table is staged as part of a transaction
        #[serde(rename = "metadata-location", skip_serializing_if = "Option::is_none")]
        pub(super) metadata_location: Option<String>,
        #[serde(rename = "metadata")]
        pub(super) metadata: TableMetadataSerDe,
        #[serde(rename = "config", skip_serializing_if = "Option::is_none")]
        pub(super) config: Option<::std::collections::HashMap<String, String>>,
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

        assert_eq!("a%1Fb", ns.encode_in_url().unwrap());
    }
}
