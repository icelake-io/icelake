/*
 * Apache Iceberg REST Catalog API
 *
 * Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.
 *
 * The version of the OpenAPI document: 0.0.1
 * 
 * Generated by: https://openapi-generator.tech
 */




#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct CreateNamespaceResponse {
    /// Reference to one or more levels of a namespace
    #[serde(rename = "namespace")]
    pub namespace: Vec<String>,
    /// Properties stored on the namespace, if supported by the server.
    #[serde(rename = "properties", skip_serializing_if = "Option::is_none")]
    pub properties: Option<::std::collections::HashMap<String, String>>,
}

impl CreateNamespaceResponse {
    pub fn new(namespace: Vec<String>) -> CreateNamespaceResponse {
        CreateNamespaceResponse {
            namespace,
            properties: None,
        }
    }
}


