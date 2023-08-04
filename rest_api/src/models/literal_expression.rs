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
pub struct LiteralExpression {
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "term")]
    pub term: Box<crate::models::Term>,
    #[serde(rename = "value")]
    pub value: serde_json::Value,
}

impl LiteralExpression {
    pub fn new(
        r#type: String,
        term: crate::models::Term,
        value: serde_json::Value,
    ) -> LiteralExpression {
        LiteralExpression {
            r#type,
            term: Box::new(term),
            value,
        }
    }
}
