use std::{collections::HashMap, sync::Arc};

use icelake::{
    catalog::{Catalog, RestCatalog},
    types::{Any, Field, Primitive, Schema, Struct, TableFormatVersion},
    Namespace, TableIdentifier,
};

mod utils;
pub use utils::*;

struct TestFixture2 {
    docker_compose: DockerCompose,
    poetry: Poetry,
}

fn create_test_fixture(project_name: &str) -> TestFixture2 {
    set_up();

    let docker_compose = DockerCompose::new(project_name, "iceberg-rest");
    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    TestFixture2 {
        docker_compose,
        poetry,
    }
}

impl TestFixture2 {
    async fn get_rest_catalog(&self) -> RestCatalog {
        let config: HashMap<String, String> = HashMap::from([(
            "uri",
            format!(
                "http://{}:{REST_CATALOG_PORT}",
                self.docker_compose.get_container_ip("rest")
            ),
        )])
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

        RestCatalog::new("test", config).await.unwrap()
    }
}

#[tokio::test]
async fn test_list_tables() {
    let test_fixture = create_test_fixture(&normalize_test_name(format!(
        "{}_test_list_ables",
        module_path!()
    )));

    test_fixture.docker_compose.run();

    test_fixture.poetry.run_file(
        "init.py",
        vec![
            "-s",
            &format!(
                "sc://{}:{}",
                test_fixture.docker_compose.get_container_ip("spark"),
                SPARK_CONNECT_SERVER_PORT
            ),
            "--sql",
            "CREATE SCHEMA IF NOT EXISTS s1",
            "DROP TABLE IF EXISTS s1.t1",
            "CREATE TABLE s1.t1 (id long) using ICEBERG TBLPROPERTIES ('format-version'='2')",
            "DROP TABLE IF EXISTS s1.t2",
            "CREATE TABLE s1.t2 (id long) using ICEBERG TBLPROPERTIES ('format-version'='2')",
            "CREATE SCHEMA IF NOT EXISTS s2",
            "DROP TABLE IF EXISTS s2.t1",
            "CREATE TABLE s2.t1 (id long) using ICEBERG TBLPROPERTIES ('format-version'='2')",
        ],
        "Init spark tables",
    );

    let catalog = test_fixture.get_rest_catalog().await;

    let mut table_ids = catalog
        .list_tables(&Namespace::new(vec!["s1"]))
        .await
        .unwrap();

    let s2_table_ids = catalog
        .list_tables(&Namespace::new(vec!["s2"]))
        .await
        .unwrap();

    table_ids.extend(s2_table_ids);

    let expected_tables = vec![
        TableIdentifier::new(vec!["s1", "t1"]).unwrap(),
        TableIdentifier::new(vec!["s1", "t2"]).unwrap(),
        TableIdentifier::new(vec!["s2", "t1"]).unwrap(),
    ];

    assert_eq!(expected_tables, table_ids);
}

#[tokio::test]
async fn test_load_table() {
    let test_fixture = create_test_fixture(&normalize_test_name(format!(
        "{}_test_load_table",
        module_path!()
    )));

    test_fixture.docker_compose.run();

    test_fixture.poetry.run_file(
        "init.py",
        vec![
            "-s",
            &format!(
                "sc://{}:{}",
                test_fixture.docker_compose.get_container_ip("spark"),
                SPARK_CONNECT_SERVER_PORT
            ),
            "--sql",
            "CREATE SCHEMA IF NOT EXISTS s1",
            "DROP TABLE IF EXISTS s1.t1",
            "CREATE TABLE s1.t1 (id long not null, name string, dist double) Using iceberg TBLPROPERTIES ('format-version'='2')",
        ],
        "Init spark tables",
    );

    let catalog = test_fixture.get_rest_catalog().await;

    let table = catalog
        .load_table(&TableIdentifier::new(vec!["s1", "t1"]).unwrap())
        .await
        .unwrap();

    let current_metadata = table.current_table_metadata();
    assert_eq!(TableFormatVersion::V2, current_metadata.format_version);

    let expected_schema = Schema::new(
        0,
        None,
        Struct::new(vec![
            Arc::new(Field::required(1, "id", Any::Primitive(Primitive::Long))),
            Arc::new(Field::optional(
                2,
                "name",
                Any::Primitive(Primitive::String),
            )),
            Arc::new(Field::optional(
                3,
                "dist",
                Any::Primitive(Primitive::Double),
            )),
        ]),
    );

    assert_eq!(&expected_schema, current_metadata.current_schema().unwrap());
}
