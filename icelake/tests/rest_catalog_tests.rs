use std::collections::HashMap;

use icelake::{
    catalog::{Catalog, RestCatalog},
    Namespace, TableIdentifier,
};

use utils::{DockerCompose, Poetry};

use crate::utils::set_up;

mod utils;
use utils::REST_CATALOG_PORT;
use utils::SPARK_CONNECT_SERVER_PORT;

struct TestFixture {
    docker_compose: DockerCompose,
    poetry: Poetry,
}

fn create_test_fixture(project_name: &str) -> TestFixture {
    set_up();

    let docker_compose = DockerCompose::new(project_name, "iceberg-rest");
    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    TestFixture {
        docker_compose,
        poetry,
    }
}

#[tokio::test]
async fn test_list_tables() {
    let test_fixture = create_test_fixture("test2");

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

    let config: HashMap<String, String> = HashMap::from([(
        "uri",
        format!(
            "http://{}:{REST_CATALOG_PORT}",
            test_fixture.docker_compose.get_container_ip("rest")
        ),
    )])
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();

    let catalog = RestCatalog::new("test", config).await.unwrap();

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
