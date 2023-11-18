use std::{collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::SchemaRef;
use arrow_select::concat::concat_batches;
use icelake::io::{DefaultFileAppender, FileAppenderLayer, UpsertWriter};
use icelake::types::{AnyValue, Field, Struct, StructValueBuilder};
use icelake::{catalog::load_catalog, transaction::Transaction, Table, TableIdentifier};
mod utils;
use tokio::runtime::Builder;
pub use utils::*;

use libtest_mimic::{Arguments, Trial};

pub struct DeltaTest {
    docker_compose: DockerCompose,
    poetry: Poetry,
    catalog_configs: HashMap<String, String>,
}

impl DeltaTest {
    pub fn new(docker_compose: DockerCompose, poetry: Poetry, catalog_type: &str) -> Self {
        let warehouse_root = "demo";
        let table_namespace = "s1";
        let table_name = "t1";
        let catalog_configs = match catalog_type {
            "storage" => HashMap::from([
                ("iceberg.catalog.name", "demo".to_string()),
                ("iceberg.catalog.type", "storage".to_string()),
                (
                    "iceberg.catalog.demo.warehouse",
                    format!("s3://icebergdata/{}", &warehouse_root),
                ),
                ("iceberg.table.io.region", "us-east-1".to_string()),
                (
                    "iceberg.table.io.endpoint",
                    format!(
                        "http://{}:{}",
                        docker_compose.get_container_ip("minio"),
                        MINIO_DATA_PORT
                    ),
                ),
                ("iceberg.table.io.bucket", "icebergdata".to_string()),
                ("iceberg.table.io.root", warehouse_root.to_string()),
                ("iceberg.table.io.access_key_id", "admin".to_string()),
                ("iceberg.table.io.secret_access_key", "password".to_string()),
            ]),
            "rest" => HashMap::from([
                ("iceberg.catalog.name", "demo".to_string()),
                ("iceberg.catalog.type", "rest".to_string()),
                (
                    "iceberg.catalog.demo.uri",
                    format!(
                        "http://{}:{REST_CATALOG_PORT}",
                        docker_compose.get_container_ip("rest")
                    ),
                ),
                ("iceberg.table.io.region", "us-east-1".to_string()),
                (
                    "iceberg.table.io.endpoint",
                    format!(
                        "http://{}:{}",
                        docker_compose.get_container_ip("minio"),
                        MINIO_DATA_PORT
                    ),
                ),
                ("iceberg.table.io.bucket", "icebergdata".to_string()),
                (
                    "iceberg.table.io.root",
                    format!("{}/{}/{}", warehouse_root, table_namespace, table_name,),
                ),
                ("iceberg.table.io.access_key_id", "admin".to_string()),
                ("iceberg.table.io.secret_access_key", "password".to_string()),
            ]),
            _ => panic!("Unrecognized catalog type: {catalog_type}"),
        };

        let struct_type = Struct::new(vec![Arc::new(Field::required(
            1,
            "id",
            icelake::types::Any::Primitive(icelake::types::Primitive::Long),
        ))]);
        let mut builder = StructValueBuilder::new(struct_type.into());
        builder
            .add_field(
                1,
                Some(AnyValue::Primitive(icelake::types::PrimitiveValue::Long(1))),
            )
            .unwrap();
        Self {
            docker_compose,
            poetry,
            catalog_configs: catalog_configs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn init(&self) {
        let args = vec![
            "-s".to_string(),
            self.spark_connect_url(),
            "--sql".to_string(),
        ];
        let init_sqls = vec![
            "CREATE SCHEMA if not exists s1;".to_string(),
            "
        CREATE TABLE s1.t1
        (
            id long,
            key long,
            value long
        )  USING iceberg partitioned by(id)
        TBLPROPERTIES ('format-version'='2','write.delete.mode'='merge-on-read');
            "
            .to_string(),
            "
        CREATE TABLE s1.t2
        (
            id long,
            key long,
            value long
        ) USING iceberg partitioned by(id)
        TBLPROPERTIES ('format-version'='2','write.delete.mode'='merge-on-read');
            "
            .to_string(),
        ];
        let args: Vec<String> = args.into_iter().chain(init_sqls).collect();
        self.poetry.run_file("init.py", args, "Init t1 with spark")
    }

    fn spark_connect_url(&self) -> String {
        format!(
            "sc://{}:{}",
            self.docker_compose.get_container_ip("spark"),
            SPARK_CONNECT_SERVER_PORT
        )
    }

    pub async fn create_icelake_table(&self) -> Table {
        let catalog = load_catalog(&self.catalog_configs).await.unwrap();
        catalog
            .load_table(&TableIdentifier::new(vec!["s1", "t1"].into_iter()).unwrap())
            .await
            .unwrap()
    }

    async fn write_and_delete_with_delta(
        &self,
        table: &Table,
        delta_writer: &mut UpsertWriter<impl FileAppenderLayer<DefaultFileAppender>>,
        write: Option<Vec<ArrayRef>>,
        delete: Option<Vec<ArrayRef>>,
    ) {
        let mut ops = Vec::new();
        let mut batches = Vec::with_capacity(2);
        let schema: SchemaRef = Arc::new(
            table
                .current_table_metadata()
                .current_schema()
                .unwrap()
                .clone()
                .try_into()
                .unwrap(),
        );

        if let Some(write) = write {
            let records = RecordBatch::try_new(schema.clone(), write).unwrap();
            ops.append(&mut vec![1; records.num_rows()]);
            batches.push(records);
        }

        if let Some(delete) = delete {
            let records = RecordBatch::try_new(schema.clone(), delete).unwrap();
            ops.append(&mut vec![2; records.num_rows()]);
            batches.push(records);
        }

        let batch = concat_batches(&schema, batches.iter()).unwrap();
        delta_writer.write(ops, &batch).await.unwrap();
    }

    async fn commit_writer(
        &self,
        table: &mut Table,
        delta_writer: UpsertWriter<impl FileAppenderLayer<DefaultFileAppender>>,
    ) {
        let mut result = delta_writer.close().await.unwrap().remove(0);

        // Commit table transaction
        {
            let mut tx = Transaction::new(table);
            if let Some(data) = result.data.pop() {
                tx.append_data_file([data].into_iter());
            }
            if let Some(delete) = result.pos_delete.pop() {
                tx.append_delete_file([delete].into_iter());
            }
            if let Some(delete) = result.eq_delete.pop() {
                tx.append_delete_file([delete].into_iter());
            }
            tx.commit().await.unwrap();
        }
    }

    pub async fn test_write(&mut self) {
        self.poetry.run_file(
            "execute.py",
            [
                "-s",
                &self.spark_connect_url(),
                "-q",
                "insert into s1.t2 values (1,1,1),(1,2,2),(1,3,3),(1,4,4)",
            ],
            "insert into s1.t2 values (1,1,1),(1,2,2),(1,3,3),(1,4,4)",
        );

        let mut table = self.create_icelake_table().await;
        log::info!(
            "Real path of table is: {}",
            table.current_table_metadata().location
        );
        let mut delta_writer = table
            .writer_builder()
            .await
            .unwrap()
            .build_upsert_writer(vec![1, 2])
            .unwrap();

        self.write_and_delete_with_delta(
            &table,
            &mut delta_writer,
            Some(vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![1, 4, 5])),
            ]),
            None,
        )
        .await;

        self.write_and_delete_with_delta(
            &table,
            &mut delta_writer,
            Some(vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![2, 3, 4])),
                Arc::new(Int64Array::from(vec![2, 3, 4])),
            ]),
            None,
        )
        .await;

        self.commit_writer(&mut table, delta_writer).await;

        self.poetry.run_file(
            "check.py",
            [
                "-s",
                &self.spark_connect_url(),
                "-q1",
                "select * from s1.t1",
                "-q2",
                "select * from s1.t2",
            ],
            "Check select * from s1.t1",
        )
    }

    pub async fn test_delete(&mut self) {
        self.poetry.run_file(
            "execute.py",
            [
                "-s",
                &self.spark_connect_url(),
                "-q",
                "insert into s1.t2 values (1,1,1),(1,2,2),(1,3,3)",
            ],
            "insert into s1.t2 values (1,1,1),(1,2,2),(1,3,3)",
        );

        let mut table = self.create_icelake_table().await;
        log::info!(
            "Real path of table is: {}",
            table.current_table_metadata().location
        );
        let mut delta_writer = table
            .writer_builder()
            .await
            .unwrap()
            .build_upsert_writer(vec![1, 2])
            .unwrap();

        self.write_and_delete_with_delta(
            &table,
            &mut delta_writer,
            Some(vec![
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 4])),
                Arc::new(Int64Array::from(vec![1, 2, 4])),
            ]),
            None,
        )
        .await;

        self.commit_writer(&mut table, delta_writer).await;

        let mut delta_writer = table
            .writer_builder()
            .await
            .unwrap()
            .build_upsert_writer(vec![1, 2])
            .unwrap();

        self.write_and_delete_with_delta(
            &table,
            &mut delta_writer,
            Some(vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int64Array::from(vec![3, 5])),
                Arc::new(Int64Array::from(vec![3, 5])),
            ]),
            Some(vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(Int64Array::from(vec![5, 4])),
                Arc::new(Int64Array::from(vec![5, 4])),
            ]),
        )
        .await;

        self.commit_writer(&mut table, delta_writer).await;

        self.poetry.run_file(
            "check.py",
            [
                "-s",
                &self.spark_connect_url(),
                "-q1",
                "select * from s1.t1",
                "-q2",
                "select * from s1.t2",
            ],
            "Check select * from s1.t1",
        )
    }

    async fn run_equality_delta_write_test(mut self) {
        self.init();
        self.test_write().await;
    }

    async fn run_equality_delta_delete_test(mut self) {
        self.init();
        self.test_delete().await;
    }

    pub fn block_run(self, test_case: String) {
        let rt = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .thread_name(self.docker_compose.project_name())
            .build()
            .unwrap();

        if test_case == "equality_delta_write_test" {
            rt.block_on(async { self.run_equality_delta_write_test().await });
        } else if test_case == "equality_delta_delete_test" {
            rt.block_on(async { self.run_equality_delta_delete_test().await });
        }
    }
}

fn create_test_fixture(project_name: &str, catalog: &str) -> DeltaTest {
    set_up();

    let docker_compose = match catalog {
        "storage" => DockerCompose::new(project_name, "iceberg-fs"),
        "rest" => DockerCompose::new(project_name, "iceberg-rest"),
        _ => panic!("Unrecognized catalog : {catalog}"),
    };
    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    docker_compose.run();

    DeltaTest::new(docker_compose, poetry, catalog)
}

fn main() {
    // Parse command line arguments
    let args = Arguments::from_args();

    let catalogs = vec!["storage", "rest"];
    let test_cases = vec!["equality_delta_delete_test"];

    let mut tests = Vec::with_capacity(2);
    for catalog in &catalogs {
        for test_case in &test_cases {
            let test_case = test_case.to_string();
            let test_name = format!("{}_{}_{}", module_path!(), test_case, catalog);
            let fixture = create_test_fixture(&test_name, catalog);
            tests.push(Trial::test(test_name, move || {
                fixture.block_run(test_case);
                Ok(())
            }));
        }
    }

    // Run all tests and exit the application appropriately.
    libtest_mimic::run(&args, tests).exit();
}
