use std::{collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StructArray};
use arrow_schema::{DataType, SchemaRef};
use icelake::io_v2::input_wrapper::{DeltaWriter, RecordBatchWriter};
use icelake::io_v2::{EqualityDeltaWriterBuilder, IcebergWriterBuilder};
use icelake::types::{AnyValue, Field, Struct, StructValueBuilder};
use icelake::{catalog::load_catalog, transaction::Transaction, Table, TableIdentifier};
mod utils;
use itertools::Itertools;
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

    async fn create_append_only_writer(
        &self,
        table: &Table,
        partition_col_idx: usize,
        schema: &SchemaRef,
    ) -> RecordBatchWriter {
        let builder_helper = table.builder_helper().unwrap();

        let data = builder_helper.data_file_writer_builder(0).unwrap();
        let partition = builder_helper
            .precompute_partition_writer_builder(data, partition_col_idx)
            .unwrap();

        RecordBatchWriter::new(partition.build(schema).await.unwrap())
    }

    async fn create_delta_writer(
        &self,
        table: &Table,
        equality_ids: Vec<usize>,
        partition_col_idx: usize,
        schema: &SchemaRef,
    ) -> DeltaWriter {
        let builder_helper = table.builder_helper().unwrap();

        let data = builder_helper.data_file_writer_builder(0).unwrap();
        let pos_delete = builder_helper
            .position_delete_writer_builder(0, 1000)
            .unwrap();
        let eq_delete = builder_helper
            .equality_delete_writer_builder(equality_ids.clone(), 0)
            .unwrap();

        let eq_delta = EqualityDeltaWriterBuilder::new(data, pos_delete, eq_delete, equality_ids);
        let partition_eq_delta = builder_helper
            .precompute_partition_writer_builder(eq_delta, partition_col_idx)
            .unwrap();

        DeltaWriter::new(partition_eq_delta.build(schema).await.unwrap())
    }

    async fn commit_append_writer(&self, table: &mut Table, writer: &mut RecordBatchWriter) {
        let result = writer.flush().await.unwrap();

        // Commit table transaction
        {
            let mut tx = Transaction::new(table);
            result.into_iter().for_each(|data| {
                tx.append_data_file([data]);
            });
            tx.commit().await.unwrap();
        }
    }

    async fn commit_delta_writer(&self, table: &mut Table, delta_writer: &mut DeltaWriter) {
        let result = delta_writer.flush().await.unwrap().remove(0);

        // Commit table transaction
        {
            let mut tx = Transaction::new(table);
            result.data.into_iter().for_each(|data| {
                tx.append_data_file([data]);
            });
            result.pos_delete.into_iter().for_each(|delete| {
                tx.append_delete_file([delete]);
            });
            result.eq_delete.into_iter().for_each(|delete| {
                tx.append_delete_file([delete]);
            });
            tx.commit().await.unwrap();
        }
    }

    pub async fn test_append_only(&mut self) {
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

        let schema_with_partition_type = {
            let arrow_schema = table.current_arrow_schema().unwrap();
            let partition_type = table.current_partition_type().unwrap();
            let mut new_fields = arrow_schema.fields().into_iter().cloned().collect_vec();
            new_fields.push(
                arrow_schema::Field::new(
                    "partition",
                    partition_type.clone().try_into().unwrap(),
                    false,
                )
                .into(),
            );
            Arc::new(arrow_schema::Schema::new(new_fields))
        };

        let mut writer = self
            .create_append_only_writer(&table, 3, &schema_with_partition_type)
            .await;

        writer
            .write(
                RecordBatch::try_new(
                    schema_with_partition_type.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                        Arc::new(StructArray::from(vec![(
                            if let DataType::Struct(fields) =
                                schema_with_partition_type.field(3).data_type()
                            {
                                fields[0].clone()
                            } else {
                                unreachable!()
                            },
                            Arc::new(Int64Array::from(vec![1, 1, 1, 1])) as ArrayRef,
                        )])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        self.commit_append_writer(&mut table, &mut writer).await;

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

    pub async fn test_delta_test(&mut self) {
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

        let schema_with_partition_type = {
            let arrow_schema = table.current_arrow_schema().unwrap();
            let partition_type = table.current_partition_type().unwrap();
            let mut new_fields = arrow_schema.fields().into_iter().cloned().collect_vec();
            new_fields.push(
                arrow_schema::Field::new(
                    "partition",
                    partition_type.clone().try_into().unwrap(),
                    false,
                )
                .into(),
            );
            Arc::new(arrow_schema::Schema::new(new_fields))
        };
        let mut delta_writer = self
            .create_delta_writer(&table, vec![1, 2], 3, &schema_with_partition_type)
            .await;

        delta_writer
            .write(
                vec![1, 1, 1, 1],
                RecordBatch::try_new(
                    schema_with_partition_type.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                        Arc::new(StructArray::from(vec![(
                            if let DataType::Struct(fields) =
                                schema_with_partition_type.field(3).data_type()
                            {
                                fields[0].clone()
                            } else {
                                unreachable!()
                            },
                            Arc::new(Int64Array::from(vec![1, 1, 1, 1])) as ArrayRef,
                        )])),
                    ],
                )
                .unwrap(),
            )
            .await
            .unwrap();

        self.commit_delta_writer(&mut table, &mut delta_writer)
            .await;

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

    async fn run_append_only_test(mut self) {
        self.init();
        self.test_append_only().await;
    }

    async fn run_delta_test(mut self) {
        self.init();
        self.test_delta_test().await;
    }

    pub fn block_run(self, test_case: String) {
        let rt = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .thread_name(self.docker_compose.project_name())
            .build()
            .unwrap();

        if test_case == "append_only_test" {
            rt.block_on(async { self.run_append_only_test().await });
        } else if test_case == "delta_test" {
            rt.block_on(async { self.run_delta_test().await });
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

    let catalogs = vec!["storage"];
    let test_cases = vec!["append_only_test", "delta_test"];

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
