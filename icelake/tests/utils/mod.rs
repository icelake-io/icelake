use testcontainers::{Container, GenericImage};

mod poetry;

pub use poetry::*;

mod containers;

pub use containers::*;

use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use icelake::transaction::Transaction;
use icelake::Table;
use opendal::services::S3;
use opendal::Operator;
use std::fs::File;
use std::process::Command;
use std::sync::Arc;

use std::sync::Once;

static INIT: Once = Once::new();

pub fn set_up() {
    INIT.call_once(env_logger::init);
}

pub fn run_command(mut cmd: Command, desc: impl ToString) {
    let desc = desc.to_string();
    log::info!("Starting to {}, command: {:?}", &desc, cmd);
    let exit = cmd.status().unwrap();
    if exit.success() {
        log::info!("{} succeed!", desc)
    } else {
        panic!("{} failed: {:?}", desc, exit);
    }
}

fn read_records_to_arrow(filename: &str) -> Vec<RecordBatch> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v_int", DataType::Int32, true),
        Field::new("v_long", DataType::Int64, true),
        Field::new("v_float", DataType::Float32, true),
        Field::new("v_double", DataType::Float64, true),
        Field::new("v_varchar", DataType::Utf8, true),
        Field::new("v_bool", DataType::Boolean, true),
        Field::new("v_date", DataType::Date32, true),
        Field::new(
            "v_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+04:00".into())),
            true,
        ),
        Field::new("v_decimal", DataType::Decimal128(36, 10), true),
        Field::new(
            "v_ts_ntz",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]);

    let csv_reader = ReaderBuilder::new(Arc::new(schema))
        .has_header(false)
        .build(File::open(filename).unwrap())
        .unwrap();

    csv_reader.map(|r| r.unwrap()).collect::<Vec<RecordBatch>>()
}

pub struct TestFixture<'a> {
    pub spark: Container<'a, GenericImage>,
    pub minio: Container<'a, GenericImage>,

    pub poetry: Poetry,

    pub table_name: String,
    pub csv_file: String,
    pub partition_csv_file: Option<String>,
    pub table_root: String,

    pub init_sqls: Vec<String>,
}

impl TestFixture<'_> {
    pub fn init_table_with_spark(&self) {
        let args = vec![
            "-s".to_string(),
            self.spark_connect_url(),
            "--sql".to_string(),
        ];
        let args: Vec<String> = args.into_iter().chain(self.init_sqls.clone()).collect();
        self.poetry.run_file(
            "init.py",
            args,
            format!("Init {} with spark", self.table_name.as_str()),
        )
    }

    pub fn check_table_with_spark(&self) {
        self.poetry.run_file(
            "check.py",
            [
                "-s",
                &self.spark_connect_url(),
                "-f",
                self.csv_file_path().as_str(),
                "-q",
                format!("SELECT * FROM s1.{} ORDER BY id ASC", self.table_name).as_str(),
            ],
            format!("Check {} with spark", self.table_name.as_str()),
        )
    }

    pub fn check_table_partition_with_spark(&self) {
        self.poetry.run_file(
            "check.py",
            [
                "-s",
                &self.spark_connect_url(),
                "-f",
                self.partition_csv_file_path().as_str(),
                "--partition",
                "-q",
                format!("SELECT * FROM s1.{}.partitions", self.table_name).as_str(),
            ],
            format!("Check {} with spark", self.table_name.as_str()),
        )
    }

    pub fn spark_connect_url(&self) -> String {
        format!(
            "sc://localhost:{}",
            self.spark.get_host_port_ipv4(SPARK_CONNECT_SERVER_PORT)
        )
    }

    pub async fn create_icelake_table(&self) -> Table {
        let mut builder = S3::default();
        builder.root(self.table_root.as_str());
        builder.bucket("icebergdata");
        builder.endpoint(
            format!(
                "http://localhost:{}",
                self.minio.get_host_port_ipv4(MINIO_DATA_PORT)
            )
            .as_str(),
        );
        builder.access_key_id("admin");
        builder.secret_access_key("password");
        builder.region("us-east-1");

        let op = Operator::new(builder).unwrap().finish();
        Table::open_with_op(op).await.unwrap()
    }

    pub async fn write_data_with_icelake(&mut self) {
        let mut table = self.create_icelake_table().await;
        log::info!(
            "Real path of table is: {}",
            table.current_table_metadata().location
        );

        let records = read_records_to_arrow(self.csv_file_path().as_str());

        let mut task_writer = table.task_writer().await.unwrap();

        for record_batch in &records {
            log::info!(
                "Insert record batch with {} records using icelake.",
                record_batch.num_rows()
            );
            task_writer.write(record_batch).await.unwrap();
        }

        let result = task_writer.close().await.unwrap();
        log::debug!("Insert {} data files: {:?}", result.len(), result);

        // Commit table transaction
        {
            let mut tx = Transaction::new(&mut table);
            tx.append_file(result);
            tx.commit().await.unwrap();
        }
    }

    pub fn csv_file_path(&self) -> String {
        format!(
            "{}/../testdata/csv/{}",
            env!("CARGO_MANIFEST_DIR"),
            self.csv_file
        )
    }

    pub fn partition_csv_file_path(&self) -> String {
        format!(
            "{}/../testdata/csv/{}",
            env!("CARGO_MANIFEST_DIR"),
            self.partition_csv_file.as_ref().unwrap()
        )
    }

    pub async fn run(mut self) {
        self.init_table_with_spark();
        self.write_data_with_icelake().await;
        self.check_table_with_spark();
        if self.partition_csv_file.is_some() {
            self.check_table_partition_with_spark();
        }
    }
}
