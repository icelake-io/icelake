//! This module contains append only tests.

use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use confique::Config;
use icelake::transaction::Transaction;
use icelake::Table;
use opendal::services::S3;
use opendal::Operator;
use std::fs::File;
use std::process::Command;
use std::sync::Arc;
use tokio::runtime::Builder;

use crate::utils::{path_of, run_command};

#[derive(Config, Debug)]
struct TestConfig {
    s3_bucket: String,
    s3_endpoint: String,
    s3_username: String,
    s3_password: String,
    s3_region: String,
    spark_url: String,
}

struct TestFixture {
    args: TestConfig,
}

impl TestFixture {
    async fn write_data_with_icelake(&mut self, table_root: &str, csv_file: &str) {
        let mut table = create_icelake_table(&self.args, table_root).await;
        log::info!(
            "Real path of table is: {}",
            table.current_table_metadata().location
        );

        let records = read_records_to_arrow(csv_file);

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
}

async fn prepare_env() -> TestFixture {
    TestFixture {
        args: Config::from_file(path_of("../testdata/config.toml")).unwrap(),
    }
}

async fn create_icelake_table(args: &TestConfig, table_root: &str) -> Table {
    let mut builder = S3::default();
    builder.root(table_root);
    builder.bucket(args.s3_bucket.as_str());
    builder.endpoint(args.s3_endpoint.as_str());
    builder.access_key_id(args.s3_username.as_str());
    builder.secret_access_key(args.s3_password.as_str());
    builder.region(args.s3_region.as_str());

    let op = Operator::new(builder).unwrap().finish();
    Table::open_with_op(op).await.unwrap()
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
        .build(File::open(path_of(format!("../testdata/{}", filename))).unwrap())
        .unwrap();

    csv_reader.map(|r| r.unwrap()).collect::<Vec<RecordBatch>>()
}

fn init_iceberg_table_with_spark(config: &TestConfig, table_name: &str) {
    let mut cmd = Command::new("poetry");
    cmd.args([
        "run",
        "python",
        "init.py",
        "-s",
        config.spark_url.as_str(),
        "-t",
        table_name,
    ])
    .current_dir(path_of("../python"));

    run_command(cmd, "init iceberg table")
}

fn check_iceberg_table_with_spark(config: &TestConfig, table_name: &str, data_csv: &str) {
    let mut cmd = Command::new("poetry");
    cmd.args([
        "run",
        "python",
        "check.py",
        "-s",
        config.spark_url.as_str(),
        "-f",
        path_of(format!("../testdata/{}", data_csv)).as_str(),
        "-t",
        table_name,
    ])
    .current_dir(path_of("../python"));

    run_command(cmd, "check iceberg table")
}

async fn do_test_append_data() {
    let mut fixture = prepare_env().await;

    init_iceberg_table_with_spark(&fixture.args, "t1");

    // Check simple table
    fixture
        .write_data_with_icelake("demo/s1/t1", "data.csv")
        .await;
    check_iceberg_table_with_spark(&fixture.args, "t1", "data.csv");
}

async fn do_test_append_data_partition() {
    let mut fixture = prepare_env().await;

    init_iceberg_table_with_spark(&fixture.args, "t2");

    // Check partition table
    fixture
        .write_data_with_icelake("demo/s1/t2", "partition_data.csv")
        .await;
    check_iceberg_table_with_spark(&fixture.args, "t2", "partition_data.csv");
}

pub fn test_append_data() {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .unwrap();
    rt.block_on(async { do_test_append_data().await });
}

pub fn test_append_data_partition() {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .build()
        .unwrap();
    rt.block_on(async { do_test_append_data_partition().await });
}
