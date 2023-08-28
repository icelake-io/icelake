use crate::utils::{mc_image, minio_image, set_up, spark_image, Poetry, TestFixture};
use testcontainers::clients::Cli;

mod utils;

#[tokio::test]
async fn test_insert_no_partition() {
    set_up();

    let cli = Cli::default();

    let minio = cli.run(minio_image());

    log::debug!("Minio ports: {:?}", minio.ports());
    let minio_ip_addr = minio.get_bridge_ip_address();
    log::debug!("Minio ipaddress: {:?}", minio_ip_addr);

    {
        log::info!("Running minio control.");
        let _ = cli.run(mc_image(&minio_ip_addr));
    }

    let spark = cli.run(spark_image(&minio_ip_addr));

    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    let table_name = "t1".to_string();
    let table_root = "demo/s1/t1".to_string();

    let init_sqls = vec![
        "CREATE SCHEMA IF NOT EXISTS s1".to_string(),
        format!("DROP TABLE IF EXISTS s1.{table_name}"),
        format!(
            "
        CREATE TABLE s1.{table_name}
        (
          id long,
          v_int int,
          v_long long,
          v_float float,
          v_double double,
          v_varchar string,
          v_bool boolean,
          v_date date,
          v_timestamp timestamp,
          v_decimal decimal(36, 10),
          v_ts_ntz timestamp_ntz
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2');"
        ),
    ];

    let test_fixture = TestFixture {
        spark,
        minio,
        poetry,
        table_name,
        csv_file: "data.csv".to_string(),
        table_root,
        init_sqls,
        partition_csv_file: None,
    };

    test_fixture.run().await
}

#[tokio::test]
async fn test_insert_partition_identity() {
    set_up();

    let cli = Cli::default();

    let minio = cli.run(minio_image());

    log::debug!("Minio ports: {:?}", minio.ports());
    let minio_ip_addr = minio.get_bridge_ip_address();
    log::debug!("Minio ipaddress: {:?}", minio_ip_addr);

    {
        log::info!("Running minio control.");
        let _ = cli.run(mc_image(&minio_ip_addr));
    }

    let spark = cli.run(spark_image(&minio_ip_addr));

    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    let table_name = "t2".to_string();
    let table_root = "demo/s1/t2".to_string();

    let init_sqls = vec![
        "CREATE SCHEMA IF NOT EXISTS s1".to_string(),
        format!("DROP TABLE IF EXISTS s1.{}", table_name),
        format!(
            "
        CREATE TABLE s1.{}
        (
          id long,
          v_int int,
          v_long long,
          v_float float,
          v_double double,
          v_varchar string,
          v_bool boolean,
          v_date date,
          v_timestamp timestamp,
          v_decimal decimal(36, 10),
          v_ts_ntz timestamp_ntz
        ) USING iceberg
        PARTITIONED BY (v_int, v_long, v_float, v_double, v_varchar, v_bool, v_date, v_timestamp,  v_ts_ntz)
        TBLPROPERTIES ('format-version'='2');",
            table_name
        ),
    ];

    let test_fixture = TestFixture {
        spark,
        minio,
        poetry,
        table_name,
        csv_file: "partition_data.csv".to_string(),
        table_root,
        init_sqls,
        partition_csv_file: None,
    };

    test_fixture.run().await
}

#[tokio::test]
async fn test_insert_partition_years() {
    set_up();

    let cli = Cli::default();

    let minio = cli.run(minio_image());

    log::debug!("Minio ports: {:?}", minio.ports());
    let minio_ip_addr = minio.get_bridge_ip_address();
    log::debug!("Minio ipaddress: {:?}", minio_ip_addr);

    {
        log::info!("Running minio control.");
        let _ = cli.run(mc_image(&minio_ip_addr));
    }

    let spark = cli.run(spark_image(&minio_ip_addr));

    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    let table_name = "t2".to_string();
    let table_root = "demo/s1/t2".to_string();

    let init_sqls = vec![
        "CREATE SCHEMA IF NOT EXISTS s1".to_string(),
        format!("DROP TABLE IF EXISTS s1.{}", table_name),
        format!(
            "
        CREATE TABLE s1.{}
        (
          id long,
          v_int int,
          v_long long,
          v_float float,
          v_double double,
          v_varchar string,
          v_bool boolean,
          v_date date,
          v_timestamp timestamp,
          v_decimal decimal(36, 10),
          v_ts_ntz timestamp_ntz
        ) USING iceberg
        PARTITIONED BY (years(v_date), years(v_timestamp), years(v_ts_ntz))
        TBLPROPERTIES ('format-version'='2');",
            table_name
        ),
    ];

    let test_fixture = TestFixture {
        spark,
        minio,
        poetry,
        table_name,
        csv_file: "partition_data.csv".to_string(),
        table_root,
        init_sqls,
        partition_csv_file: Some("partition_years.csv".to_string()),
    };

    test_fixture.run().await
}

#[tokio::test]
async fn test_insert_partition_months() {
    set_up();

    let cli = Cli::default();

    let minio = cli.run(minio_image());

    log::debug!("Minio ports: {:?}", minio.ports());
    let minio_ip_addr = minio.get_bridge_ip_address();
    log::debug!("Minio ipaddress: {:?}", minio_ip_addr);

    {
        log::info!("Running minio control.");
        let _ = cli.run(mc_image(&minio_ip_addr));
    }

    let spark = cli.run(spark_image(&minio_ip_addr));

    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    let table_name = "t2".to_string();
    let table_root = "demo/s1/t2".to_string();

    let init_sqls = vec![
        "CREATE SCHEMA IF NOT EXISTS s1".to_string(),
        format!("DROP TABLE IF EXISTS s1.{}", table_name),
        format!(
            "
        CREATE TABLE s1.{}
        (
          id long,
          v_int int,
          v_long long,
          v_float float,
          v_double double,
          v_varchar string,
          v_bool boolean,
          v_date date,
          v_timestamp timestamp,
          v_decimal decimal(36, 10),
          v_ts_ntz timestamp_ntz
        ) USING iceberg
        PARTITIONED BY (months(v_date), months(v_timestamp), months(v_ts_ntz))
        TBLPROPERTIES ('format-version'='2');",
            table_name
        ),
    ];

    let test_fixture = TestFixture {
        spark,
        minio,
        poetry,
        table_name,
        csv_file: "partition_data.csv".to_string(),
        table_root,
        init_sqls,
        partition_csv_file: Some("partition_months.csv".to_string()),
    };

    test_fixture.run().await
}

#[tokio::test]
async fn test_insert_partition_days() {
    set_up();

    let cli = Cli::default();

    let minio = cli.run(minio_image());

    log::debug!("Minio ports: {:?}", minio.ports());
    let minio_ip_addr = minio.get_bridge_ip_address();
    log::debug!("Minio ipaddress: {:?}", minio_ip_addr);

    {
        log::info!("Running minio control.");
        let _ = cli.run(mc_image(&minio_ip_addr));
    }

    let spark = cli.run(spark_image(&minio_ip_addr));

    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    let table_name = "t2".to_string();
    let table_root = "demo/s1/t2".to_string();

    let init_sqls = vec![
        "CREATE SCHEMA IF NOT EXISTS s1".to_string(),
        format!("DROP TABLE IF EXISTS s1.{}", table_name),
        format!(
            "
        CREATE TABLE s1.{}
        (
          id long,
          v_int int,
          v_long long,
          v_float float,
          v_double double,
          v_varchar string,
          v_bool boolean,
          v_date date,
          v_timestamp timestamp,
          v_decimal decimal(36, 10),
          v_ts_ntz timestamp_ntz
        ) USING iceberg
        PARTITIONED BY (days(v_date), days(v_timestamp), days(v_ts_ntz))
        TBLPROPERTIES ('format-version'='2');",
            table_name
        ),
    ];

    let test_fixture = TestFixture {
        spark,
        minio,
        poetry,
        table_name,
        csv_file: "partition_data.csv".to_string(),
        table_root,
        init_sqls,
        partition_csv_file: Some("partition_days.csv".to_string()),
    };

    test_fixture.run().await
}

#[tokio::test]
async fn test_insert_partition_hours() {
    set_up();

    let cli = Cli::default();

    let minio = cli.run(minio_image());

    log::debug!("Minio ports: {:?}", minio.ports());
    let minio_ip_addr = minio.get_bridge_ip_address();
    log::debug!("Minio ipaddress: {:?}", minio_ip_addr);

    {
        log::info!("Running minio control.");
        let _ = cli.run(mc_image(&minio_ip_addr));
    }

    let spark = cli.run(spark_image(&minio_ip_addr));

    let poetry = Poetry::new(format!("{}/../testdata/python", env!("CARGO_MANIFEST_DIR")));

    let table_name = "t2".to_string();
    let table_root = "demo/s1/t2".to_string();

    let init_sqls = vec![
        "CREATE SCHEMA IF NOT EXISTS s1".to_string(),
        format!("DROP TABLE IF EXISTS s1.{}", table_name),
        format!(
            "
        CREATE TABLE s1.{}
        (
          id long,
          v_int int,
          v_long long,
          v_float float,
          v_double double,
          v_varchar string,
          v_bool boolean,
          v_date date,
          v_timestamp timestamp,
          v_decimal decimal(36, 10),
          v_ts_ntz timestamp_ntz
        ) USING iceberg
        PARTITIONED BY (hours(v_timestamp), hours(v_ts_ntz))
        TBLPROPERTIES ('format-version'='2');",
            table_name
        ),
    ];

    let test_fixture = TestFixture {
        spark,
        minio,
        poetry,
        table_name,
        csv_file: "partition_data.csv".to_string(),
        table_root,
        init_sqls,
        partition_csv_file: Some("partition_hours.csv".to_string()),
    };

    test_fixture.run().await
}
