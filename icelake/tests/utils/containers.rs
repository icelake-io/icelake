use std::net::IpAddr;
use std::time::Duration;
use testcontainers::core::WaitFor;
use testcontainers::GenericImage;

pub const MINIO_DATA_PORT: u16 = 9000u16;
pub const MINIO_CONSOLE_PORT: u16 = 9001u16;

pub const SPARK_CONNECT_SERVER_PORT: u16 = 15002u16;

pub fn spark_image(minio_ip: &IpAddr) -> GenericImage {
    GenericImage::new("apache/spark", "3.4.1")
        .with_volume(
            format!("{}/../testdata/docker/spark-script", env!("CARGO_MANIFEST_DIR")),
            "/spark-script",
        )
        .with_env_var("SPARK_HOME", "/opt/spark")
        .with_env_var("PYSPARK_PYTON", "/usr/bin/python3.9")
        .with_env_var("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/spark/bin:/opt/spark/sbin")
        .with_env_var("MINIO_IP", format!("{minio_ip}"))
        // .with_env_var("AWS_ACCESS_KEY_ID", "admin")
        // .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        // .with_env_var("AWS_REGION", "us-east-1")
        .with_exposed_port(SPARK_CONNECT_SERVER_PORT)
        .with_entrypoint("/spark-script/spark-connect-server.sh")
        .with_wait_for(WaitFor::StdOutMessage { message: "SparkConnectServer: Spark Connect server started".to_string()})
        .with_wait_for(WaitFor::Duration { length: Duration::from_secs(5)})
}

pub fn minio_image() -> GenericImage {
    GenericImage::new("minio/minio", "latest")
        .with_entrypoint("/minio-scripts/minio_server.sh")
        .with_volume(format!("{}/../testdata/docker/minio", env!("CARGO_MANIFEST_DIR")), "/minio-scripts")
        .with_exposed_port(MINIO_DATA_PORT)
        .with_exposed_port(MINIO_CONSOLE_PORT)
        .with_env_var("MINIO_ROOT_USER", "admin")
        .with_env_var("MINIO_ROOT_PASSWORD", "password")
        .with_env_var("MINIO_DOMAIN", "minio")
        .with_env_var("MINIO_HTTP_TRACE", "/dev/stdout")
        .with_wait_for(WaitFor::Duration {
            length: Duration::from_secs(3),
        })
}

pub fn mc_image(minio_ip: &IpAddr) -> GenericImage {
    GenericImage::new("minio/mc", "latest")
        .with_volume(format!("{}/../testdata/docker/minio", env!("CARGO_MANIFEST_DIR")), "/minio-scripts")
        .with_env_var("AWS_ACCESS_KEY_ID", "admin")
        .with_env_var("AWS_SECRET_ACCESS_KEY", "password")
        .with_env_var("AWS_REGION", "us-east-1")
        .with_env_var("MINIO_IP", format!("{minio_ip}"))
        .with_entrypoint("/minio-scripts/mc_init.sh")
        .with_wait_for(WaitFor::message_on_stderr("MC Done"))
}
