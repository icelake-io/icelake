mod poetry;

pub use poetry::*;

mod docker;
mod test_generator;

pub use docker::*;
pub use test_generator::*;

use std::process::Command;

use std::sync::Once;

static INIT: Once = Once::new();

pub const SPARK_CONNECT_SERVER_PORT: u16 = 15002;
pub const MINIO_DATA_PORT: u16 = 9000;
pub const REST_CATALOG_PORT: u16 = 8181;

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

pub fn get_cmd_output(mut cmd: Command, desc: impl ToString) -> String {
    let desc = desc.to_string();
    log::info!("Starting to {}, command: {:?}", &desc, cmd);
    let output = cmd.output().unwrap();
    if output.status.success() {
        log::info!("{} succeed!", desc);
        String::from_utf8(output.stdout).unwrap()
    } else {
        panic!("{} failed: {:?}", desc, output.status);
    }
}

pub fn normalize_test_name(s: impl ToString) -> String {
    s.to_string().replace("::", "__")
}
