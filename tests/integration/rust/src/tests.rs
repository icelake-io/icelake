use std::process::Command;

use icelake_integration_tests::{
    append::{test_append_data, test_append_data_partition},
    utils::{path_of, run_command},
};
use libtest_mimic::{Arguments, Trial};

fn main() {
    env_logger::init();

    // Parse command line arguments
    let args = Arguments::from_args();

    start_docker_compose();

    run_poetry_update();

    // Create a list of tests and/or benchmarks (in this case: two dummy tests).
    let tests = vec![
        Trial::test("test_append_data", move || {
            test_append_data();
            Ok(())
        }),
        Trial::test("test_append_data_partition", move || {
            test_append_data_partition();
            Ok(())
        }),
    ];

    // Run all tests and exit the application appropriatly.
    let res = libtest_mimic::run(&args, tests);

    shutdown_docker_compose();
    res.exit();
}

fn start_docker_compose() {
    let mut cmd = Command::new("docker");
    cmd.args(["compose", "up", "-d", "--wait", "spark"])
        .current_dir(path_of("../docker"));

    run_command(cmd, "start docker compose");
}

fn shutdown_docker_compose() {
    let mut cmd = Command::new("docker");
    cmd.args(["compose", "down", "-v", "--remove-orphans"])
        .current_dir(path_of("../docker"));

    run_command(cmd, "shutdown docker compose");
}

fn run_poetry_update() {
    let mut cmd = Command::new("poetry");
    cmd.arg("update").current_dir(path_of("../python"));

    run_command(cmd, "poetry update")
}
