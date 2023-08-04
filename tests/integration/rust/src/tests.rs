use icelake_integration_tests::append::test_append_data;
use libtest_mimic::{Arguments, Trial};
use tokio::runtime::Handle;

fn main() {
    // Parse command line arguments
    let args = Arguments::from_args();

    // Create a list of tests and/or benchmarks (in this case: two dummy tests).
    let tests = vec![Trial::test("test_append_data", move || {
        Handle::current().block_on(async { test_append_data().await });
        Ok(())
    })];

    // Run all tests and exit the application appropriatly.
    libtest_mimic::run(&args, tests).exit();
}
