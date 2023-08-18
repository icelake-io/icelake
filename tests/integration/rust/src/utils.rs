use std::{path::Path, process::Command};

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

pub fn path_of<P: AsRef<Path>>(relative_path: P) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(relative_path)
        .to_str()
        .unwrap()
        .to_string()
}
