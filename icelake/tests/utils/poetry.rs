use crate::utils::run_command;
use std::ffi::OsStr;
use std::process::Command;

const POETRY_CMD: &str = "poetry";

pub struct Poetry {
    proj_dir: String,
}

impl Poetry {
    pub fn new<S: ToString>(s: S) -> Self {
        let proj_dir = s.to_string();

        Self { proj_dir }
    }

    pub fn run_file(
        &self,
        file: &str,
        other_args: impl IntoIterator<Item = impl AsRef<OsStr>>,
        desc: impl ToString,
    ) {
        let mut cmd = Command::new(POETRY_CMD);
        cmd.current_dir(self.proj_dir.as_str());

        cmd.args(vec!["run", "python", file]);
        cmd.args(other_args);

        run_command(cmd, desc)
    }
}
