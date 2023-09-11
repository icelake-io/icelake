use std::process::Command;

use super::{get_cmd_output, run_command};

pub struct DockerCompose {
    project_name: String,
    docker_compose_dir: String,
}

impl DockerCompose {
    pub fn new(project_name: impl ToString, docker_compose_dir: impl ToString) -> Self {
        Self {
            project_name: project_name.to_string(),
            docker_compose_dir: docker_compose_dir.to_string(),
        }
    }

    pub fn run(&self) {
        let mut cmd = Command::new("docker");
        let absolute_dir = format!(
            "{}/../testdata/docker/{}",
            env!("CARGO_MANIFEST_DIR"),
            self.docker_compose_dir
        );
        cmd.current_dir(absolute_dir.as_str());

        cmd.args(vec![
            "compose",
            "-p",
            self.project_name.as_str(),
            "up",
            "-d",
            "--wait",
            "--timeout",
            "1200000",
        ]);

        run_command(
            cmd,
            format!(
                "Starting docker compose in {absolute_dir}, project name: {}",
                self.project_name
            ),
        )
    }

    pub fn get_container_ip(&self, service_name: impl AsRef<str>) -> String {
        let container_name = format!("{}-{}-1", self.project_name, service_name.as_ref());
        let mut cmd = Command::new("docker");
        cmd.arg("inspect")
            .arg("-f")
            .arg("{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}")
            .arg(&container_name);

        get_cmd_output(cmd, format!("Get container ip of {container_name}"))
            .trim()
            .to_string()
    }
}

impl Drop for DockerCompose {
    fn drop(&mut self) {
        let mut cmd = Command::new("docker");
        let absolute_dir = format!(
            "{}/../testdata/docker/{}",
            env!("CARGO_MANIFEST_DIR"),
            self.docker_compose_dir
        );
        cmd.current_dir(absolute_dir.as_str());

        cmd.args(vec![
            "compose",
            "-p",
            self.project_name.as_str(),
            "down",
            "-v",
            "--remove-orphans",
        ]);

        // run_command(
        //     cmd,
        //     format!(
        //         "Stopping docker compose in {absolute_dir}, project name: {}",
        //         self.project_name
        //     ),
        // )
    }
}
