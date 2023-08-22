use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::archive::load_uncompressed_archive;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Task {
    id: uuid::Uuid,
    archive_id: u64,
    binary_name: String,
    arguments: Vec<String>,
}

impl Task {
    pub fn new(archive_id: u64, binary_name: String, arguments: Vec<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            archive_id,
            binary_name,
            arguments,
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let mut path = load_uncompressed_archive(self.archive_id)
            .await?
            .canonicalize()?;

        let current_dir = std::env::current_dir()?;
        std::env::set_current_dir(&path)?;

        let mut build_cmd = tokio::process::Command::new("cargo");
        let build_cmd = build_cmd.args(&["build", "--release"]);

        println!("Building Task {:?}", build_cmd);
        let build = build_cmd.spawn()?;
        println!("build: {:?}", build.stdout);

        path.push("target/release/");
        path.push(&self.binary_name);

        println!("binary path: {:?}", path);

        let mut run_cmd = tokio::process::Command::new(&path);
        let run_cmd = run_cmd.args(&self.arguments);

        println!("Running Task");
        let run = run_cmd.output().await?;
        println!("run: {:?}", run.stdout);

        std::env::set_current_dir(&current_dir)?;

        Ok(())
    }
}
