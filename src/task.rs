use serde::{Deserialize, Serialize};

use crate::archive::load_uncompressed_archive;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub id: uuid::Uuid,
    pub archive_id: u64,
    binary_name: String,
    arguments: Vec<String>,
}

impl Task {
    #[inline]
    #[must_use]
    pub fn new(archive_id: u64, binary_name: String, arguments: Vec<String>) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            archive_id,
            binary_name,
            arguments,
        }
    }

    pub async fn run(&self) -> anyhow::Result<String> {
        let path = load_uncompressed_archive(self.archive_id)
            .await?
            .canonicalize()?;

        let build_output = tokio::process::Command::new("cargo")
            .current_dir(&path)
            .args(["run", "--release"])
            .output()
            .await?;

        tracing::debug!("build_output: {:?}", build_output);

        Ok(String::from_utf8_lossy(&build_output.stdout).to_string())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TaskResult {
    pub id: uuid::Uuid,
    pub result: String,
}
