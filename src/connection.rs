use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Metadata {
    message_id: Uuid,
    time_sent: chrono::DateTime<chrono::Utc>,
    src: std::net::SocketAddr,
    dst: std::net::SocketAddr,
}

impl Metadata {
    pub fn new<S: Into<std::net::SocketAddr>, D: Into<std::net::SocketAddr>>(
        src: S,
        dst: D,
    ) -> Self {
        Self {
            message_id: Uuid::new_v4(),
            time_sent: chrono::Utc::now(),
            src: src.into(),
            dst: dst.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Message {
    // metadata: Metadata,
    pub payload: Payload,
}

impl Message {
    pub fn new(payload: Payload) -> Self {
        Self { payload }
    }

    pub async fn write<T: Unpin + AsyncWriteExt>(&self, writer: &mut T) -> anyhow::Result<()> {
        let serialized = serde_json::to_string(&self).context("Failed to serialize")?;
        let serialized = serialized.as_bytes();

        writer.write_u32_le(serialized.len() as u32).await?;
        writer.write_all(&serialized).await?;
        writer.flush().await?;

        Ok(())
    }

    pub async fn read<T: Unpin + AsyncReadExt>(reader: &mut T) -> anyhow::Result<Self> {
        let length = reader.read_u32_le().await?;
        let mut buf = vec![0; length as usize];

        reader
            .read_exact(&mut buf)
            .await
            .context("Failed to read payload")?;

        let message = serde_json::from_slice(&buf).context("Failed to deserialize")?;

        Ok(message)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[non_exhaustive]
pub enum Payload {
    Shutdown,
    Archive(crate::archive::Archive),
    RunTask(crate::task::Task),
    RequestArchive(u64),
}
