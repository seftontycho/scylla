use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub struct Message {
    // metadata: Metadata,
    pub payload: Payload,
}

impl Message {
    #[inline]
    #[must_use]
    pub const fn new(payload: Payload) -> Self {
        Self { payload }
    }

    #[inline]
    pub async fn write<T: Unpin + AsyncWriteExt>(&self, writer: &mut T) -> anyhow::Result<()> {
        let serialized = serde_json::to_string(&self).context("Failed to serialize")?;
        let serialized = serialized.as_bytes();

        writer
            .write_u32_le(u32::try_from(serialized.len())?)
            .await?;
        writer.write_all(serialized).await?;
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
