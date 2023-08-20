use std::path::Path;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use uuid::Uuid;

pub struct Metadata {
    time_sent: u64,
    destination: String,
}

#[derive(Debug)]
pub struct Message {
    //metadata: Metadata,
    pub payload: Payload,
}

impl Message {
    pub fn new(payload: Payload) -> Self {
        Self { payload }
    }

    pub async fn write<T: Unpin + AsyncWrite>(&self, stream: &mut T) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&self.payload).context("Failed to serialize")?;
        let payload = payload.as_bytes();

        let mut writer = BufWriter::new(stream);

        writer.write_u32_le(payload.len() as u32).await?;
        writer.write_all(&payload).await?;
        writer.flush().await?;

        Ok(())
    }

    pub async fn read<T: Unpin + AsyncRead>(stream: &mut T) -> anyhow::Result<Self> {
        let mut reader = BufReader::new(stream);

        let length = reader.read_u32_le().await?;

        let mut buf = vec![0; length as usize];
        reader.read_exact(&mut buf).await?;

        let payload = String::from_utf8(buf)?;
        let payload: Payload = serde_json::from_str(&payload).context("Failed to deserialize")?;

        Ok(Self { payload })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Payload {
    Archive(Archive),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Archive {
    pub id: Uuid,
    pub data: Vec<u8>,
}
