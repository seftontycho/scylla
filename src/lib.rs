use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::TcpStream,
};
use uuid::Uuid;

/*
pub struct Metadata {
    time_sent: u64,
    destination: String,
}
*/

#[derive(Debug)]
pub struct Message {
    //metadata: Metadata,
    pub payload: Payload,
}

impl Message {
    pub fn new(payload: Payload) -> Self {
        Self { payload }
    }

    pub async fn write<T: Unpin + AsyncWriteExt>(&self, writer: &mut T) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&self.payload).context("Failed to serialize")?;
        let payload = payload.as_bytes();

        writer.write_u32_le(payload.len() as u32).await?;
        writer.write_all(&payload).await?;
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

        let payload = String::from_utf8(buf).context("Failed to convert to String")?;
        let payload: Payload = serde_json::from_str(&payload).context("Failed to deserialize")?;

        Ok(Self { payload })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[non_exhaustive]
pub enum Payload {
    Shutdown,
    Archive(Archive),
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Archive {
    pub id: Uuid,
    pub data: Vec<u8>,
}
