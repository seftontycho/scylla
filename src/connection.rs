use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
};

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
    ArchiveRequest { id: u64 },
    ArchiveNotFound { id: u64 },
    Task(crate::task::Task),
    TaskResult { result: String },
    TaskCanceled { id: uuid::Uuid },
    TaskFailed { id: uuid::Uuid },
}

#[tracing::instrument(skip_all, name = "MESSAGE WRITER")]
pub async fn write_messages(
    mut writer: tokio::io::BufWriter<OwnedWriteHalf>,
    mut rx: tokio::sync::mpsc::Receiver<Message>,
) -> anyhow::Result<()> {
    tracing::info!("Starting");

    while let Some(msg) = rx.recv().await {
        tracing::info!("Sending message {msg:?}");

        msg.write(&mut writer)
            .await
            .context("Failed to write message")?;
    }

    Ok(())
}

#[tracing::instrument(skip_all, name = "MESSAGE READER")]
pub async fn read_messages(
    mut reader: tokio::io::BufReader<OwnedReadHalf>,
    tx: tokio::sync::mpsc::Sender<Message>,
) -> anyhow::Result<()> {
    tracing::info!("Starting");

    loop {
        match Message::read(&mut reader).await {
            Ok(msg) => {
                tracing::info!("Received message {msg:?}");

                tx.send(msg)
                    .await
                    .context("Failed to add message to read queue")?;
            }
            // if we get a tokio::ErrorKind::UnexpectedEof error we break otherwise print the error
            Err(e) => {
                if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                    if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Ok(());
                    }
                }

                return Err(e).context("Failed to read message");
            }
        };
    }
}
