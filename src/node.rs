use std::collections::HashMap;

use anyhow::Context;
use tokio::{net::TcpStream, sync::mpsc};

use crate::{
    archive,
    connection::{read_messages, write_messages, Message, Payload},
    task,
};

async fn run_tasks(
    mut task_queue: mpsc::Receiver<crate::task::Task>,
    write_queue: mpsc::Sender<Message>,
) -> anyhow::Result<()> {
    loop {
        match task_queue.recv().await {
            Some(task) => {
                tracing::info!("Running task {:?}", task);
                let msg = match task.run().await {
                    Ok(result) => {
                        tracing::info!("Task {:?} completed", task);
                        Message::new(Payload::TaskResult { result })
                    }
                    Err(e) => {
                        tracing::error!("Task {:?} failed with error {:?}", task, e);
                        Message::new(Payload::TaskFailed { id: task.id })
                    }
                };

                write_queue
                    .send(msg)
                    .await
                    .context("Failed to add message to write queue")?;
            }
            None => {
                tracing::info!("Task queue closed");
                break Ok(());
            }
        };
    }
}

struct Node {
    address: std::net::SocketAddr,
}

impl Node {
    pub fn new<S: std::net::ToSocketAddrs>(address: S) -> anyhow::Result<Self> {
        Ok(Self {
            address: address
                .to_socket_addrs()?
                .next()
                .context("No valid address")?,
        })
    }

    #[tracing::instrument(skip_all, name = "MESSAGE HANDLER")]
    async fn handle_messages(
        &self,
        mut read_queue: mpsc::Receiver<Message>,
        write_queue: mpsc::Sender<Message>,
        task_queue: mpsc::Sender<crate::task::Task>,
    ) -> anyhow::Result<()> {
        let mut waiting: HashMap<u64, Vec<crate::task::Task>> = HashMap::new();

        while let Some(msg) = read_queue.recv().await {
            match msg.payload {
                Payload::Shutdown => {
                    tracing::info!("Received shutdown");
                    return Ok(());
                }
                Payload::Archive(archive) => {
                    tracing::info!("Received archive");
                    self.handle_archive(archive, &mut waiting, task_queue.clone())
                        .await?;
                }
                Payload::ArchiveRequest { id } => {
                    tracing::info!("Received archive request");
                    self.handle_archive_request(id, write_queue.clone()).await?;
                }
                Payload::ArchiveNotFound { id } => {
                    tracing::info!("Received archive not found");
                    self.handle_archive_not_found(id, &mut waiting, write_queue.clone())
                        .await?;
                }
                Payload::Task(task) => {
                    tracing::info!("Received task");
                    task_queue
                        .send(task)
                        .await
                        .context("Failed to add task to queue")?;
                }
                Payload::TaskResult { result } => {
                    tracing::info!("Received task result");
                    todo!();
                }
                _ => {
                    tracing::warn!("Received unknown message");
                }
            }
        }

        Ok(())
    }

    async fn handle_archive(
        &self,
        archive: archive::Archive,
        waiting: &mut HashMap<u64, Vec<crate::task::Task>>,
        task_queue: mpsc::Sender<crate::task::Task>,
    ) -> anyhow::Result<()> {
        let exists = archive::archive_exists(archive.id);
        archive::store_archive(&archive).await?;

        if !exists {
            if let Some(tasks) = waiting.remove(&archive.id) {
                for task in tasks {
                    task_queue
                        .send(task)
                        .await
                        .context("Failed to add task to queue")?;
                }
            }
        }

        Ok(())
    }

    async fn handle_archive_request(
        &self,
        id: u64,
        write_queue: mpsc::Sender<Message>,
    ) -> anyhow::Result<()> {
        let msg = if archive::archive_exists(id) {
            let archive = archive::load_archive(id).await?;
            Message::new(Payload::Archive(archive))
        } else {
            Message::new(Payload::ArchiveNotFound { id })
        };

        write_queue.send(msg).await?;

        Ok(())
    }

    async fn handle_archive_not_found(
        &self,
        id: u64,
        waiting: &mut HashMap<u64, Vec<crate::task::Task>>,
        write_queue: mpsc::Sender<Message>,
    ) -> anyhow::Result<()> {
        let tasks = waiting.remove(&id).unwrap_or_default();

        for task in tasks {
            tracing::warn!("Archive {} not found, canceling task {:?}", id, task);

            let msg = Message::new(Payload::TaskCanceled { id: task.id });

            write_queue
                .send(msg)
                .await
                .context("Failed to add message to write queue")?;
        }

        Ok(())
    }

    pub async fn step(&self, stream: TcpStream) -> anyhow::Result<()> {
        let (read, write) = stream.into_split();

        let reader = tokio::io::BufReader::new(read);
        let writer = tokio::io::BufWriter::new(write);

        let (read_queue_tx, read_queue_rx) = mpsc::channel::<Message>(32);
        let (write_queue_tx, write_queue_rx) = mpsc::channel::<Message>(32);
        let (task_queue_tx, task_queue_rx) = mpsc::channel::<crate::task::Task>(32);

        let read_task = tokio::task::spawn(async move {
            match read_messages(reader, read_queue_tx).await {
                Ok(_) => tracing::info!("Read connection closed"),
                Err(e) => tracing::error!("Read connection closed with error: {:?}", e),
            };
        });

        let write_task = tokio::task::spawn(async move {
            match write_messages(writer, write_queue_rx).await {
                Ok(_) => tracing::info!("Write connection closed"),
                Err(e) => tracing::error!("Write connection closed with error: {:?}", e),
            };
        });

        let write_queue = write_queue_tx.clone();

        let run_task = tokio::task::spawn(async move {
            match run_tasks(task_queue_rx, write_queue).await {
                Ok(_) => tracing::info!("Task queue closed"),
                Err(e) => tracing::error!("Task queue closed with error: {:?}", e),
            };
        });

        self.handle_messages(read_queue_rx, write_queue_tx, task_queue_tx)
            .await?;

        // join the read and write tasks
        read_task.await?;
        write_task.await?;
        run_task.await?;

        Ok(())
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(self.address)
            .await
            .context("Failed to bind to address")?;

        loop {
            let (stream, addr) = listener.accept().await?;
            tracing::info!("Accepted connection from {}", addr);

            self.step(stream).await?;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_node_shutdown() {
        let address = "127.0.0.1:8080";

        let root = Node::new(address);
        let node = Node::new(address);
    }
}
