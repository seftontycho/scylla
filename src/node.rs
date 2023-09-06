use async_trait::async_trait;

use anyhow::Context;
use tokio::{net::TcpStream, sync::mpsc};

use crate::{
    archive,
    connection::{read_messages, write_messages, Message, Payload},
};

#[async_trait]
pub trait TaskScheduler {
    fn push(&mut self, task: crate::task::Task);
    fn remove(&mut self, archive_id: u64) -> Vec<crate::task::Task>;
    fn notify(&mut self, archive_id: u64);

    async fn next(&mut self) -> Option<crate::task::Task>;
}

pub struct FifoScheduler {
    queue: std::collections::VecDeque<crate::task::Task>,
    waiting: std::collections::HashMap<u64, Vec<crate::task::Task>>,
}

impl Default for FifoScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TaskScheduler for FifoScheduler {
    fn push(&self, task: crate::task::Task) {
        self.queue.push_back(task);
    }

    fn remove(&self, archive_id: u64) -> Vec<crate::task::Task> {
        let mut tasks = Vec::new();

        self.queue.retain(|task| {
            if task.archive_id == archive_id {
                tasks.push(task.clone());
                false
            } else {
                true
            }
        });

        tasks
    }

    fn notify(&self, archive_id: u64) {
        if let Some(tasks) = self.waiting.remove(&archive_id) {
            self.queue.extend(tasks);
        }
    }

    async fn next(&self) -> Option<crate::task::Task> {
        self.queue.pop_front()
    }
}

struct Node<T: TaskScheduler> {
    scheduler: T,
    address: std::net::SocketAddr,
}

impl<T: TaskScheduler + Default> Node<T> {
    pub fn new<S: std::net::ToSocketAddrs>(address: S) -> anyhow::Result<Self> {
        Ok(Self {
            scheduler: Default::default(),
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
    ) -> anyhow::Result<()> {
        while let Some(msg) = read_queue.recv().await {
            match msg.payload {
                Payload::Shutdown => {
                    tracing::info!("Received shutdown");
                    return Ok(());
                }
                Payload::Archive(archive) => {
                    tracing::info!("Received archive");
                    self.handle_archive(archive).await?;
                }
                Payload::ArchiveRequest { id } => {
                    tracing::info!("Received archive request");
                    self.handle_archive_request(id, write_queue.clone()).await?;
                }
                Payload::ArchiveNotFound { id } => {
                    tracing::info!("Received archive not found");
                    self.handle_archive_not_found(id, write_queue.clone())
                        .await?;
                }
                Payload::Task(task) => {
                    tracing::info!("Received task");
                    todo!();
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

    async fn handle_archive(&self, archive: archive::Archive) -> anyhow::Result<()> {
        let exists = archive::archive_exists(archive.id);
        archive::store_archive(&archive).await?;

        if !exists {
            self.scheduler.notify(archive.id);
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
        write_queue: mpsc::Sender<Message>,
    ) -> anyhow::Result<()> {
        let tasks = self.scheduler.remove(id);

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

        self.handle_messages(read_queue_rx, write_queue_tx).await?;

        // join the read and write tasks
        read_task.await?;
        write_task.await?;

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
schedul