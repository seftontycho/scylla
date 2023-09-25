use anyhow::Context;
use std::net::SocketAddr;
use tokio::sync::mpsc;

use crate::{
    connection::{read_messages, write_messages, Message, Payload},
    task::{Task, TaskResult},
};

pub struct Root {
    addresses: Vec<SocketAddr>,
}

impl Root {
    fn new(addresses: Vec<SocketAddr>) -> Self {
        Self { addresses }
    }

    #[tracing::instrument(skip_all, name = "MESSAGE HANDLER")]
    async fn handle_messages(
        &self,
        mut read_queue: mpsc::Receiver<Message>,
        write_queue: mpsc::Sender<Message>,
        result_queue: mpsc::Sender<TaskResult>,
    ) -> anyhow::Result<()> {
        while let Some(msg) = read_queue.recv().await {
            match msg.payload {
                Payload::ArchiveRequest { id } => {
                    tracing::info!("Received archive request for id {}", id);

                    if let Ok(archive) = crate::archive::load_archive(id).await {
                        tracing::info!("Sending archive for id {}", id);

                        write_queue
                            .send(Message::new(Payload::Archive(archive)))
                            .await?;
                    } else {
                        tracing::info!("Sending archive not found for id {}", id);

                        write_queue
                            .send(Message::new(Payload::ArchiveNotFound { id }))
                            .await?;
                    }
                }
                Payload::TaskResult { result } => {
                    tracing::info!("Received task result: {:?}", result);
                    result_queue.send(result).await?;
                }
                Payload::TaskCanceled { id } => {
                    tracing::info!("Received task canceled: {:?}", id);
                    todo!("Handle task canceled");
                }
                Payload::TaskFailed { id } => {
                    tracing::info!("Received task failed: {:?}", id);
                    todo!("Handle task failed");
                }
                msg => {
                    tracing::warn!("Recieved invalid message for root node: {:?}", msg);
                }
            }
        }

        Ok(())
    }

    async fn run(&self) -> anyhow::Result<(mpsc::Sender<Task>, mpsc::Receiver<TaskResult>)> {
        let (task_queue_tx, task_queue_rx) = mpsc::channel(100);
        let (result_queue_tx, result_queue_rx) = mpsc::channel(100);

        let mut streams = Vec::new();

        for addr in self.addresses.iter() {
            let stream = tokio::net::TcpStream::connect(addr)
                .await
                .context("Failed to connect to node at address {addr:?}")?;

            streams.push(stream);
        }

        let step_handle =
            tokio::task::spawn(async move { step(streams, task_queue_rx, result_queue_tx).await });

        Ok((task_queue_tx, result_queue_rx))
    }
}

async fn step(
    mut streams: Vec<tokio::net::TcpStream>,
    task_queue_rx: mpsc::Receiver<Task>,
    result_queue_tx: mpsc::Sender<TaskResult>,
) -> anyhow::Result<()> {
    let num_nodes = streams.len();

    let (readers, writers): (Vec<_>, Vec<_>) = streams
        .into_iter()
        .map(|s| {
            let (read, write) = s.into_split();
            (
                tokio::io::BufReader::new(read),
                tokio::io::BufWriter::new(write),
            )
        })
        .unzip();

    let (read_queue_txs, read_queue_rxs): (Vec<_>, Vec<_>) =
        (0..num_nodes).map(|_| mpsc::channel::<Message>(32)).unzip();

    let (write_queue_txs, write_queue_rxs): (Vec<_>, Vec<_>) =
        (0..num_nodes).map(|_| mpsc::channel::<Message>(32)).unzip();

    let read_tasks = readers
        .into_iter()
        .zip(read_queue_txs.into_iter())
        .map(|(reader, read_queue_tx)| {
            tokio::task::spawn(async move {
                match read_messages(reader, read_queue_tx).await {
                    Ok(_) => tracing::info!("Read connection closed"),
                    Err(e) => tracing::error!("Read connection closed with error: {:?}", e),
                };
            })
        })
        .collect::<Vec<_>>();

    let write_tasks = writers
        .into_iter()
        .zip(write_queue_rxs.into_iter())
        .map(|(writer, write_queue_rx)| {
            tokio::task::spawn(async move {
                match write_messages(writer, write_queue_rx).await {
                    Ok(_) => tracing::info!("Write connection closed"),
                    Err(e) => tracing::error!("Write connection closed with error: {:?}", e),
                };
            })
        })
        .collect::<Vec<_>>();

    let send_write_queue_txs = write_queue_txs.clone();

    let send_task = tokio::task::spawn(async move {
        match send_tasks(send_write_queue_txs, task_queue_rx).await {
            Ok(_) => tracing::info!("Task queue closed"),
            Err(e) => tracing::error!("Task queue closed with error: {:?}", e),
        };
    });

    let result_queue_txs = (0..num_nodes)
        .map(|_| result_queue_tx.clone())
        .collect::<Vec<_>>();

    let handle_tasks = read_queue_rxs
        .into_iter()
        .zip(write_queue_txs.into_iter())
        .zip(result_queue_txs.into_iter())
        .map(|((read_queue_rx, write_queue_tx), result_queue_tx)| {
            tokio::task::spawn(async move {
                match handle_messages(read_queue_rx, write_queue_tx, result_queue_tx).await {
                    Ok(_) => tracing::info!("Message handler closed"),
                    Err(e) => tracing::error!("Message handler closed with error: {:?}", e),
                };
            })
        })
        .collect::<Vec<_>>();

    tracing::info!("Waiting for tasks to finish");

    Ok(())
}

async fn handle_messages(
    mut read_queue: mpsc::Receiver<Message>,
    write_queue: mpsc::Sender<Message>,
    result_queue: mpsc::Sender<TaskResult>,
) -> anyhow::Result<()> {
    while let Some(msg) = read_queue.recv().await {
        match msg.payload {
            Payload::ArchiveRequest { id } => {
                tracing::info!("Received archive request for id {}", id);

                if let Ok(archive) = crate::archive::load_archive(id).await {
                    tracing::info!("Sending archive for id {}", id);

                    write_queue
                        .send(Message::new(Payload::Archive(archive)))
                        .await?;
                } else {
                    tracing::info!("Sending archive not found for id {}", id);

                    write_queue
                        .send(Message::new(Payload::ArchiveNotFound { id }))
                        .await?;
                }
            }
            Payload::TaskResult { result } => {
                tracing::info!("Received task result: {:?}", result);
                result_queue.send(result).await?;
            }
            Payload::TaskCanceled { id } => {
                tracing::info!("Received task canceled: {:?}", id);
                // ADD THIS
            }
            Payload::TaskFailed { id } => {
                tracing::info!("Received task failed: {:?}", id);
                // ADD THIS
            }
            msg => {
                tracing::warn!("Recieved invalid message for root node: {:?}", msg);
            }
        }
    }

    Ok(())
}

#[tracing::instrument(skip_all, name = "TASK SENDER")]
async fn send_tasks(
    write_queues: Vec<mpsc::Sender<Message>>,
    mut task_queue: mpsc::Receiver<Task>,
) -> anyhow::Result<()> {
    let mut next = 0usize;

    while let Some(task) = task_queue.recv().await {
        tracing::info!("Sending task: {:?} to node {}", task, next);

        write_queues
            .get(next)
            .context("Node not found")?
            .send(Message::new(Payload::Task(task)))
            .await
            .context("Failed to add task to write queue")?;

        next = (next + 1) % write_queues.len();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archive::compress_dir;
    use crate::archive::store_archive;
    use crate::task::Task;
    use std::net::Ipv4Addr;
    use std::net::SocketAddrV4;

    #[tokio::test]
    async fn test_root_hello_world() -> anyhow::Result<()> {
        let addr1 = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8081);
        let addr2 = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 8082);
        let root = Root::new(vec![SocketAddr::from(addr1), SocketAddr::from(addr2)]);

        let (task_queue, mut result_queue) = root.run().await?;

        let path = std::path::Path::new("./helloworld");

        let archive = compress_dir(path)?;
        store_archive(&archive).await?;

        let iters = 2;

        for _ in 0..iters {
            task_queue
                .send(Task::new(archive.id, "".to_owned(), vec![]))
                .await?;
        }

        for n in 0..iters {
            let result = result_queue.recv().await.unwrap();

            assert_eq!(result.result, "Hello, world!\n");
            println!("Recieved {n} results");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_root_prime_factors() -> anyhow::Result<()> {
        // needs nodes running at 127.0.0.1:8081 and 127.0.0.1:8082 etc
        let num_nodes = 2;

        let addresses: Vec<_> = (0..num_nodes)
            .map(|n| {
                SocketAddr::from(SocketAddrV4::new(
                    Ipv4Addr::new(127, 0, 0, 1),
                    8081 + n as u16,
                ))
            })
            .collect();

        let root = Root::new(addresses);

        let (task_queue, mut result_queue) = root.run().await?;

        let path = std::path::Path::new("./primes");

        let archive = compress_dir(path)?;
        store_archive(&archive).await?;

        let iters = 100;

        for _ in 0..iters {
            task_queue
                .send(Task::new(archive.id, "".to_owned(), vec![]))
                .await?;
        }

        for n in 0..iters {
            let result = result_queue.recv().await.unwrap();

            assert!(result.result.len() > 2);
            println!("Recieved result \"{:?}\"", result.result);
            println!("Recieved {n} results");
        }

        Ok(())
    }
}
