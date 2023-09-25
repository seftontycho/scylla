use std::collections::HashMap;

use anyhow::Context;
use clap::Parser;
use tokio::{net::TcpStream, sync::mpsc};

use scylla::{
    archive,
    connection::{read_messages, write_messages, Message, Payload},
    task::TaskResult,
};

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    address: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::FmtSubscriber::builder()
        .pretty()
        .with_line_number(false)
        .with_file(false)
        .init();

    let args = Args::parse();

    tracing::info!("Starting node on {}", args.address);
    let node = Node::new(args.address).context("Failed to create node")?;
    node.run().await?;

    Ok(())
}

#[tracing::instrument(skip_all, name = "TASK RUNNER")]
async fn run_tasks(
    mut task_queue: mpsc::Receiver<scylla::task::Task>,
    write_queue: mpsc::Sender<Message>,
) -> anyhow::Result<()> {
    loop {
        match task_queue.recv().await {
            Some(task) => {
                tracing::info!("Running {:?}", task);
                let msg = match task.run().await {
                    Ok(result) => {
                        tracing::info!("{:?} completed", task);
                        Message::new(Payload::TaskResult {
                            result: TaskResult {
                                id: task.id,
                                result,
                            },
                        })
                    }
                    Err(e) => {
                        tracing::error!("{:?} failed with error {:?}", task, e);
                        Message::new(Payload::TaskFailed { id: task.id })
                    }
                };

                write_queue
                    .send(msg)
                    .await
                    .context("Failed to add message to write queue")?;
            }
            None => {
                break Ok(());
            }
        };
    }
}

pub struct Node {
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

    pub async fn run(&self) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(self.address)
            .await
            .context("Failed to bind to address")?;

        loop {
            let (stream, addr) = listener.accept().await?;
            tracing::info!("Accepted connection from {}", addr);

            step(stream).await?;
        }
    }
}

#[tracing::instrument(skip_all, name = "MESSAGE HANDLER")]
async fn handle_messages(
    mut read_queue: mpsc::Receiver<Message>,
    write_queue: mpsc::Sender<Message>,
    task_queue: mpsc::Sender<scylla::task::Task>,
) -> anyhow::Result<()> {
    let mut waiting: HashMap<u64, Vec<scylla::task::Task>> = HashMap::new();

    while let Some(msg) = read_queue.recv().await {
        match msg.payload {
            Payload::Shutdown => {
                tracing::info!("Received shutdown");
                return Ok(());
            }
            Payload::Archive(archive) => {
                tracing::info!("Received archive");
                handle_archive(archive, &mut waiting, task_queue.clone()).await?;
            }
            Payload::ArchiveNotFound { id } => {
                tracing::info!("Received archive not found");
                handle_archive_not_found(id, &mut waiting, write_queue.clone()).await?;
            }
            Payload::Task(task) => {
                tracing::info!("Received task");

                if archive::archive_exists(task.archive_id) {
                    tracing::info!("Archive exists, queueing task: {}", task.id);

                    task_queue
                        .send(task)
                        .await
                        .context("Failed to add task to queue")?;
                } else {
                    tracing::info!("Archive does not exist, task {} is waiting", task.id);

                    let msg = Message::new(Payload::ArchiveRequest {
                        id: task.archive_id,
                    });

                    waiting.entry(task.archive_id).or_default().push(task);

                    write_queue
                        .send(msg)
                        .await
                        .context("Failed to add message to write queue")?;
                }
            }
            msg => {
                tracing::warn!("Recieved invalid message for remote node: {:?}", msg);
            }
        }

        tracing::info!("Waiting: {:?}", waiting);
    }

    Ok(())
}

async fn handle_archive(
    archive: archive::Archive,
    waiting: &mut HashMap<u64, Vec<scylla::task::Task>>,
    task_queue: mpsc::Sender<scylla::task::Task>,
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

async fn handle_archive_not_found(
    id: u64,
    waiting: &mut HashMap<u64, Vec<scylla::task::Task>>,
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

async fn step(stream: TcpStream) -> anyhow::Result<()> {
    let (read, write) = stream.into_split();

    let reader = tokio::io::BufReader::new(read);
    let writer = tokio::io::BufWriter::new(write);

    let (read_queue_tx, read_queue_rx) = mpsc::channel::<Message>(32);
    let (write_queue_tx, write_queue_rx) = mpsc::channel::<Message>(32);
    let (task_queue_tx, task_queue_rx) = mpsc::channel::<scylla::task::Task>(32);

    let read_task = tokio::task::spawn(async move {
        match read_messages(reader, read_queue_tx).await {
            Ok(_) => tracing::info!("Read handler closed"),
            Err(e) => tracing::error!("Read handler closed with error: {:?}", e),
        };
    });

    let write_task = tokio::task::spawn(async move {
        match write_messages(writer, write_queue_rx).await {
            Ok(_) => tracing::info!("Write handler closed"),
            Err(e) => tracing::error!("Write handler closed with error: {:?}", e),
        };
    });

    let write_queue = write_queue_tx.clone();

    let run_task = tokio::task::spawn(async move {
        match run_tasks(task_queue_rx, write_queue).await {
            Ok(_) => tracing::info!("Task handler closed"),
            Err(e) => tracing::error!("Task handler closed with error: {:?}", e),
        };
    });

    handle_messages(read_queue_rx, write_queue_tx, task_queue_tx).await?;

    Ok(())
}
