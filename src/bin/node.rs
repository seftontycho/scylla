use anyhow::Context;
use std::collections::HashMap;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use scylla::connection::{read_messages, write_messages};

#[tokio::main]
#[tracing::instrument(skip_all)]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt().with_target(false).finish();

    tracing::subscriber::set_global_default(subscriber)?;

    loop {
        let listener = TcpListener::bind("127.0.0.1:8080").await?;
        let (stream, addr) = listener.accept().await.context("Failed to accept tcp")?;
        tracing::info!("New incoming connection on {}", addr);

        let (read, write) = stream.into_split();

        let reader = tokio::io::BufReader::new(read);
        let writer = tokio::io::BufWriter::new(write);

        let (read_queue_tx, read_queue_rx) = mpsc::channel::<scylla::connection::Message>(32);
        let (write_queue_tx, write_queue_rx) = mpsc::channel::<scylla::connection::Message>(32);

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

        handle_messages(read_queue_rx, write_queue_tx).await?;

        // join the read and write tasks
        read_task.await?;
        write_task.await?;
    }
}

#[tracing::instrument(skip_all, name = "HANDLE MESSAGES")]
async fn handle_messages(
    mut read_queue: mpsc::Receiver<scylla::connection::Message>,
    write_queue: mpsc::Sender<scylla::connection::Message>,
) -> anyhow::Result<()> {
    let mut task_queue: HashMap<u64, scylla::task::Task> = HashMap::new();

    while let Some(msg) = read_queue.recv().await {
        match msg.payload {
            scylla::connection::Payload::Archive(archive) => {
                tracing::info!("Received archive: {}", archive.id);
                scylla::archive::store_archive(&archive).await?;

                if let Some(task) = task_queue.get(&archive.id) {
                    tracing::info!("Running waiting task for archive {}", archive.id);
                    let result = task.run().await?;

                    tracing::info!("Sending task {:?} result", task);
                    task_queue.remove(&archive.id);

                    let msg =
                        scylla::connection::Message::new(scylla::connection::Payload::TaskResult {
                            result,
                        });

                    write_queue
                        .send(msg)
                        .await
                        .context("Failed to add message to write queue")?;
                }
            }
            scylla::connection::Payload::RunTask(task) => {
                tracing::info!("Received task: {:?}", task);
                if scylla::archive::archive_exists(task.archive_id) {
                    tracing::info!("Archive found, running task: {:?}", task);
                    let result = task.run().await?;

                    tracing::info!("Sending task {:?} result", task);
                    let msg =
                        scylla::connection::Message::new(scylla::connection::Payload::TaskResult {
                            result,
                        });

                    write_queue
                        .send(msg)
                        .await
                        .context("Failed to add message to write queue")?;
                } else {
                    tracing::info!(
                        "Archive {:?} not found, requesting archive",
                        task.archive_id
                    );

                    let msg = scylla::connection::Message::new(
                        scylla::connection::Payload::RequestArchive {
                            id: task.archive_id,
                        },
                    );

                    task_queue.insert(task.archive_id, task);

                    write_queue
                        .send(msg)
                        .await
                        .context("Failed to add message to write queue")?;
                }
            }
            scylla::connection::Payload::Shutdown => {
                tracing::info!("Received shutdown");
                return Ok(());
            }
            _ => {
                tracing::warn!("Received unknown message");
            }
        }
    }

    Ok(())
}
