use anyhow::Context;
use scylla::connection::{read_messages, write_messages, Message, Payload};
use std::{net::SocketAddr, path::Path, str::FromStr};
use tokio::{net::TcpStream, sync::mpsc};

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about=None)]
struct Cli {
    ip: String,
    path: String,
    #[arg(short, long)]
    arguments: Option<Vec<String>>,
    #[arg(short, long, default_value = "false")]
    debug: bool,
}

#[tokio::main]
#[tracing::instrument]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt().with_target(false).finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let args = Cli::parse();

    let ip = SocketAddr::from_str(&args.ip).context("Failed to parse ip address")?;

    let path = Path::new(&args.path);

    if !path.exists() {
        tracing::error!("Path {:?} does not exist", path);
        return Ok(());
    }

    if !path.is_dir() {
        tracing::error!("Path {:?} is not a directory", path);
        return Ok(());
    }

    tracing::info!("Connecting to node: {ip:?}");
    let stream = TcpStream::connect(ip).await?;
    tracing::info!("Connected to node: {ip:?}");

    tracing::info!("Splitting Stream");
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

    tracing::info!("Compressing archive");
    let archive = scylla::archive::compress_dir(path)?;

    tracing::info!("Storing archive");
    scylla::archive::store_archive(&archive).await?;
    let archive_id = archive.id;

    let task = scylla::task::Task::new(
        archive_id,
        "main".to_owned(),
        args.arguments.unwrap_or(vec![]),
    );

    let payload = Payload::RunTask(task);
    let msg = Message::new(payload);

    tracing::info!("Sending task to node on {ip:?}");
    write_queue_tx.send(msg).await?;

    handle_messages(read_queue_rx, write_queue_tx).await?;

    // join the read and write tasks
    read_task.await?;
    write_task.await?;

    Ok(())
}

#[tracing::instrument(skip_all, name = "HANDLE MESSAGES")]
async fn handle_messages(
    mut read_queue: mpsc::Receiver<Message>,
    write_queue: mpsc::Sender<Message>,
) -> anyhow::Result<Option<String>> {
    tracing::info!("Starting");

    while let Some(msg) = read_queue.recv().await {
        match msg.payload {
            Payload::RequestArchive { id } => {
                tracing::info!("Received request for archive {}", id);

                if scylla::archive::archive_exists(id) {
                    tracing::info!("Archive {} exists", id);
                    let archive = scylla::archive::load_archive(id).await?;
                    let msg = Message::new(Payload::Archive(archive));

                    tracing::info!("Sending archive {}", id);
                    write_queue.send(msg).await?;
                } else {
                    tracing::info!("Archive {} does not exist", id);
                    let msg = Message::new(Payload::ArchiveNotFound { id });

                    tracing::info!("Sending archive not found {}", id);
                    write_queue.send(msg).await?;
                }
            }
            Payload::TaskResult { result } => {
                tracing::info!("Received task result: {}", result);
                return Ok(Some(result));
            }
            _ => {
                tracing::error!("Received enexpected message {msg:?}");
            }
        }
    }

    Ok(None)
}
