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
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    let ip = SocketAddr::from_str(&args.ip).context("Failed to parse ip address")?;

    let path = Path::new(&args.path);

    if !path.exists() {
        println!("Path does not exist");
        return Ok(());
    }

    if !path.is_dir() {
        println!("Path is not a directory");
        return Ok(());
    }

    println!("Connecting to node: {ip:?}");
    let stream = TcpStream::connect(ip).await?;
    println!("Connected to node");

    println!("Splitting Stream");
    let (read, write) = stream.into_split();

    let reader = tokio::io::BufReader::new(read);
    let writer = tokio::io::BufWriter::new(write);

    let (read_queue_tx, read_queue_rx) = mpsc::channel::<scylla::connection::Message>(32);
    let (write_queue_tx, write_queue_rx) = mpsc::channel::<scylla::connection::Message>(32);

    let read_task = tokio::task::spawn(async move {
        match read_messages(reader, read_queue_tx).await {
            Ok(_) => println!("Read connection closed"),
            Err(e) => println!("Read connection closed with error: {:?}", e),
        };
    });

    let write_task = tokio::task::spawn(async move {
        match write_messages(writer, write_queue_rx).await {
            Ok(_) => println!("Write connection closed"),
            Err(e) => println!("Write connection closed with error: {:?}", e),
        };
    });

    println!("Compressing archive");
    let archive = scylla::archive::compress_dir(path)?;
    scylla::archive::store_archive(&archive).await?;
    let archive_id = archive.id;

    let task = scylla::task::Task::new(
        archive_id,
        "main".to_owned(),
        args.arguments.unwrap_or(vec![]),
    );

    let payload = Payload::RunTask(task);
    let msg = Message::new(payload);

    println!("Sending task to node");
    write_queue_tx.send(msg).await?;

    handle_messages(read_queue_rx, write_queue_tx).await?;

    // join the read and write tasks
    read_task.await?;
    write_task.await?;

    Ok(())
}

async fn handle_messages(
    mut read_queue: mpsc::Receiver<Message>,
    write_queue: mpsc::Sender<Message>,
) -> anyhow::Result<Option<String>> {
    while let Some(msg) = read_queue.recv().await {
        match msg.payload {
            Payload::RequestArchive { id } => {
                println!("Received request for archive {}", id);
                if scylla::archive::archive_exists(id) {
                    let archive = scylla::archive::load_archive(id).await?;
                    let msg = Message::new(Payload::Archive(archive));
                    write_queue.send(msg).await?;
                } else {
                    println!("Archive {} does not exist", id);
                    let msg = Message::new(Payload::ArchiveNotFound { id });
                    write_queue.send(msg).await?;
                }
            }
            Payload::TaskResult { result } => {
                println!("Received task result: {}", result);
                return Ok(Some(result));
            }
            _ => {
                println!("Received enexpected message {msg:?}");
            }
        }
    }

    Ok(None)
}
