use anyhow::Context;
use std::collections::HashMap;
use std::io;
use tokio::net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    loop {
        let listener = TcpListener::bind("127.0.0.1:8080").await?;
        let (stream, addr) = listener.accept().await.context("Failed to accept tcp")?;
        println!("New incoming connection on {addr}");

        let (read, write) = stream.into_split();

        let reader = tokio::io::BufReader::new(read);
        let writer = tokio::io::BufWriter::new(write);

        let (read_queue_tx, read_queue_rx) = mpsc::channel::<scylla::connection::Message>(32);
        let (write_queue_tx, write_queue_rx) = mpsc::channel::<scylla::connection::Message>(32);

        let read_task = tokio::task::spawn(async move {
            match read_messages(reader, read_queue_tx).await {
                Ok(_) => println!("Connection closed"),
                Err(e) => println!("Connection closed with error: {:?}", e),
            };
        });

        let write_task = tokio::task::spawn(async move {
            match write_messages(writer, write_queue_rx).await {
                Ok(_) => println!("Connection closed"),
                Err(e) => println!("Connection closed with error: {:?}", e),
            };
        });

        handle_messages(read_queue_rx, write_queue_tx).await?;

        // join the read and write tasks
        read_task.await?;
        write_task.await?;
    }
}

async fn handle_messages(
    mut read_queue: mpsc::Receiver<scylla::connection::Message>,
    write_queue: mpsc::Sender<scylla::connection::Message>,
) -> anyhow::Result<()> {
    let mut task_queue: HashMap<u64, scylla::task::Task> = HashMap::new();

    while let Some(msg) = read_queue.recv().await {
        match msg.payload {
            scylla::connection::Payload::Archive(archive) => {
                println!("Received archive");
                scylla::archive::store_archive(&archive).await?;

                if let Some(task) = task_queue.get(&archive.id) {
                    let result = task.run().await?;
                    task_queue.remove(&archive.id);
                    println!("Task result: {:?}", result);
                }
            }
            scylla::connection::Payload::RunTask(task) => {
                if scylla::archive::archive_exists(task.archive_id).await {
                    let result = task.run().await?;
                    println!("Task result: {:?}", result);
                } else {
                    let msg = scylla::connection::Message {
                        payload: scylla::connection::Payload::RequestArchive(task.archive_id),
                    };

                    task_queue.insert(task.archive_id, task);

                    println!("Archive not found, requesting archive");
                    write_queue
                        .send(msg)
                        .await
                        .context("Failed to add message to write queue")?;
                }
            }
            scylla::connection::Payload::Shutdown => {
                println!("Received shutdown");
                return Ok(());
            }
            _ => {
                println!("Received unknown message");
            }
        }
    }

    Ok(())
}

async fn write_messages(
    mut writer: tokio::io::BufWriter<OwnedWriteHalf>,
    mut rx: mpsc::Receiver<scylla::connection::Message>,
) -> anyhow::Result<()> {
    loop {
        let msg = rx.recv().await.context("Failed to receive message")?;

        println!("Sending message {msg:?}");

        msg.write(&mut writer)
            .await
            .context("Failed to write message")?;
    }
}

async fn read_messages(
    mut reader: tokio::io::BufReader<OwnedReadHalf>,
    tx: mpsc::Sender<scylla::connection::Message>,
) -> anyhow::Result<()> {
    loop {
        match scylla::connection::Message::read(&mut reader).await {
            Ok(msg) => {
                println!("Received message {msg:?}");

                tx.send(msg)
                    .await
                    .context("Failed to add message to read queue")?;
            }
            // if we get a tokio::ErrorKind::UnexpectedEof error we break otherwise print the error
            Err(e) => {
                if let Some(io_err) = e.downcast_ref::<io::Error>() {
                    if io_err.kind() == io::ErrorKind::UnexpectedEof {
                        return Ok(());
                    }
                }

                return Err(e).context("Failed to read message");
            }
        };
    }
}
