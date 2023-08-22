use anyhow::Context;
use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let (stream, addr) = listener.accept().await.context("Failed to accept tcp")?;
    println!("New incoming connection on {addr}");

    let reader = tokio::io::BufReader::new(stream);
    let (tx, mut rx) = mpsc::channel::<scylla::connection::Message>(32);

    let read_task = tokio::task::spawn(async move {
        match read_messages(reader, tx).await {
            Ok(_) => println!("Connection closed"),
            Err(e) => println!("Connection closed with error: {:?}", e),
        };
    });

    while let Some(msg) = rx.recv().await {
        match msg.payload {
            scylla::connection::Payload::Archive(archive) => {
                println!("Received archive");
                scylla::archive::store_archive(&archive).await?;
            }
            scylla::connection::Payload::RunTask(task) => {
                println!("Received task");
                let result = task.run().await?;
                println!("Task result: {:?}", result);
            }
            scylla::connection::Payload::Shutdown => {
                println!("Received shutdown");
                break;
            }
            _ => {
                println!("Received unknown message");
            }
        }
    }

    read_task.abort();
    println!("Exiting");

    Ok(())
}

async fn read_messages(
    mut reader: tokio::io::BufReader<TcpStream>,
    msg_queue: mpsc::Sender<scylla::connection::Message>,
) -> anyhow::Result<()> {
    // this is here because it spins and prints real fast and then we can't see the other messages
    loop {
        match scylla::connection::Message::read(&mut reader).await {
            Ok(msg) => {
                println!("Received message {msg:?}");

                msg_queue
                    .send(msg)
                    .await
                    .context("Failed to add message to queue")?;
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
