use std::io;

use anyhow::Context;
use scylla::Message;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let (stream, addr) = listener.accept().await.context("Failed to accept tcp")?;
    println!("New incoming connection on {addr}");

    let reader = tokio::io::BufReader::new(stream);
    let (tx, mut rx) = mpsc::channel::<Message>(32);

    let read_task = tokio::task::spawn(async move {
        match read_messages(reader, tx).await {
            Ok(_) => println!("Connection closed"),
            Err(e) => println!("Connection closed with error: {:?}", e),
        };
    });

    while let Some(msg) = rx.recv().await {
        match msg.payload {
            scylla::Payload::Archive(archive) => {
                println!("Received archive");
                handle_archive(archive).await?;
            }
            scylla::Payload::Shutdown => {
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
    msg_queue: mpsc::Sender<Message>,
) -> anyhow::Result<()> {
    // this is here because it spins and prints real fast and then we can't see the other messages
    for _ in 0..100 {
        match Message::read(&mut reader).await {
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
                        break;
                    } else {
                        println!("Failed to read message: {:?}", e);
                    }
                } else {
                    println!("Failed to read message: {:?}", e);
                }
            }
        };
    }
    Ok(())
}

async fn handle_archive(archive: scylla::Archive) -> anyhow::Result<()> {
    let filepath = format!("./tmp/{}.tar.gz", archive.id);

    let mut file = tokio::fs::File::create(&filepath).await?;
    file.write_all(&archive.data).await?;
    file.flush().await?;

    decompress_archive(&filepath, format!("./tmp/{}", archive.id).as_str())
        .context("Failed to decompress archive")?;

    Ok(())
}

fn decompress_archive(inpath: &str, out_path: &str) -> anyhow::Result<()> {
    let tar_gz = std::fs::File::open(inpath).context("Failed to open file")?;
    let tar = flate2::read::GzDecoder::new(tar_gz);
    let mut archive = tar::Archive::new(tar);
    archive.unpack(out_path).context("Failed to unpack file")?;

    Ok(())
}
