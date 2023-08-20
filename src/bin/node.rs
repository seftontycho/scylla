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

    let (tx, mut rx) = mpsc::channel::<Message>(32);

    tokio::task::spawn(async move {
        read_messages(stream, tx).await.unwrap();
    });

    loop {
        match rx.recv().await {
            Some(msg) => match msg.payload {
                scylla::Payload::Archive(archive) => {
                    println!("Received archive: {:?}", archive);
                    handle_archive(archive).await?;
                }
            },
            None => {
                println!("Exiting");
                break;
            }
        }
    }

    Ok(())
}

async fn read_messages(
    mut stream: TcpStream,
    msg_queue: mpsc::Sender<Message>,
) -> anyhow::Result<()> {
    loop {
        stream.readable().await.context("Stream not readable")?;

        match Message::read(&mut stream).await {
            Ok(msg) => {
                println!("Received message");

                msg_queue
                    .send(msg)
                    .await
                    .context("Failed to add message to queue")?
            }
            Err(e) => {
                return Err(e).context("Failed to read message");
            }
        };
    }
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
