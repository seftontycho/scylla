use scylla::connection::{Message, Metadata, Payload};
use std::net::SocketAddr;
use std::path::Path;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to node");
    let dst = SocketAddr::from(([127, 0, 0, 1], 8080));
    let mut stream = TcpStream::connect(dst).await?;
    let src = stream.local_addr()?;
    println!("Connected to node");

    println!("Compressing archive");
    let archive = scylla::archive::compress_dir(Path::new("./testproject"))?;
    let archive_id = archive.id;

    let mut writer = tokio::io::BufWriter::new(&mut stream);

    let metadata = Metadata::new(src, dst);
    let payload = Payload::Archive(archive);
    let msg = Message::new(metadata, payload);

    println!("Sending message: {:?}", msg);
    msg.write(&mut writer).await?;

    let metadata = Metadata::new(src, dst);
    let task = scylla::task::Task::new(archive_id, "main".to_owned(), vec![]);
    let payload = Payload::RunTask(task);
    let msg = Message::new(metadata, payload);

    println!("Sending message: {:?}", msg);
    msg.write(&mut writer).await?;

    // let metadata = Metadata::new(src, dst);
    // let payload = Payload::Shutdown;
    // let msg = Message::new(metadata, payload);

    // println!("Sending message: {:?}", msg);
    // msg.write(&mut writer).await?;

    Ok(())
}
