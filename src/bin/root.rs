use scylla::connection::{Message, Payload};
use std::path::Path;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to node");
    let mut stream = TcpStream::connect("127.0.0.1:8080").await?;
    println!("Connected to node");

    println!("Compressing archive");
    let archive = scylla::archive::compress_dir(Path::new("./testproject"))?;
    let archive_id = archive.id;

    let mut writer = tokio::io::BufWriter::new(&mut stream);

    let payload = Payload::Archive(archive);
    let msg = Message::new(payload);

    println!("Sending message: {:?}", msg);
    msg.write(&mut writer).await?;

    let task = scylla::task::Task::new(archive_id, "main".to_owned(), vec![]);
    let payload = Payload::RunTask(task);
    let msg = Message::new(payload);

    println!("Sending message: {:?}", msg);
    msg.write(&mut writer).await?;

    // let payload = Payload::Shutdown;
    // let msg = Message::new(payload);

    // println!("Sending message: {:?}", msg);
    // msg.write(&mut writer).await?;

    Ok(())
}
