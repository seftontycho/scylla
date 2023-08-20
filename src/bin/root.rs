use scylla::{Archive, Message, Payload};
use std::path::Path;
use tokio::{io::AsyncReadExt, net::TcpStream};
use uuid::Uuid;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:8080").await?;

    bundle_dir(Path::new("./testfolder"), Path::new("./tmp/bundle.tar.gz")).await?;

    let mut file = tokio::fs::File::open("./tmp/bundle.tar.gz").await?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await?;

    let payload = Payload::Archive(Archive {
        id: Uuid::new_v4(),
        data: buf,
    });

    let msg = Message::new(payload);

    println!("Sending message: {:?}", msg);

    msg.write(&mut stream).await?;

    Ok(())
}

async fn bundle_dir(in_path: &Path, out_path: &Path) -> anyhow::Result<()> {
    let tar_gz = std::fs::File::create(out_path)?;
    let enc = flate2::write::GzEncoder::new(tar_gz, flate2::Compression::default());
    let mut tar = tar::Builder::new(enc);

    tar.append_dir_all(in_path, in_path)?;
    tar.finish()?;

    Ok(())
}
