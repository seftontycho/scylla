use anyhow::Context;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use std::path::PathBuf;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    path::Path,
};

const ARCHIVE_DIR: &str = "./store/archives";
const UNCOMPRESSED_DIR: &str = "./store/uncompressed";

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct Archive {
    pub id: u64,
    pub data: Vec<u8>,
}

impl Debug for Archive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Archive")
            .field("id", &self.id)
            .field("data length", &self.data.len())
            .finish()
    }
}

impl Archive {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            id: hash_data(&data),
            data,
        }
    }
}

fn hash_data(data: &[u8]) -> u64 {
    let mut s = DefaultHasher::new();
    data.hash(&mut s);
    s.finish()
}

pub fn compress_dir(in_path: &Path) -> anyhow::Result<Archive> {
    let mut compressed_data = Vec::new();

    {
        let encoder = GzEncoder::new(&mut compressed_data, flate2::Compression::default());
        let mut tar = tar::Builder::new(encoder);

        tar.append_dir_all(in_path, in_path)?;
        tar.finish()?;
    }

    Ok(Archive::new(compressed_data))
}

pub async fn uncompress_archive(archive: &Archive) -> anyhow::Result<()> {
    let output_path = Path::new(UNCOMPRESSED_DIR).join(format!("{}", archive.id));
    println!("uncompress output path: {:?}", output_path);

    if !output_path.exists() {
        println!("uncompressing archive");
        tokio::fs::create_dir_all(&output_path).await?;

        let decoder = GzDecoder::new(&archive.data[..]);
        let mut tar = tar::Archive::new(decoder);

        tar.unpack(output_path)
            .context("Failed to unpack archive")?;
    }

    Ok(())
}

pub async fn store_archive(archive: &Archive) -> anyhow::Result<PathBuf> {
    let archive_dir = Path::new(ARCHIVE_DIR);
    let archive_path = archive_dir.join(format!("{}.tar.gz", archive.id));

    if !archive_path.exists() {
        tokio::fs::create_dir_all(archive_dir).await?;
        let mut file = tokio::fs::File::create(&archive_path).await?;
        file.write_all(&archive.data).await?;
    }

    Ok(archive_path)
}

pub async fn load_archive(archive_id: u64) -> anyhow::Result<Archive> {
    let archive_dir = Path::new(ARCHIVE_DIR);
    let archive_path = archive_dir.join(format!("{}.tar.gz", archive_id));

    println!("Loading archive: {:?}", archive_path);

    let mut file = tokio::fs::File::open(&archive_path).await?;
    let mut data = Vec::new();
    file.read_to_end(&mut data).await?;

    println!("archive data: {:?}", data);

    Ok(Archive::new(data))
}

pub async fn load_uncompressed_archive(archive_id: u64) -> anyhow::Result<PathBuf> {
    let uncompressed_dir = Path::new(UNCOMPRESSED_DIR);
    let uncompressed_path = uncompressed_dir.join(format!("{}", archive_id));

    if !uncompressed_path.exists() {
        let archive = load_archive(archive_id).await?;
        uncompress_archive(&archive).await?;
    }

    Ok(uncompressed_path)
}