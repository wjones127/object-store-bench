use std::sync::Arc;

use clap::{Parser, Subcommand};
use futures::TryStreamExt;
use object_store::{parse_url, ObjectMeta};
use object_store::{path::Path, ObjectStore};
use rand::{thread_rng, Rng, RngCore};
use tokio::io::AsyncWriteExt;

mod columnar;
mod download;

/// Upload a test object of the given size
///
/// This will upload in batches of 10MB, allowing for objects larger than memory.
///
/// The data generated will be random bytes.
async fn upload_test_data(
    object_store: Arc<dyn ObjectStore>,
    location: &Path,
    size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_id, mut writer) = object_store.put_multipart(location).await?;

    // Write 10 MB at a time
    let mut written = 0;
    let mut rng = rand::thread_rng();
    let mut buffer = vec![0; 10 * 1024 * 1024];
    while written < size {
        let to_write = std::cmp::min(size - written, 10 * 1024 * 1024);
        rng.fill_bytes(&mut buffer);
        writer.write_all(&buffer[0..to_write]).await?;
        written += to_write;
    }
    writer.flush().await?;
    writer.shutdown().await?;

    Ok(())
}

async fn upload_multiple(
    object_store: Arc<dyn ObjectStore>,
    location: &Path,
    num_objects: usize,
    size: usize,
    random_prefixes: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let size_per_object = size / num_objects;
    if size % num_objects != 0 {
        panic!("size must be divisible by num_objects");
    }

    for i in 0..num_objects {
        let mut location = location.parts().collect::<Vec<_>>();
        if random_prefixes {
            let prefix = thread_rng()
                .sample_iter(rand::distributions::Alphanumeric)
                .take(8)
                .collect::<Vec<u8>>();
            let prefix = String::from_utf8(prefix).unwrap();

            location.push(prefix.into());
        }
        location.push(format!("object_{}.bin", i).into());
        let location = Path::from_iter(location);
        upload_test_data(object_store.clone(), &location, size_per_object).await?;
    }

    Ok(())
}

/// Inspects the given location and returns a list of all objects and their sizes.
///
/// If the location is an object itself, it will just return that object.
///
/// If the location is a common prefix, it will return all objects with that prefix.
async fn inspect_location(
    object_store: &dyn ObjectStore,
    location: &Path,
) -> Result<Vec<ObjectMeta>, Box<dyn std::error::Error>> {
    match object_store.head(location).await {
        Ok(metadata) => Ok(vec![metadata]),
        Err(object_store::Error::NotFound { .. }) => Ok(object_store
            .list(Some(location))
            .await?
            .try_collect()
            .await?),
        Err(err) => Err(err.into()),
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Optional name to operate on
    object_uri: String,

    // TODO: tracing flag
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Uploads test data to the given object store uri
    ///
    /// This will overwrite any existing data at the given location.
    UploadData {
        /// Number of bytes to upload to the object. Defaults to 100MB.
        #[arg(short, long, default_value = "104857600")]
        size: usize,
    },

    /// Uploads multiple test objects
    UploadMultiple {
        /// Number of objects to upload
        #[arg(short, long, default_value = "10")]
        num_objects: usize,
        /// Total number of bytes to upload across all objects
        /// Default: 10GB
        #[arg(short, long, default_value = "10737418240")]
        size: usize,
        /// Whether to use random prefixes
        #[arg(short, long, default_value = "false")]
        random_prefixes: bool,
    },

    /// Times how long it takes to download an object.
    ///
    /// This downloads the object in parallel, using the given number of parallel downloads.
    /// The file is split evenly into parallel_downloads blocks and the blocks are
    /// downloaded concurrently.
    ///
    ///
    Download {
        #[arg(short, long, default_value = "10")]
        parallel_downloads: usize,
        #[arg(short, long, default_value = None)]
        block_size: Option<usize>,
    },

    Columnar {
        /// Number of batches to read in parallel
        #[arg(short, long, default_value = "10")]
        parallel_downloads: usize,
        /// Comma-separated list of page sizes to use
        #[arg(short, long, default_value = "65536,65536,65536")]
        page_sizes: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let args: Args = Args::parse();

    let (object_store, location) = parse_url(&url::Url::parse(&args.object_uri).unwrap()).unwrap();
    let object_store: Arc<_> = object_store.into();

    match args.command {
        Some(Commands::UploadData { size }) => {
            upload_test_data(object_store, &location, size)
                .await
                .unwrap();
        }
        Some(Commands::UploadMultiple {
            num_objects,
            size,
            random_prefixes,
        }) => {
            upload_multiple(object_store, &location, num_objects, size, random_prefixes)
                .await
                .unwrap();
        }
        Some(Commands::Download {
            parallel_downloads,
            block_size,
        }) => {
            download::parallel_download_bench(
                object_store,
                location,
                parallel_downloads,
                block_size,
            )
            .await
            .unwrap();
        }
        Some(Commands::Columnar {
            parallel_downloads,
            page_sizes,
        }) => {
            let page_sizes = page_sizes
                .unwrap()
                .split(',')
                .map(|s| s.parse().unwrap())
                .collect();
            columnar::columnar_read_test(object_store, location, parallel_downloads, page_sizes)
                .await
                .unwrap();
        }
        None => {
            println!("No command specified");
        }
    }
}
