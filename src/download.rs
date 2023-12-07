//! Parallel download implementation

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStore};

/// Benchmarks the approach of downloading an object in parallel
///
/// * `location`: where the test object should be made
/// * `parallel_downloads`: maximum number of requests to make in parallel
/// * `block_size`: size of each block to download
pub async fn parallel_download_bench(
    object_store: Arc<dyn ObjectStore>,
    location: Path,
    parallel_downloads: usize,
    block_size: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_size = object_store.head(&location).await.unwrap().size;
    let block_size = block_size.unwrap_or(object_size / parallel_downloads);
    let num_blocks = (object_size + block_size - 1) / block_size;

    // TODO: add tracing
    let start = std::time::Instant::now();
    let _counts = futures::stream::iter(0..num_blocks)
        .map(|block_index| {
            let range_start = block_index * block_size;
            let range_end = std::cmp::min(range_start + block_size, object_size);
            let range = range_start..range_end;

            let location = location.clone();
            let object_store = object_store.clone();
            tokio::task::spawn(async move {
                object_store
                    .get_range(&location, range)
                    .await
                    .map(|res| res.len())
            })
        })
        .buffered(parallel_downloads)
        .try_collect::<Vec<_>>()
        .await?;
    let end = std::time::Instant::now();

    let elapsed_us = (end - start).as_micros();
    let mbps = object_size as f64 / 1024.0 / 1024.0 / (elapsed_us as f64 / 1_000_000.0);

    println!("{{\"num_blocks\": {}, \"block_size\": {}, \"parallel_downloads\": {}, \"elapsed_us\": {}, \"mbps\": {}}}",
        num_blocks, block_size, parallel_downloads, elapsed_us, mbps);
    Ok(())
}
