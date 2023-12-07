//! Parallel download implementation

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStore};

use crate::inspect_location;

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
    let objects = inspect_location(object_store.as_ref(), &location).await?;
    let object_size = objects[0].size;
    assert!(
        objects.iter().all(|o| o.size == object_size),
        "expected all objects to have the same size"
    );

    let block_size = block_size.unwrap_or(object_size / parallel_downloads);
    let num_blocks = (object_size + block_size - 1) / block_size;

    // Make requests interleaving across objects.
    let objects_ref = &objects;
    let ranges_iter = (0..num_blocks).flat_map(move |block_i| {
        let start = block_i * block_size;
        let end = std::cmp::min((block_i + 1) * block_size, object_size);
        objects_ref
            .iter()
            .map(move |meta| (meta.location.clone(), start..end))
            .collect::<Vec<_>>()
    });

    // TODO: add tracing
    let start = std::time::Instant::now();
    let _counts = futures::stream::iter(ranges_iter)
        .map(|(location, range)| {
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
    let total_size = object_size * objects.len();
    let mbps = total_size as f64 / 1024.0 / 1024.0 / (elapsed_us as f64 / 1_000_000.0);

    println!("{{\"num_objects\": {}, \"num_blocks\": {}, \"block_size\": {}, \"parallel_downloads\": {}, \"elapsed_us\": {}, \"mbps\": {}}}",
    objects.len(), num_blocks, block_size, parallel_downloads, elapsed_us, mbps);
    Ok(())
}
