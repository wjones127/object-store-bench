//! A simulated columnar format. Various pages are stored in a single file.
//!
//! We simulate this by considering an existing blob and a set of fixed-size pages
//! and splitting up the file into those pages so we can read.
//!
//! For example, we might get a parameter `--page-sizes=1024,4096,16384` and
//! so then we split up the file into pages of those sizes, repeating as necessary.

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStore};

use crate::inspect_location;

pub async fn columnar_read_test(
    object_store: Arc<dyn ObjectStore>,
    location: Path,
    parallel_downloads: usize,
    page_sizes: Vec<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let objects = inspect_location(object_store.as_ref(), &location).await?;
    let object_size = objects[0].size;
    assert!(
        objects.iter().all(|o| o.size == object_size),
        "expected all objects to have the same size"
    );

    // let page_sizes: Arc<[usize]> = page_sizes.into();

    let num_columns = page_sizes.len();
    let group_size = page_sizes.iter().sum::<usize>();
    let num_groups = object_size / group_size;
    let mut page_offsets = vec![Vec::with_capacity(num_groups); num_columns];

    let total_size = group_size * num_groups;
    assert!(total_size <= object_size, "object is too small");

    let mut offset = 0;
    for _group_i in 0..num_groups {
        for column_i in 0..num_columns {
            let page_size = page_sizes[column_i];
            page_offsets[column_i].push(offset);
            offset += page_size;
        }
    }

    let objects_ref = objects.as_slice();
    let ranges_iter = (0..num_groups).flat_map(move |group_i| {
        objects_ref
            .iter()
            .map(move |meta| (meta.location.clone(), group_i))
            .collect::<Vec<_>>()
    });

    let start = std::time::Instant::now();
    let page_sizes_ref = page_sizes.as_slice();
    let page_offsets_ref = page_offsets.as_slice();
    let _counts = futures::stream::iter(ranges_iter)
        .map(|(location, group_i)| {
            let object_store = object_store.clone();
            async move {
                let reads = page_offsets_ref
                    .iter()
                    .enumerate()
                    .map(|(column_i, offsets)| {
                        let page_size = page_sizes_ref[column_i];
                        let offset = offsets[group_i];
                        // We already checked the object size, so this should be safe
                        let range = offset..(offset + page_size);
                        let location = location.clone();
                        let object_store = object_store.clone();
                        tokio::task::spawn(async move {
                            object_store
                                .get_range(&location, range)
                                .await
                                .map(|res| res.len())
                        })
                    })
                    .collect::<Vec<_>>();
                let counts = futures::future::join_all(reads).await;
                let mut total = 0;
                for count in counts {
                    total += match count {
                        Ok(Ok(count)) => count,
                        Ok(Err(e)) => return Err(e),
                        Err(e) => return Err(object_store::Error::JoinError { source: e }),
                    };
                }
                Ok(total)
            }
        })
        .buffered(parallel_downloads)
        .try_collect::<Vec<_>>()
        .await?;
    let end = std::time::Instant::now();
    let elapsed_us = (end - start).as_micros();

    let total_size = objects.len() * group_size * num_groups;
    let mbps = total_size as f64 / 1024.0 / 1024.0 / (elapsed_us as f64 / 1_000_000.0);

    println!("{{\"num_objects\": {}, \"num_groups\": {}, \"page_sizes\": {:?}, \"parallel_downloads\": {}, \"elapsed_us\": {}, \"mbps\": {}}}",
        objects.len(), num_groups, page_sizes, parallel_downloads, elapsed_us, mbps);

    Ok(())
}
