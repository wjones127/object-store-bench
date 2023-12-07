# Create a 1GB file
cargo run --release file://$(pwd)/test.bin \
    upload-data --size $((1024 * 1024 * 1024))

OUTPUT_FILE=results.ndjson
rm $OUTPUT_FILE

# Test parallel download to see what parallelism works best
for i in {1,5,10,20}; do
    echo "Running test $i"
    cargo run --release file://$(pwd)/test.bin \
        download --parallel-downloads $i >> $OUTPUT_FILE
done

# Test columnar page reads
for page_size in {4096,65536,$((10 * 1024 * 1024))}; do
    echo "Running test $page_size"
    cargo run --release file://$(pwd)/test.bin \
        columnar --page-sizes=$page_size,$page_size,$page_size >> $OUTPUT_FILE
done


# Test columnar page reads with large blobs
cargo run --release file://$(pwd)/test.bin \
        columnar --page-sizes=4096,65536,$((100 * 1024 * 1024)) >> $OUTPUT_FILE