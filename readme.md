

## Running locally

To create test data:

```bash
cargo run --release file://$(pwd)/test.bin upload-data
```

To run the download test:

```bash
cargo run --release file://$(pwd)/test.bin download
```

To run the columnar test:

```bash
cargo run --release file://$(pwd)/test.bin columnar
```


```bash
cargo run --release file://$(pwd)/test_multiple upload-multiple --size $((100 * 1024 * 1024))
```

```bash
cargo run --release file://$(pwd)/test_multiple_random upload-multiple --size $((100 * 1024)) --random-prefixes
```

```bash
cargo run --release file://$(pwd)/test_multiple download
cargo run --release file://$(pwd)/test_multiple columnar
```