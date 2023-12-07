

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