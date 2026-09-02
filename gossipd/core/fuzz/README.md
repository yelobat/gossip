# Fuzz targets

```sh
cargo install cargo-fuzz
cd gossipd/core/fuzz

cargo +nightly fuzz run frame_decode      # frame decoder
cargo +nightly fuzz run auth_frame        # decode + auth handshake
cargo +nightly fuzz run -O doc_engine     # shared-doc engine: edits, syncs, peer bytes
```
