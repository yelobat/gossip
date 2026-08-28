# Fuzz targets

```sh
cargo install cargo-fuzz
cd gossipd/core/fuzz

cargo +nightly fuzz run frame_decode      # frame decoder
cargo +nightly fuzz run auth_frame        # decode + auth handshake
```
