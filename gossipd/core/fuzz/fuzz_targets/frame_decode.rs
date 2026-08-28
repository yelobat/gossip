#![no_main]

use gossipd_core::frame::FrameDecoder;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut d = FrameDecoder::new();
    for &b in data {
        d.feed(&[b]);
        let _ = d.next_frame();
    }
    while let Ok(Some(_)) = d.next_frame() {}
});
