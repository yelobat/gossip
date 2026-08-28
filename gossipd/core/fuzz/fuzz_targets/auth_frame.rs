#![no_main]

use gossipd_core::frame::{auth_frame_matches, FrameDecoder};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = auth_frame_matches(data, "the-token");
    let mut d = FrameDecoder::new();
    d.feed(&gossipd_core::frame::encode_frame(data));
    if let Ok(Some(body)) = d.next_frame() {
        let _ = auth_frame_matches(&body, "the-token");
    }
});
