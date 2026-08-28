#[derive(Debug, PartialEq)]
pub enum FrameError {
    MissingContentLength,
    BadContentLength,
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::MissingContentLength => write!(f, "missing Content-Length header"),
            FrameError::BadContentLength => write!(f, "unparseable Content-Length header"),
        }
    }
}

impl std::error::Error for FrameError {}

#[derive(Default)]
pub struct FrameDecoder {
    buf: Vec<u8>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn feed(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>, FrameError> {
        let Some(header_end) = find(&self.buf, b"\r\n\r\n") else {
            return Ok(None);
        };
        let length = content_length(&self.buf[..header_end])?;
        let body_start = header_end + 4;
        let Some(frame_end) = body_start.checked_add(length) else {
            return Ok(None);
        };
        if self.buf.len() < frame_end {
            return Ok(None);
        }
        let body = self.buf[body_start..frame_end].to_vec();
        self.buf.drain(..frame_end);
        Ok(Some(body))
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn content_length(headers: &[u8]) -> Result<usize, FrameError> {
    for line in headers.split(|&b| b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            continue;
        };
        if line[..colon].eq_ignore_ascii_case(b"content-length") {
            return std::str::from_utf8(&line[colon + 1..])
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .ok_or(FrameError::BadContentLength);
        }
    }
    Err(FrameError::MissingContentLength)
}

pub fn encode_frame(body: &[u8]) -> Vec<u8> {
    let mut out = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
    out.extend_from_slice(body);
    out
}

pub fn auth_frame_matches(body: &[u8], token: &str) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("auth").and_then(serde_json::Value::as_str).map(|a| a == token))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(body: &str) -> Vec<u8> {
        encode_frame(body.as_bytes())
    }

    #[test]
    fn whole_frame() {
        let mut d = FrameDecoder::new();
        d.feed(&frame(r#"{"x":1}"#));
        assert_eq!(d.next_frame().unwrap().unwrap(), br#"{"x":1}"#);
        assert_eq!(d.next_frame().unwrap(), None);
    }

    #[test]
    fn partial_reads_byte_by_byte() {
        let mut d = FrameDecoder::new();
        let bytes = frame(r#"{"hello":"world"}"#);
        for b in &bytes[..bytes.len() - 1] {
            d.feed(&[*b]);
            assert_eq!(d.next_frame().unwrap(), None);
        }
        d.feed(&bytes[bytes.len() - 1..]);
        assert_eq!(d.next_frame().unwrap().unwrap(), br#"{"hello":"world"}"#);
    }

    #[test]
    fn multiple_frames_per_read() {
        let mut d = FrameDecoder::new();
        let mut bytes = frame(r#"{"a":1}"#);
        bytes.extend(frame(r#"{"b":2}"#));
        bytes.extend(frame(r#"{"c":3}"#));
        d.feed(&bytes);
        assert_eq!(d.next_frame().unwrap().unwrap(), br#"{"a":1}"#);
        assert_eq!(d.next_frame().unwrap().unwrap(), br#"{"b":2}"#);
        assert_eq!(d.next_frame().unwrap().unwrap(), br#"{"c":3}"#);
        assert_eq!(d.next_frame().unwrap(), None);
    }

    #[test]
    fn utf8_split_across_feeds() {
        let body = r#"{"msg":"héllo"}"#;
        let bytes = frame(body);
        let mut d = FrameDecoder::new();

        let acute = body
            .as_bytes()
            .windows(2)
            .position(|w| w == "é".as_bytes())
            .unwrap();
        let split = bytes.len() - body.len() + acute + 1;
        d.feed(&bytes[..split]);
        assert_eq!(d.next_frame().unwrap(), None);
        d.feed(&bytes[split..]);
        assert_eq!(d.next_frame().unwrap().unwrap(), body.as_bytes());
    }

    #[test]
    fn extra_headers_and_case_insensitivity() {
        let body = br#"{"ok":true}"#;
        let raw = format!(
            "Content-Type: application/json\r\ncontent-LENGTH: {}\r\n\r\n",
            body.len()
        );
        let mut d = FrameDecoder::new();
        d.feed(raw.as_bytes());
        d.feed(body);
        assert_eq!(d.next_frame().unwrap().unwrap(), body);
    }

    #[test]
    fn missing_content_length_is_an_error() {
        let mut d = FrameDecoder::new();
        d.feed(b"Content-Type: application/json\r\n\r\n{}");
        assert_eq!(d.next_frame(), Err(FrameError::MissingContentLength));
    }

    #[test]
    fn bad_content_length_is_an_error() {
        let mut d = FrameDecoder::new();
        d.feed(b"Content-Length: banana\r\n\r\n{}");
        assert_eq!(d.next_frame(), Err(FrameError::BadContentLength));
    }

    #[test]
    fn huge_content_length_never_panics() {
        let mut d = FrameDecoder::new();
        d.feed(format!("Content-Length: {}\r\n\r\nabc", usize::MAX).as_bytes());
        assert_eq!(d.next_frame().unwrap(), None);
    }

    #[test]
    fn auth_frame_matches_only_exact_token() {
        assert!(auth_frame_matches(br#"{"auth":"secret"}"#, "secret"));
        assert!(!auth_frame_matches(br#"{"auth":"secret"}"#, "other"));
        assert!(!auth_frame_matches(br#"{"method":"init"}"#, "secret"));
        assert!(!auth_frame_matches(b"not json", "secret"));
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn auth_never_panics(body: Vec<u8>, token: String) {
            let _ = auth_frame_matches(&body, &token);
        }

        #[test]
        fn roundtrips_across_any_split(body: Vec<u8>, split in 0.0f64..1.0) {
            let bytes = encode_frame(&body);
            let at = (bytes.len() as f64 * split) as usize;
            let mut d = FrameDecoder::new();
            d.feed(&bytes[..at]);
            prop_assert_eq!(d.next_frame().unwrap(), None);
            d.feed(&bytes[at..]);
            prop_assert_eq!(d.next_frame().unwrap().unwrap(), body);
        }

        #[test]
        fn arbitrary_input_never_panics(chunks: Vec<Vec<u8>>) {
            let mut d = FrameDecoder::new();
            for c in &chunks {
                d.feed(c);
                while let Ok(Some(_)) = d.next_frame() {}
            }
        }
    }
}
