use gossipd_core::identity::MasterKey;
use gossipd_core::log::LogEntry;

#[test]
fn json_roundtrip_preserves_sig() {
    let k = MasterKey::from_bytes(&[1; 32]);

    let e = LogEntry::sign(&k, [2; 32], 1, "chat", "hello bob", 1787273864.9403887);
    assert!(e.verify());
    let j = serde_json::to_string(&e).unwrap();
    let back: LogEntry = serde_json::from_str(&j).unwrap();
    assert_eq!(back, e);
    assert!(back.verify(), "json roundtrip broke the signature");
}
