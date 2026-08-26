use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use gossipd_core::frame::{encode_frame, FrameDecoder};
use serde_json::{json, Value};

struct Gossipd {
    child: Child,
    stdin: std::process::ChildStdin,
    rx: mpsc::Receiver<Value>,
    next_id: u64,
}

impl Gossipd {
    fn spawn(bin: &str, data_dir: &std::path::Path, port: u16) -> Self {
        let mut child = Command::new(bin)
            .env("GOSSIPD_NO_DISCOVERY", "1")
            .env("GOSSIPD_BIND", format!("127.0.0.1:{port}"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("cannot spawn {bin}: {e} (run `cargo build` first)"));
        let stdin = child.stdin.take().unwrap();
        let mut stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut decoder = FrameDecoder::new();
            let mut buf = [0u8; 8192];
            loop {
                let n = match stdout.read(&mut buf) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                decoder.feed(&buf[..n]);
                while let Ok(Some(body)) = decoder.next_frame() {
                    if serde_json::from_slice::<Value>(&body).is_ok_and(|v| tx.send(v).is_err()) {
                        return;
                    }
                }
            }
        });
        let _ = std::fs::create_dir_all(data_dir);
        Gossipd {
            child,
            stdin,
            rx,
            next_id: 0,
        }
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let body = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        self.stdin
            .write_all(&encode_frame(body.to_string().as_bytes()))
            .expect("write to gossipd");
        loop {
            let msg = self
                .rx
                .recv_timeout(Duration::from_secs(30))
                .expect("response");
            if msg["id"] == json!(id) {
                assert!(msg["error"].is_null(), "{method} failed: {}", msg["error"]);
                return msg["result"].clone();
            }
        }
    }

    fn wait_notification(&self, method: &str, pred: impl Fn(&Value) -> bool) -> Value {
        loop {
            let msg = self
                .rx
                .recv_timeout(Duration::from_secs(30))
                .expect("notification");
            if msg["method"] == json!(method) && pred(&msg["params"]) {
                return msg["params"].clone();
            }
        }
    }

    fn shutdown(mut self) {
        self.request("shutdown", json!({}));
        let _ = self.child.wait();
    }
}

fn main() {
    let bin = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "target/debug/gossipd".into());
    let scratch = std::env::temp_dir().join(format!("gossip-pair-demo-{}", std::process::id()));

    let mut alice = Gossipd::spawn(&bin, &scratch.join("alice"), 47411);
    let mut bob = Gossipd::spawn(&bin, &scratch.join("bob"), 47412);

    let backoff =
        json!({"initial-seconds": 1.0, "multiplier": 2.0, "max-seconds": 30.0, "jitter": 0.2});
    for (d, dir, name) in [(&mut alice, "alice", "Alice"), (&mut bob, "bob", "Bob")] {
        let identity = d.request(
            "init",
            json!({"data-dir": scratch.join(dir), "display-name": name, "backoff": backoff}),
        );
        println!("{name} is {}", identity["node-id"].as_str().unwrap());
    }

    let alice_ticket = alice.request("contact/makeTicket", json!({}))["ticket"].clone();
    let bob_ticket = bob.request("contact/makeTicket", json!({}))["ticket"].clone();
    let bob_contact = alice.request("contact/addTicket", json!({"ticket": bob_ticket}));
    let alice_contact = bob.request("contact/addTicket", json!({"ticket": alice_ticket}));
    let bob_id = bob_contact["id"].as_str().unwrap().to_string();
    let alice_id = alice_contact["id"].as_str().unwrap().to_string();

    let sent = alice.request(
        "msg/send",
        json!({"to": bob_id, "kind": "chat", "body": "hello from Rust"}),
    );
    println!("alice -> bob: {}", sent["status"].as_str().unwrap());
    let got = bob.wait_notification("msg/received", |p| p["kind"] == json!("chat"));
    println!("bob got {:?} from {}", got["body"], got["from-name"]);
    alice.wait_notification("msg/delivered", |_| true);

    bob.request(
        "msg/send",
        json!({"to": alice_id, "kind": "chat", "body": "hello back"}),
    );
    let got = alice.wait_notification("msg/received", |p| p["kind"] == json!("chat"));
    println!("alice got {:?} from {}", got["body"], got["from-name"]);

    alice.request(
        "msg/send",
        json!({"to": bob_id, "kind": "demo/counter", "body": "41"}),
    );
    let got = bob.wait_notification("msg/received", |p| p["kind"] == json!("demo/counter"));
    let n: i64 = got["body"].as_str().unwrap().parse().unwrap();
    println!("bob's demo/counter handler computed {}", n + 1);

    let history = alice.request("msg/history", json!({"peer-id": bob_id, "limit": 10}));
    println!(
        "alice's history with bob: {} messages",
        history.as_array().unwrap().len()
    );

    alice.shutdown();
    bob.shutdown();
    let _ = std::fs::remove_dir_all(&scratch);
    println!("PAIR DEMO OK");
}
