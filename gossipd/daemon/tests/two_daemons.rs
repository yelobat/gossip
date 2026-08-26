use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use gossipd_core::frame::{encode_frame, FrameDecoder};
use serde_json::{json, Value};

const BACKOFF_INITIAL: f64 = 1.0;
const BACKOFF_MULTIPLIER: f64 = 2.0;
const BACKOFF_MAX: f64 = 8.0;
const BACKOFF_JITTER: f64 = 0.2;

struct Daemon {
    child: Child,
    stdin: std::process::ChildStdin,
    rx: mpsc::Receiver<Value>,
    stash: Vec<Value>,
    next_id: u64,
}

impl Daemon {
    fn spawn(data_dir: &std::path::Path, port: u16) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_gossipd"))
            .env("GOSSIPD_NO_DISCOVERY", "1")
            .env("GOSSIPD_BIND", format!("127.0.0.1:{port}"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn gossipd");
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
                    if let Ok(v) = serde_json::from_slice(&body) {
                        if tx.send(v).is_err() {
                            return;
                        }
                    }
                }
            }
        });
        let mut daemon = Self {
            child,
            stdin,
            rx,
            stash: Vec::new(),
            next_id: 0,
        };

        std::fs::create_dir_all(data_dir).unwrap();
        daemon.init(data_dir);
        daemon
    }

    fn init(&mut self, data_dir: &std::path::Path) {
        let result = self.request(
            "init",
            json!({
                "data-dir": data_dir,
                "backoff": {
                    "initial-seconds": BACKOFF_INITIAL,
                    "multiplier": BACKOFF_MULTIPLIER,
                    "max-seconds": BACKOFF_MAX,
                    "jitter": BACKOFF_JITTER,
                },
            }),
        );
        assert!(result["node-id"].as_str().unwrap().starts_with("gsp1-"));

        let again = self.request("init", json!({"data-dir": data_dir}));
        assert_eq!(again["node-id"], result["node-id"]);
    }

    fn send_frame(&mut self, payload: &Value) {
        let bytes = encode_frame(payload.to_string().as_bytes());
        self.stdin.write_all(&bytes).unwrap();
        self.stdin.flush().unwrap();
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.send_frame(&json!({
            "jsonrpc": "2.0", "id": id, "method": method, "params": params,
        }));
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let msg = self
                .rx
                .recv_timeout(remaining)
                .unwrap_or_else(|_| panic!("no response to {method}"));
            if msg["id"] == json!(id) {
                assert!(msg["error"].is_null(), "{method} failed: {}", msg["error"]);
                return msg["result"].clone();
            }
            self.stash.push(msg);
        }
    }

    fn wait_notification(
        &mut self,
        timeout: Duration,
        pred: impl Fn(&str, &Value) -> bool,
    ) -> Value {
        let matches = |m: &Value| {
            m.get("method")
                .and_then(Value::as_str)
                .is_some_and(|method| pred(method, &m["params"]))
        };
        if let Some(i) = self.stash.iter().position(matches) {
            return self.stash.remove(i)["params"].clone();
        }
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let msg = self
                .rx
                .recv_timeout(remaining)
                .expect("timed out waiting for notification");
            if matches(&msg) {
                return msg["params"].clone();
            }
            if msg.get("method").is_some() {
                self.stash.push(msg);
            }
        }
    }

    fn kill(mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }

    fn shutdown(&mut self) {
        self.request("shutdown", json!({}));
        self.child.wait().ok();
    }
}

fn history_bodies(d: &mut Daemon, peer: &str) -> Vec<String> {
    d.request("msg/history", json!({"peer-id": peer, "limit": 100}))
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["body"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn two_daemons_deliver_queue_and_converge() {
    let base = std::env::temp_dir().join(format!("gossipd-it-{}", std::process::id()));
    let (dir_a, dir_b) = (base.join("a"), base.join("b"));
    let _ = std::fs::remove_dir_all(&base);

    let port_a = 20000 + (std::process::id() % 20000) as u16;
    let port_b = port_a + 1;

    let mut a = Daemon::spawn(&dir_a, port_a);
    let mut b = Daemon::spawn(&dir_b, port_b);
    let a_id = a.request("identity/get", json!({}))["node-id"]
        .as_str()
        .unwrap()
        .to_string();
    let b_id = b.request("identity/get", json!({}))["node-id"]
        .as_str()
        .unwrap()
        .to_string();

    let ticket_a = a.request("contact/makeTicket", json!({}))["ticket"]
        .as_str()
        .unwrap()
        .to_string();
    let ticket_b = b.request("contact/makeTicket", json!({}))["ticket"]
        .as_str()
        .unwrap()
        .to_string();
    let added = a.request(
        "contact/addTicket",
        json!({"ticket": ticket_b, "name": "bob"}),
    );
    assert_eq!(added["id"].as_str().unwrap(), b_id);
    b.request(
        "contact/addTicket",
        json!({"ticket": ticket_a, "name": "alice"}),
    );

    let sent = a.request(
        "msg/send",
        json!({"to": b_id, "kind": "chat", "body": "hello bob"}),
    );
    let m1 = sent["msg-id"].as_str().unwrap().to_string();
    let delivered = a.wait_notification(Duration::from_secs(15), |m, p| {
        m == "msg/delivered" && p["msg-id"] == json!(m1)
    });
    assert_eq!(delivered["to"].as_str().unwrap(), b_id);
    let received = b.wait_notification(Duration::from_secs(15), |m, p| {
        m == "msg/received" && p["body"] == json!("hello bob")
    });
    assert_eq!(received["from"].as_str().unwrap(), a_id);
    assert_eq!(received["from-name"].as_str().unwrap(), "alice");

    b.request("msg/send", json!({"to": a_id, "body": "hi alice"}));
    a.wait_notification(Duration::from_secs(15), |m, p| {
        m == "msg/received" && p["body"] == json!("hi alice")
    });

    b.kill();
    let sent = a.request("msg/send", json!({"to": b_id, "body": "are you there?"}));
    let queued_msg = sent["msg-id"].as_str().unwrap().to_string();
    let mut last_attempts = 0;
    for _ in 0..3 {
        let update = a.wait_notification(Duration::from_secs(45), |m, p| {
            m == "queue/update" && p["msg-id"] == json!(queued_msg)
        });
        let attempts = update["attempts"].as_u64().unwrap();
        assert_eq!(attempts, last_attempts + 1, "attempts must count up");
        last_attempts = attempts;

        let expected =
            (BACKOFF_INITIAL * BACKOFF_MULTIPLIER.powi(attempts as i32 - 1)).min(BACKOFF_MAX);
        let delay = update["delay-seconds"].as_f64().unwrap();
        let (lo, hi) = (
            expected * (1.0 - BACKOFF_JITTER) - 0.01,
            expected * (1.0 + BACKOFF_JITTER) + 0.01,
        );
        assert!(
            (lo..=hi).contains(&delay),
            "attempt {attempts}: delay {delay} outside jittered schedule {lo}..{hi}"
        );
        assert_eq!(update["to-name"].as_str().unwrap(), "bob");
    }

    let mut b = Daemon::spawn(&dir_b, port_b);
    let delivered = a.wait_notification(Duration::from_secs(45), |m, p| {
        m == "msg/delivered" && p["msg-id"] == json!(queued_msg)
    });
    assert_eq!(delivered["to"].as_str().unwrap(), b_id);
    b.wait_notification(Duration::from_secs(20), |m, p| {
        m == "msg/received" && p["body"] == json!("are you there?")
    });

    let expect = ["hello bob", "hi alice", "are you there?"];
    assert_eq!(history_bodies(&mut a, &b_id), expect);
    assert_eq!(history_bodies(&mut b, &a_id), expect);

    let status = a.request("status", json!({}));
    assert_eq!(status["queue"].as_array().unwrap().len(), 0);

    let payload = vec![7u8; 512 * 1024];
    let blob_path = base.join("photo.bin");
    std::fs::write(&blob_path, &payload).unwrap();
    let sent = a.request("blob/send", json!({"to": b_id, "path": blob_path}));
    let transfer_id = sent["transfer-id"].as_str().unwrap().to_string();
    a.wait_notification(Duration::from_secs(30), |m, p| {
        m == "transfer/progress"
            && p["transfer-id"] == json!(transfer_id)
            && p["percent"] == json!(100)
    });
    b.wait_notification(Duration::from_secs(30), |m, p| {
        m == "log" && p["message"].as_str().unwrap_or("").contains("photo.bin")
    });
    let received_file = dir_b.join("downloads").join("photo.bin");
    assert_eq!(std::fs::read(received_file).unwrap(), payload);

    a.shutdown();
    b.shutdown();
    let _ = std::fs::remove_dir_all(&base);
}
