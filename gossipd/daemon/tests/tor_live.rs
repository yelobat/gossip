use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use gossipd_core::frame::{encode_frame, FrameDecoder};
use serde_json::{json, Value};

struct Daemon {
    child: Child,
    stdin: std::process::ChildStdin,
    rx: mpsc::Receiver<Value>,
    stash: Vec<Value>,
    next_id: u64,
}

impl Daemon {
    fn spawn(data_dir: &std::path::Path, port: u16) -> Self {
        std::fs::create_dir_all(data_dir).unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_gossipd"))
            .env("GOSSIPD_NO_DISCOVERY", "1")
            .env("GOSSIPD_TOR_ONLY", "1")
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
            let mut dec = FrameDecoder::new();
            let mut buf = [0u8; 8192];
            loop {
                match stdout.read(&mut buf) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => dec.feed(&buf[..n]),
                }
                while let Ok(Some(body)) = dec.next_frame() {
                    if let Ok(v) = serde_json::from_slice(&body) {
                        if tx.send(v).is_err() {
                            return;
                        }
                    }
                }
            }
        });
        let mut d = Self {
            child,
            stdin,
            rx,
            stash: Vec::new(),
            next_id: 0,
        };
        let name = data_dir.file_name().unwrap().to_string_lossy().to_string();
        d.request(
            "init",
            json!({"data-dir": data_dir, "display-name": name,
                   "transport": {"tor": {"enabled": true}}}),
        );
        d
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let frame = encode_frame(
            json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
                .to_string()
                .as_bytes(),
        );
        self.stdin.write_all(&frame).unwrap();
        self.stdin.flush().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let msg = self
                .rx
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|_| panic!("no response to {method}"));
            if msg["id"] == json!(id) {
                assert!(msg["error"].is_null(), "{method}: {}", msg["error"]);
                return msg["result"].clone();
            }
            self.stash.push(msg);
        }
    }

    fn wait(&mut self, timeout: Duration, pred: impl Fn(&str, &Value) -> bool) -> Value {
        let m = |v: &Value| {
            v.get("method")
                .and_then(Value::as_str)
                .is_some_and(|meth| pred(meth, &v["params"]))
        };
        if let Some(i) = self.stash.iter().position(&m) {
            return self.stash.remove(i)["params"].clone();
        }
        let deadline = Instant::now() + timeout;
        loop {
            let msg = self
                .rx
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("timed out waiting for notification");
            if m(&msg) {
                return msg["params"].clone();
            }
            if msg.get("method").is_some() {
                self.stash.push(msg);
            }
        }
    }

    fn await_tor(&mut self) {
        self.wait(Duration::from_secs(180), |method, p| {
            method == "tor/status" && p["percent"] == json!(100)
        });

        let deadline = Instant::now() + Duration::from_secs(120);
        while Instant::now() < deadline {
            if self.request("status", json!({}))["tor"]
                .as_str()
                .is_some_and(|t| t.ends_with(".onion"))
            {
                return;
            }
            std::thread::sleep(Duration::from_secs(2));
        }
        panic!("onion address never appeared in status");
    }

    fn shutdown(&mut self) {
        self.request("shutdown", json!({}));
        self.child.wait().ok();
    }
}

#[test]
#[ignore = "needs real Tor network access, run with --ignored, takes minutes"]
fn message_delivers_over_tor() {
    let base = std::env::temp_dir().join(format!("gossipd-tor-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let (dir_a, dir_b) = (base.join("alice"), base.join("bob"));
    let port = 21000 + (std::process::id() % 10000) as u16;

    let mut a = Daemon::spawn(&dir_a, port);
    let mut b = Daemon::spawn(&dir_b, port + 1);

    eprintln!("waiting for both onion services (this is the slow part)...");
    a.await_tor();
    b.await_tor();
    eprintln!("both onions live, exchanging tickets");

    let a_ticket = a.request("contact/makeTicket", json!({}))["ticket"].clone();
    let b_ticket = b.request("contact/makeTicket", json!({}))["ticket"].clone();
    let b_contact = a.request("contact/addTicket", json!({"ticket": b_ticket}));
    b.request("contact/addTicket", json!({"ticket": a_ticket}));

    let bob_id = b_contact["id"].as_str().unwrap().to_string();

    a.request(
        "msg/send",
        json!({"to": bob_id, "kind": "chat", "body": "hello over tor"}),
    );

    let got = b.wait(Duration::from_secs(300), |method, p| {
        method == "msg/received" && p["body"] == json!("hello over tor")
    });
    eprintln!("bob received over tor from {}", got["from-name"]);

    let presence = b.wait(Duration::from_secs(30), |method, p| {
        method == "peer/presence" && p["path"] == json!("tor")
    });
    assert_eq!(presence["path"], json!("tor"));

    a.shutdown();
    b.shutdown();
    let _ = std::fs::remove_dir_all(&base);
    eprintln!("TOR LIVE OK");
}
