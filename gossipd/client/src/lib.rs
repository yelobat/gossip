use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gossipd_core::frame::{encode_frame, FrameDecoder};
use serde_json::{json, Value};

pub use serde_json;

pub struct Notification {
    pub method: String,
    pub params: Value,
}

type Pending = Arc<Mutex<HashMap<u64, Sender<Result<Value, String>>>>>;

pub struct Client {
    child: Option<Child>,
    writer: Mutex<Box<dyn Write + Send>>,
    next_id: AtomicU64,
    pending: Pending,
}

impl Client {
    /// Launch a private daemon and talk to it over its stdio. The daemon dies
    /// when this client is dropped.
    pub fn spawn(command: &[String]) -> std::io::Result<(Self, Receiver<Notification>)> {
        let mut child = Command::new(&command[0])
            .args(&command[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let writer = Box::new(child.stdin.take().expect("piped stdin"));
        let reader = Box::new(child.stdout.take().expect("piped stdout"));
        Ok(Self::from_io(reader, writer, Some(child)))
    }

    /// Attach to a daemon already listening on its control port. `control_file`
    /// holds `port\ntoken` (written by the daemon). Dropping this client leaves
    /// the daemon running for other clients.
    pub fn connect(control_file: &str) -> std::io::Result<(Self, Receiver<Notification>)> {
        let (port, token) = read_control(control_file)?;
        let mut stream = TcpStream::connect(("127.0.0.1", port))?;
        stream.set_nodelay(true).ok();
        let reader = Box::new(stream.try_clone()?);
        let auth = encode_frame(json!({"auth": token}).to_string().as_bytes());
        stream.write_all(&auth)?;
        stream.flush()?;
        Ok(Self::from_io(reader, Box::new(stream), None))
    }

    /// Attach to the shared daemon for this profile, starting it as a background
    /// service (listening on a loopback control port) if nothing is up yet.
    pub fn connect_or_spawn(
        control_file: &str,
        command: &[String],
    ) -> std::io::Result<(Self, Receiver<Notification>)> {
        if let Ok(c) = Self::connect(control_file) {
            return Ok(c);
        }
        Command::new(&command[0])
            .args(&command[1..])
            .env("GOSSIPD_CONTROL", control_file)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?;
        for _ in 0..100 {
            if let Ok(c) = Self::connect(control_file) {
                return Ok(c);
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Self::connect(control_file)
    }

    fn from_io(
        reader: Box<dyn Read + Send>,
        writer: Box<dyn Write + Send>,
        child: Option<Child>,
    ) -> (Self, Receiver<Notification>) {
        let pending: Pending = Default::default();
        let (ntx, nrx) = channel();
        let reader_pending = pending.clone();
        std::thread::spawn(move || reader_loop(reader, reader_pending, ntx));
        (
            Self {
                child,
                writer: Mutex::new(writer),
                next_id: AtomicU64::new(0),
                pending,
            },
            nrx,
        )
    }

    pub fn request(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = channel();
        self.pending.lock().unwrap().insert(id, tx);
        let frame = encode_frame(
            json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
                .to_string()
                .as_bytes(),
        );
        {
            let mut writer = self.writer.lock().unwrap();
            writer
                .write_all(&frame)
                .and_then(|_| writer.flush())
                .map_err(|e| e.to_string())?;
        }
        rx.recv()
            .map_err(|_| "daemon closed connection".to_string())?
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            child.kill().ok();
            child.wait().ok();
        }
    }
}

/// Unpack a profile archive into `data_dir` (identity keys + database), before
/// any daemon opens it. Returns the profile's node id.
pub fn import_profile(archive_path: &str, data_dir: &str) -> Result<String, String> {
    let bytes = std::fs::read(archive_path)
        .map_err(|e| format!("cannot read {archive_path}: {e}"))?;
    let archive = gossipd_core::profile::ProfileArchive::decode(&bytes)?;
    archive
        .unpack_into_dir(std::path::Path::new(data_dir))
        .map_err(|e| format!("cannot write profile: {e}"))?;
    let id: [u8; 32] = archive.identity[..]
        .try_into()
        .map_err(|_| "archive has an invalid identity key".to_string())?;
    Ok(gossipd_core::identity::node_id(
        &gossipd_core::identity::MasterKey::from_bytes(&id).public(),
    ))
}

/// Parse the daemon's control file: first line is the port, second the token.
fn read_control(path: &str) -> std::io::Result<(u16, String)> {
    let text = std::fs::read_to_string(path)?;
    let mut lines = text.lines();
    let port = lines
        .next()
        .and_then(|l| l.trim().parse().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad control file"))?;
    let token = lines.next().unwrap_or("").trim().to_string();
    Ok((port, token))
}

enum Dispatch {
    Response(u64, Result<Value, String>),
    Notify(Notification),
    Ignore,
}

fn dispatch(msg: Value) -> Dispatch {
    if let Some(id) = msg["id"].as_u64() {
        let result = if msg["error"].is_null() {
            Ok(msg["result"].clone())
        } else {
            Err(msg["error"]["message"]
                .as_str()
                .unwrap_or("daemon error")
                .to_string())
        };
        Dispatch::Response(id, result)
    } else if let Some(method) = msg["method"].as_str() {
        Dispatch::Notify(Notification {
            method: method.to_string(),
            params: msg["params"].clone(),
        })
    } else {
        Dispatch::Ignore
    }
}

fn reader_loop(mut reader: Box<dyn Read + Send>, pending: Pending, ntx: Sender<Notification>) {
    let mut decoder = FrameDecoder::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = match reader.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        decoder.feed(&buf[..n]);
        while let Ok(Some(body)) = decoder.next_frame() {
            let Ok(msg) = serde_json::from_slice::<Value>(&body) else {
                continue;
            };
            match dispatch(msg) {
                Dispatch::Response(id, res) => {
                    if let Some(tx) = pending.lock().unwrap().remove(&id) {
                        tx.send(res).ok();
                    }
                }
                Dispatch::Notify(n) => {
                    ntx.send(n).ok();
                }
                Dispatch::Ignore => {}
            }
        }
    }
    for (_, tx) in pending.lock().unwrap().drain() {
        tx.send(Err("daemon exited".into())).ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_response_result_error_and_notification() {
        match dispatch(json!({"id": 7, "result": {"ok": true}})) {
            Dispatch::Response(7, Ok(v)) => assert_eq!(v["ok"], json!(true)),
            _ => panic!("expected ok response for id 7"),
        }
        match dispatch(json!({"id": 8, "error": {"code": -1, "message": "boom"}})) {
            Dispatch::Response(8, Err(m)) => assert_eq!(m, "boom"),
            _ => panic!("expected error response for id 8"),
        }
        match dispatch(json!({"method": "msg/received", "params": {"body": "hi"}})) {
            Dispatch::Notify(n) => {
                assert_eq!(n.method, "msg/received");
                assert_eq!(n.params["body"], json!("hi"));
            }
            _ => panic!("expected notification"),
        }
        assert!(matches!(dispatch(json!({"junk": 1})), Dispatch::Ignore));
    }
}
