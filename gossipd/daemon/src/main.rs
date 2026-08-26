mod blob;
mod keys;
mod net;
mod peer;
mod rpc;
mod state;
mod store;
mod sync;
mod tor;

use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use gossipd_core::frame::{encode_frame, FrameDecoder};
use serde_json::{json, Value};

#[derive(Clone)]
pub struct Notifier(Arc<Mutex<std::io::Stdout>>);

impl Notifier {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(std::io::stdout())))
    }

    pub fn send(&self, payload: &Value) {
        let bytes = encode_frame(payload.to_string().as_bytes());
        let mut out = self.0.lock().unwrap();
        out.write_all(&bytes).and_then(|_| out.flush()).ok();
    }

    pub fn notify(&self, method: &str, params: Value) {
        self.send(&json!({"jsonrpc": "2.0", "method": method, "params": params}));
    }

    pub fn respond(&self, id: &Value, result: Value) {
        self.send(&json!({"jsonrpc": "2.0", "id": id, "result": result}));
    }

    pub fn respond_error(&self, id: &Value, code: i64, message: String) {
        self.send(&json!({"jsonrpc": "2.0", "id": id,
                          "error": {"code": code, "message": message}}));
    }
}

fn read_requests(tx: tokio::sync::mpsc::Sender<Value>) {
    let mut stdin = std::io::stdin().lock();
    let mut decoder = FrameDecoder::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = match stdin.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        decoder.feed(&chunk[..n]);
        loop {
            match decoder.next_frame() {
                Ok(Some(body)) => match serde_json::from_slice(&body) {
                    Ok(request) => {
                        if tx.blocking_send(request).is_err() {
                            return;
                        }
                    }
                    Err(err) => tracing::warn!("dropping unparseable frame: {err}"),
                },
                Ok(None) => break,
                Err(err) => {
                    tracing::error!("framing error: {err}, closing");
                    return;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "gossipd=info,warn".into()),
        )
        .init();

    let notifier = Notifier::new();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Value>(64);
    std::thread::spawn(move || read_requests(tx));

    let mut daemon = rpc::Daemon::new(notifier.clone());
    tracing::info!("gossipd up");
    while let Some(request) = rx.recv().await {
        daemon.handle(request).await;
    }
    tracing::info!("stdin closed, exiting");
}
