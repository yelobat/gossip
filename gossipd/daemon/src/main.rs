mod blob;
mod docs;
mod keys;
mod listen;
mod net;
mod peer;
mod rpc;
mod state;
mod store;
mod sync;
mod tor;

use std::sync::{Arc, Mutex};

use gossipd_core::frame::encode_frame;
use serde_json::{json, Value};
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct ClientHandle {
    id: u64,
    tx: mpsc::UnboundedSender<Arc<Vec<u8>>>,
}

impl ClientHandle {
    pub fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, payload: &Value) {
        let bytes = Arc::new(encode_frame(payload.to_string().as_bytes()));
        self.tx.send(bytes).ok();
    }

    pub fn respond(&self, id: &Value, result: Value) {
        self.send(&json!({"jsonrpc": "2.0", "id": id, "result": result}));
    }

    pub fn respond_error(&self, id: &Value, code: i64, message: String) {
        self.send(&json!({"jsonrpc": "2.0", "id": id,
                          "error": {"code": code, "message": message}}));
    }
}

#[derive(Clone, Default)]
pub struct Notifier {
    clients: Arc<Mutex<Vec<ClientHandle>>>,
}

impl Notifier {
    fn new() -> Self {
        Self::default()
    }

    fn register(&self, handle: ClientHandle) {
        self.clients.lock().unwrap().push(handle);
    }

    pub fn notify(&self, method: &str, params: Value) {
        let payload = json!({"jsonrpc": "2.0", "method": method, "params": params});
        let bytes = Arc::new(encode_frame(payload.to_string().as_bytes()));
        self.clients
            .lock()
            .unwrap()
            .retain(|c| c.tx.send(bytes.clone()).is_ok());
    }

    pub fn notify_client(&self, id: u64, method: &str, params: Value) {
        let payload = json!({"jsonrpc": "2.0", "method": method, "params": params});
        let bytes = Arc::new(encode_frame(payload.to_string().as_bytes()));
        for c in self.clients.lock().unwrap().iter() {
            if c.id == id {
                c.tx.send(bytes.clone()).ok();
            }
        }
    }
}

pub type Requests = mpsc::Sender<(Value, ClientHandle)>;

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
    let (req_tx, mut req_rx) = mpsc::channel::<(Value, ClientHandle)>(64);

    listen::serve_stdio(req_tx.clone(), &notifier);
    if let Ok(path) = std::env::var("GOSSIPD_CONTROL") {
        listen::serve_control(path.into(), req_tx.clone(), notifier.clone()).await;
    }
    drop(req_tx);

    let mut daemon = rpc::Daemon::new(notifier);
    tracing::info!("gossipd up");
    while let Some((request, origin)) = req_rx.recv().await {
        daemon.handle(request, &origin).await;
    }
    tracing::info!("all clients gone, exiting");
}
