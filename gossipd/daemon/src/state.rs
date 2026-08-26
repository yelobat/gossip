use std::collections::HashMap;
use std::sync::Mutex;

use gossipd_core::backoff::BackoffCfg;
use tokio::sync::mpsc;

use crate::store::Store;
use crate::Notifier;

pub enum PeerCmd {
    Inbound(iroh::endpoint::Connection),

    Push,

    Nudge,
}

pub struct Shared {
    pub notifier: Notifier,
    pub store: Mutex<Store>,
    pub endpoint: iroh::Endpoint,
    pub blobs: iroh_blobs::api::Store,
    pub blobs_proto: iroh_blobs::BlobsProtocol,
    pub data_dir: std::path::PathBuf,
    pub backoff: BackoffCfg,
    pub my_master: [u8; 32],

    pub peers: Mutex<HashMap<String, mpsc::UnboundedSender<PeerCmd>>>,

    pub conns: Mutex<HashMap<String, iroh::endpoint::Connection>>,

    pub inbound_seen: Mutex<bool>,

    pub tor: Mutex<Option<std::sync::Arc<crate::tor::TorState>>>,
}

impl Shared {
    pub fn is_connected(&self, contact_id: &str) -> bool {
        self.conns.lock().unwrap().contains_key(contact_id)
    }

    pub fn live_connection(&self, contact_id: &str) -> Option<iroh::endpoint::Connection> {
        self.conns.lock().unwrap().get(contact_id).cloned()
    }
}

pub fn now_ts() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub fn msg_id(seq: u64) -> String {
    format!("m{seq:04}")
}

pub fn jitter_roll() -> f64 {
    let mut b = [0u8; 8];
    getrandom::fill(&mut b).ok();
    (u64::from_le_bytes(b) as f64 / u64::MAX as f64) * 2.0 - 1.0
}

pub fn random_nonce() -> [u8; 32] {
    let mut b = [0u8; 32];
    getrandom::fill(&mut b).ok();
    b
}
