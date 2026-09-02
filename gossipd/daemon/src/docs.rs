use std::collections::HashMap;
use std::sync::Arc;

use gossipd_core::doc::{Op, SharedDoc};
use serde_json::{json, Value};
use tokio::io::BufReader;
use tokio::sync::mpsc;

use crate::state::{PeerCmd, Shared};
use crate::sync::{read_msg, write_msg, PeerInfo, WireMsg};

const COMPACT_EVERY: u64 = 500;
const RESYNC_EVERY: std::time::Duration = std::time::Duration::from_secs(30);

pub const KIND_INVITE: &str = "doc-invite";

pub struct Entry {
    pub doc: SharedDoc,
    pub name: String,
    pub inviter: Option<String>,
    links: HashMap<String, mpsc::UnboundedSender<WireMsg>>,
}

impl Entry {
    fn allows(&self, peer: &str) -> bool {
        self.doc.is_member(peer) || self.inviter.as_deref() == Some(peer)
    }
}

#[derive(Default)]
pub struct Docs {
    entries: HashMap<String, Entry>,
}

fn b64(bytes: &[u8]) -> String {
    data_encoding::BASE64.encode(bytes)
}

fn unb64(s: &str) -> Result<Vec<u8>, String> {
    data_encoding::BASE64
        .decode(s.as_bytes())
        .map_err(|e| format!("bad base64: {e}"))
}

fn err<T>(code: i64, msg: impl Into<String>) -> Result<T, (i64, String)> {
    Err((code, msg.into()))
}

pub fn load_all(shared: &Arc<Shared>) {
    let rows = shared.store.lock().unwrap().docs();
    let mut docs = shared.docs.lock().unwrap();
    for row in rows {
        if row.state != "active" {
            continue;
        }
        let (snapshot, updates) = shared.store.lock().unwrap().doc_state(&row.id);
        match SharedDoc::load(snapshot.as_deref(), updates.iter().map(Vec::as_slice)) {
            Ok(doc) => {
                docs.entries.insert(
                    row.id.clone(),
                    Entry {
                        doc,
                        name: row.name,
                        inviter: row.inviter,
                        links: HashMap::new(),
                    },
                );
            }
            Err(e) => tracing::error!(doc = %row.id, "cannot load document: {e}"),
        }
    }
    tracing::info!(n = docs.entries.len(), "documents loaded");
}

fn record(shared: &Arc<Shared>, id: &str, entry: &Entry, update: &[u8], from: Option<&str>) {
    let count = {
        let store = shared.store.lock().unwrap();
        store.doc_append(id, update);
        store.doc_update_count(id)
    };
    if count >= COMPACT_EVERY {
        shared
            .store
            .lock()
            .unwrap()
            .doc_snapshot(id, &entry.doc.full_state());
    }
    let msg = WireMsg::Up { b: b64(update) };
    for (peer, tx) in &entry.links {
        if Some(peer.as_str()) != from {
            tx.send(msg.clone()).ok();
        }
    }
}

fn notify_dirty(shared: &Arc<Shared>, id: &str, entry: &mut Entry) {
    for client in entry.doc.mark_dirty() {
        shared
            .notifier
            .notify_client(client, "doc/dirty", json!({"id": id}));
    }
}

fn my_id(shared: &Arc<Shared>) -> String {
    ed25519_dalek::VerifyingKey::from_bytes(&shared.my_master)
        .map(|k| gossipd_core::identity::node_id(&k))
        .unwrap_or_default()
}

fn new_id() -> String {
    let n = crate::state::random_nonce();
    data_encoding::HEXLOWER.encode(&n[..8])
}

pub fn rpc_create(shared: &Arc<Shared>, params: &Value, my_name: &str) -> Result<Value, (i64, String)> {
    let name = params["name"].as_str().unwrap_or("untitled").to_string();
    let id = new_id();
    let doc = SharedDoc::new();
    let update = doc.set_member(&my_id(shared), my_name);
    shared
        .store
        .lock()
        .unwrap()
        .doc_upsert(&id, &name, "active", None);
    let entry = Entry {
        doc,
        name: name.clone(),
        inviter: None,
        links: HashMap::new(),
    };
    record(shared, &id, &entry, &update, None);
    shared.docs.lock().unwrap().entries.insert(id.clone(), entry);
    Ok(json!({"id": id, "name": name}))
}

pub fn rpc_invite(shared: &Arc<Shared>, params: &Value) -> Result<Vec<(String, String)>, (i64, String)> {
    let id = doc_id(params)?;
    let to: Vec<String> = params["to"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    if to.is_empty() {
        return err(-32602, "doc/invite needs to: [contact ids]");
    }
    let contacts = shared.store.lock().unwrap().contacts();
    let mut chosen = vec![];
    for cid in &to {
        match contacts.iter().find(|c| c.id == *cid) {
            Some(c) => chosen.push(c),
            None => return err(-32602, format!("unknown contact {cid}")),
        }
    }
    let mut docs = shared.docs.lock().unwrap();
    let entry = docs.entries.get_mut(&id).ok_or((-32602, format!("no document {id}")))?;
    let mut invited = vec![];
    for c in chosen {
        let update = entry.doc.set_member(&c.id, &c.name);
        record(shared, &id, entry, &update, None);
        invited.push((c.id.clone(), entry.name.clone()));
    }
    Ok(invited)
}

pub fn rpc_list(shared: &Arc<Shared>) -> Value {
    let rows = shared.store.lock().unwrap().docs();
    let docs = shared.docs.lock().unwrap();
    let mine = my_id(shared);
    json!(rows
        .iter()
        .map(|r| {
            let members: Vec<Value> = docs
                .entries
                .get(&r.id)
                .map(|e| {
                    e.doc
                        .members()
                        .into_iter()
                        .filter(|(id, _)| *id != mine)
                        .map(|(id, name)| {
                            json!({"id": id, "name": name,
                                   "online": e.links.contains_key(&id)})
                        })
                        .collect()
                })
                .unwrap_or_default();
            json!({"id": r.id, "name": r.name, "state": r.state,
                   "inviter": r.inviter, "members": members})
        })
        .collect::<Vec<_>>())
}

pub fn rpc_join(shared: &Arc<Shared>, params: &Value) -> Result<Value, (i64, String)> {
    let id = doc_id(params)?;
    let row = shared
        .store
        .lock()
        .unwrap()
        .doc(&id)
        .ok_or((-32602, format!("no invitation for {id}")))?;
    if row.state != "active" {
        shared
            .store
            .lock()
            .unwrap()
            .doc_upsert(&id, &row.name, "active", row.inviter.as_deref());
        shared.docs.lock().unwrap().entries.insert(
            id.clone(),
            Entry {
                doc: SharedDoc::new(),
                name: row.name.clone(),
                inviter: row.inviter.clone(),
                links: HashMap::new(),
            },
        );
    }
    open_links_for_doc(shared, &id);
    Ok(json!({"id": id, "name": row.name}))
}

pub fn rpc_leave(shared: &Arc<Shared>, params: &Value) -> Result<Value, (i64, String)> {
    let id = doc_id(params)?;
    shared.docs.lock().unwrap().entries.remove(&id);
    shared.store.lock().unwrap().doc_delete(&id);
    Ok(json!({"id": id}))
}

pub fn rpc_attach(shared: &Arc<Shared>, params: &Value, client: u64) -> Result<Value, (i64, String)> {
    let id = doc_id(params)?;
    let mut docs = shared.docs.lock().unwrap();
    let entry = docs.entries.get_mut(&id).ok_or((-32602, format!("no document {id}")))?;
    let text = entry.doc.attach(client);
    Ok(json!({"id": id, "name": entry.name, "text": text, "size": text.chars().count()}))
}

pub fn rpc_detach(shared: &Arc<Shared>, params: &Value, client: u64) -> Result<Value, (i64, String)> {
    let id = doc_id(params)?;
    if let Some(e) = shared.docs.lock().unwrap().entries.get_mut(&id) {
        e.doc.detach(client);
    }
    Ok(json!({"id": id}))
}

pub fn client_gone(shared: &Arc<Shared>, client: u64) {
    for e in shared.docs.lock().unwrap().entries.values_mut() {
        e.doc.detach(client);
    }
}

pub fn rpc_sync(shared: &Arc<Shared>, params: &Value, client: u64) -> Result<Value, (i64, String)> {
    let id = doc_id(params)?;
    let ops: Vec<Op> = serde_json::from_value(params["ops"].clone())
        .map_err(|e| (-32602, format!("bad ops: {e}")))?;
    let cursor = params["cursor"].as_u64().map(|c| c as usize);
    let mut docs = shared.docs.lock().unwrap();
    let entry = docs.entries.get_mut(&id).ok_or((-32602, format!("no document {id}")))?;
    let out = entry.doc.sync(client, &ops, cursor).map_err(|e| (-32001, e))?;
    if let Some(update) = &out.update {
        record(shared, &id, entry, update, None);
        for other in entry.doc.mark_dirty_except(client) {
            shared
                .notifier
                .notify_client(other, "doc/dirty", json!({"id": id}));
        }
    }
    if let Some(cur) = &out.cursor {
        let msg = WireMsg::Cur { b: b64(cur) };
        for tx in entry.links.values() {
            tx.send(msg.clone()).ok();
        }
    }
    Ok(json!({
        "ops": out.ops,
        "size": out.size,
        "peers": out.peers.iter().map(|(id, name, pos)| {
            json!({"id": id, "name": name, "pos": pos})
        }).collect::<Vec<_>>(),
    }))
}

fn doc_id(params: &Value) -> Result<String, (i64, String)> {
    params["id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(String::from)
        .ok_or((-32602, "missing document id".into()))
}

pub fn shares_doc_with(shared: &Arc<Shared>, peer: &str) -> bool {
    shared
        .docs
        .lock()
        .unwrap()
        .entries
        .values()
        .any(|e| e.allows(peer))
}

pub fn on_invite(shared: &Arc<Shared>, peer: &PeerInfo, body: &str) {
    let Ok(v) = serde_json::from_str::<Value>(body) else {
        return;
    };
    let (Some(id), Some(name)) = (v["id"].as_str(), v["name"].as_str()) else {
        return;
    };
    {
        let store = shared.store.lock().unwrap();
        if let Some(row) = store.doc(id) {
            if row.state == "active" || row.inviter.as_deref() != Some(&peer.id) {
                tracing::warn!(peer = %peer.id, doc = id, "ignoring invite for a doc we already hold");
                return;
            }
        }
        store.doc_upsert(id, name, "invited", Some(&peer.id));
    }
    shared.notifier.notify(
        "doc/invited",
        json!({"id": id, "name": name, "from": peer.id, "from-name": peer.name}),
    );
}

pub fn open_links(shared: &Arc<Shared>, peer_id: &str) {
    let ids: Vec<String> = shared
        .docs
        .lock()
        .unwrap()
        .entries
        .iter()
        .filter(|(_, e)| e.allows(peer_id))
        .map(|(id, _)| id.clone())
        .collect();
    for id in ids {
        spawn_open(shared.clone(), peer_id.to_string(), id);
    }
}

fn open_links_for_doc(shared: &Arc<Shared>, id: &str) {
    let peers: Vec<String> = {
        let docs = shared.docs.lock().unwrap();
        let Some(e) = docs.entries.get(id) else {
            return;
        };
        let mut p: Vec<String> = e.doc.members().into_iter().map(|(id, _)| id).collect();
        p.extend(e.inviter.clone());
        p
    };
    let mine = my_id(shared);
    for peer in peers.into_iter().filter(|p| *p != mine) {
        match shared.peers.lock().unwrap().get(&peer) {
            Some(tx) => {
                tx.send(PeerCmd::Nudge).ok();
            }
            None => continue,
        }
        spawn_open(shared.clone(), peer, id.to_string());
    }
}

fn spawn_open(shared: Arc<Shared>, peer_id: String, doc_id: String) {
    tokio::spawn(async move {
        let Some(conn) = shared.live_connection(&peer_id) else {
            return;
        };
        let Some(peer) = crate::peer::peer_info(&shared, &peer_id) else {
            return;
        };
        let Ok((mut send, recv)) = conn.open_bi().await else {
            return;
        };
        if write_msg(&mut send, &WireMsg::Doc { id: doc_id.clone() })
            .await
            .is_err()
        {
            return;
        }
        drive(&shared, &peer, &doc_id, send, BufReader::new(recv)).await;
    });
}

pub async fn serve_inbound(
    shared: &Arc<Shared>,
    peer: &PeerInfo,
    id: &str,
    send: iroh::endpoint::SendStream,
    recv: BufReader<iroh::endpoint::RecvStream>,
) {
    let allowed = shared
        .docs
        .lock()
        .unwrap()
        .entries
        .get(id)
        .map(|e| e.allows(&peer.id))
        .unwrap_or(false);
    if !allowed {
        tracing::warn!(peer = %peer.id, doc = id, "refusing doc stream: not a member");
        return;
    }
    drive(shared, peer, id, send, recv).await;
}

async fn drive(
    shared: &Arc<Shared>,
    peer: &PeerInfo,
    id: &str,
    mut send: iroh::endpoint::SendStream,
    mut recv: BufReader<iroh::endpoint::RecvStream>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<WireMsg>();
    {
        let mut docs = shared.docs.lock().unwrap();
        let Some(e) = docs.entries.get_mut(id) else {
            return;
        };
        e.links.insert(peer.id.clone(), tx.clone());
    }
    tracing::debug!(peer = %peer.id, doc = id, "doc stream up");

    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if write_msg(&mut send, &msg).await.is_err() {
                break;
            }
        }
        send.finish().ok();
    });

    let send_sv = |tx: &mpsc::UnboundedSender<WireMsg>| {
        let sv = shared
            .docs
            .lock()
            .unwrap()
            .entries
            .get(id)
            .map(|e| e.doc.state_vector());
        if let Some(sv) = sv {
            tx.send(WireMsg::Sv { b: b64(&sv) }).ok();
        }
    };
    send_sv(&tx);

    let mut tick = tokio::time::interval(RESYNC_EVERY);
    tick.tick().await;
    loop {
        let msg = tokio::select! {
            m = read_msg(&mut recv) => match m { Some(m) => m, None => break },
            _ = tick.tick() => { send_sv(&tx); continue; }
        };
        let result = match msg {
            WireMsg::Sv { b } => unb64(&b).and_then(|sv| {
                let diff = shared
                    .docs
                    .lock()
                    .unwrap()
                    .entries
                    .get(id)
                    .ok_or_else(|| "document gone".to_string())?
                    .doc
                    .diff(&sv)?;
                tx.send(WireMsg::Up { b: b64(&diff) }).ok();
                Ok(())
            }),
            WireMsg::Up { b } => unb64(&b).and_then(|u| apply_from_peer(shared, id, &peer.id, &u)),
            WireMsg::Cur { b } => unb64(&b).and_then(|c| {
                let mut docs = shared.docs.lock().unwrap();
                let e = docs
                    .entries
                    .get_mut(id)
                    .ok_or_else(|| "document gone".to_string())?;
                e.doc.set_peer_cursor(&peer.id, &peer.name, &c)?;
                notify_dirty(shared, id, e);
                Ok(())
            }),
            _ => Err("unexpected message on doc stream".into()),
        };
        if let Err(e) = result {
            tracing::warn!(peer = %peer.id, doc = id, "doc stream: {e}");
            break;
        }
    }

    writer.abort();
    let mut docs = shared.docs.lock().unwrap();
    if let Some(e) = docs.entries.get_mut(id) {
        if e.links.get(&peer.id).is_some_and(|t| t.same_channel(&tx)) {
            e.links.remove(&peer.id);
            e.doc.clear_peer_cursor(&peer.id);
            notify_dirty(shared, id, e);
        }
    }
    tracing::debug!(peer = %peer.id, doc = id, "doc stream down");
}

fn apply_from_peer(shared: &Arc<Shared>, id: &str, from: &str, update: &[u8]) -> Result<(), String> {
    let mut docs = shared.docs.lock().unwrap();
    let e = docs
        .entries
        .get_mut(id)
        .ok_or_else(|| "document gone".to_string())?;
    let members_before = e.doc.members().len();
    match e.doc.apply_remote(update) {
        Ok(true) => {
            record(shared, id, e, update, Some(from));
            notify_dirty(shared, id, e);
            if e.doc.members().len() != members_before {
                let missing: Vec<String> = e
                    .doc
                    .members()
                    .into_iter()
                    .map(|(m, _)| m)
                    .filter(|m| !e.links.contains_key(m) && shared.is_connected(m))
                    .collect();
                for m in missing {
                    spawn_open(shared.clone(), m, id.to_string());
                }
            }
            Ok(())
        }
        Ok(false) => Ok(()),
        Err(err) if err.starts_with(gossipd_core::doc::CORRUPT) => {
            tracing::error!(peer = from, doc = id, "{err}; rebuilding document from log");
            let (snapshot, updates) = shared.store.lock().unwrap().doc_state(id);
            let fresh = SharedDoc::load(snapshot.as_deref(), updates.iter().map(Vec::as_slice))
                .map_err(|e| format!("cannot rebuild document: {e}"))?;
            let clients = e.doc.attached();
            e.doc = fresh;
            for client in clients {
                shared
                    .notifier
                    .notify_client(client, "doc/dirty", json!({"id": id}));
            }
            Err(err)
        }
        Err(err) => Err(err),
    }
}
