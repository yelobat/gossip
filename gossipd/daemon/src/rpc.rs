use std::ops::Deref;
use std::sync::{Arc, Mutex};

use gossipd_core::backoff::BackoffCfg;
use gossipd_core::identity::{node_id, ROLE_IROH};
use gossipd_core::log::LogEntry;
use gossipd_core::ticket::{OnionEndpoint, Ticket};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::keys::Keys;
use crate::net;
use crate::peer::ensure_peer;
use crate::state::{msg_id, now_ts, PeerCmd, Shared};
use crate::store::{Contact, Store};
use crate::Notifier;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "kebab-case", default)]
pub struct TorCfg {
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "kebab-case", default)]
pub struct TransportCfg {
    pub allow_relays: bool,
    pub relay_urls: Vec<String>,
    pub tor: TorCfg,
    pub advertised_addrs: Vec<String>,
}

pub struct Session {
    pub keys: Keys,
    pub shared: Arc<Shared>,
}

pub struct Daemon {
    notifier: Notifier,
    display_name: String,
    backoff: BackoffCfg,
    transport: TransportCfg,
    session: Option<Session>,
}

impl Daemon {
    pub fn new(notifier: Notifier) -> Self {
        Self {
            notifier,
            display_name: whoami(),
            backoff: BackoffCfg::default(),
            transport: TransportCfg::default(),
            session: None,
        }
    }

    fn node_id(&self) -> String {
        match &self.session {
            Some(s) => node_id(&s.keys.master.public()),
            None => "gsp1-uninitialized".into(),
        }
    }

    fn session(&self) -> Result<&Session, (i64, String)> {
        self.session
            .as_ref()
            .ok_or((-32002, "daemon not initialized, call init first".into()))
    }

    pub async fn handle(&mut self, request: Value, origin: &crate::ClientHandle) {
        let method = request["method"].as_str().unwrap_or("").to_string();
        let id = request["id"].clone();
        let params = request["params"].clone();
        let result = self.dispatch(&method, params, &id, origin).await;
        if id.is_null() {
            return;
        }
        match result {
            Ok(result) => origin.respond(&id, result),
            Err((code, message)) => origin.respond_error(&id, code, message),
        }
    }

    async fn dispatch(
        &mut self,
        method: &str,
        params: Value,
        id: &Value,
        origin: &crate::ClientHandle,
    ) -> Result<Value, (i64, String)> {
        match method {
            "init" => self.init(params).await,
            "identity/get" => Ok(self.identity()),
            "identity/setName" => {
                if let Some(name) = params["name"].as_str() {
                    self.display_name = name.to_string();
                    if let Some(s) = &self.session {
                        s.shared
                            .store
                            .lock()
                            .unwrap()
                            .meta_set("display_name", name);
                    }
                }
                Ok(json!({"ok": true}))
            }
            "contact/list" => Ok(json!(self.contact_list())),
            "contact/makeTicket" => self.make_ticket(),
            "contact/addTicket" => self.add_ticket(params),
            "msg/send" => self.msg_send(params),
            "msg/history" => self.msg_history(params),
            "blob/send" => self.blob_send(params),
            "config/setDownloadsDir" => self.set_downloads_dir(params),
            "config/setFilePolicy" => self.set_file_policy(params),
            "config/setContactFilePolicy" => self.set_contact_file_policy(params),
            "file/respond" => self.file_respond(params),
            "profile/export" => self.profile_export(params),
            "profile/import" => self.profile_import(params),
            "net/check" => self.net_check(),
            "peer/disconnect" => {
                let s = self.session()?;
                let id = params["id"].as_str().unwrap_or("");
                let had = s.shared.live_connection(id).map(|c| c.close(0u32.into(), b"disconnect"));
                Ok(json!({"id": id, "disconnected": had.is_some()}))
            }
            "doc/create" => {
                let s = self.session()?;
                crate::docs::rpc_create(&s.shared, &params, &self.display_name)
            }
            "doc/invite" => self.doc_invite(params),
            "doc/list" => Ok(crate::docs::rpc_list(&self.session()?.shared)),
            "doc/join" => crate::docs::rpc_join(&self.session()?.shared, &params),
            "doc/leave" => crate::docs::rpc_leave(&self.session()?.shared, &params),
            "doc/attach" => crate::docs::rpc_attach(&self.session()?.shared, &params, origin.id()),
            "doc/detach" => crate::docs::rpc_detach(&self.session()?.shared, &params, origin.id()),
            "doc/sync" => crate::docs::rpc_sync(&self.session()?.shared, &params, origin.id()),
            "client/gone" => {
                if let Some(s) = &self.session {
                    crate::docs::client_gone(&s.shared, origin.id());
                }
                Ok(json!({"ok": true}))
            }
            "status" => Ok(self.status()),
            "shutdown" => {
                origin.respond(id, json!({"ok": true}));
                tracing::info!("shutdown");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                std::process::exit(0);
            }
            _ => Err((-32601, format!("unknown method {method}"))),
        }
    }

    async fn init(&mut self, params: Value) -> Result<Value, (i64, String)> {
        if let Some(s) = &self.session {
            if let Some(name) = params["display-name"].as_str().filter(|n| !n.is_empty()) {
                self.display_name = name.to_string();
                s.shared
                    .store
                    .lock()
                    .unwrap()
                    .meta_set("display_name", name);
            }
            return Ok(self.identity());
        }
        if let Ok(backoff) = serde_json::from_value(params["backoff"].clone()) {
            self.backoff = backoff;
        }
        if let Ok(transport) = serde_json::from_value::<TransportCfg>(params["transport"].clone()) {
            self.transport = transport;
        }

        let data_dir = std::path::PathBuf::from(
            params["data-dir"]
                .as_str()
                .ok_or((-32602, "init needs data-dir".to_string()))?,
        );
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| (-32603, format!("cannot create data-dir: {e}")))?;
        let keys = Keys::load_or_create(&data_dir)
            .map_err(|e| (-32603, format!("cannot load identity: {e}")))?;
        let store =
            Store::open(&data_dir).map_err(|e| (-32603, format!("cannot open database: {e}")))?;

        match params["display-name"].as_str().filter(|n| !n.is_empty()) {
            Some(name) => {
                self.display_name = name.to_string();
                store.meta_set("display_name", name);
            }
            None => {
                if let Some(stored) = store.meta_get("display_name") {
                    self.display_name = stored;
                }
            }
        }

        let downloads_dir = match params["downloads-dir"].as_str().filter(|s| !s.is_empty()) {
            Some(path) => {
                store.meta_set("downloads_dir", path);
                std::path::PathBuf::from(path)
            }
            None => store
                .meta_get("downloads_dir")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| data_dir.join("downloads")),
        };

        let (endpoint, mdns) = net::build_endpoint(&keys.iroh_secret, &self.transport)
            .await
            .map_err(|e| (-32603, e))?;
        let blob_store = iroh_blobs::store::fs::FsStore::load(data_dir.join("blobs"))
            .await
            .map_err(|e| (-32603, format!("cannot open blob store: {e}")))?;
        let blobs: iroh_blobs::api::Store = blob_store.deref().clone();

        let events = {
            use iroh_blobs::provider::events::{EventMask, EventSender, RequestMode};
            let (tx, _rx) = tokio::sync::mpsc::channel(1);
            EventSender::new(
                tx,
                EventMask {
                    push: RequestMode::None,
                    ..EventMask::DEFAULT
                },
            )
        };
        let blobs_proto = iroh_blobs::BlobsProtocol::new(&blobs, Some(events));
        let shared = Arc::new(Shared {
            notifier: self.notifier.clone(),
            store: Mutex::new(store),
            endpoint,
            blobs,
            blobs_proto,
            data_dir: data_dir.clone(),
            downloads_dir: Mutex::new(downloads_dir),
            backoff: self.backoff.clone(),
            my_master: keys.master.public().to_bytes(),
            peers: Mutex::new(Default::default()),
            conns: Mutex::new(Default::default()),
            inbound_seen: Mutex::new(false),
            pending_files: Mutex::new(Default::default()),
            tor: Mutex::new(None),
            docs: Mutex::new(Default::default()),
        });
        net::spawn_accept_loop(shared.clone());
        if let Some(mdns) = mdns {
            net::spawn_discovery_nudges(shared.clone(), mdns);
        }

        if self.transport.tor.enabled {
            self.notifier.notify(
                "tor/status",
                json!({"state": "bootstrapping", "percent": 0}),
            );
            crate::tor::launch(
                shared.clone(),
                keys.master.to_bytes(),
                keys.tor_secret,
                keys.tor_cert.clone(),
            );
        }

        crate::docs::load_all(&shared);
        for contact in shared.store.lock().unwrap().contacts() {
            ensure_peer(&shared, &contact.id);
        }

        self.session = Some(Session { keys, shared });
        tracing::info!(node = %self.node_id(), backoff = ?self.backoff,
                       transport = ?self.transport, "init");
        Ok(self.identity())
    }

    fn identity(&self) -> Value {
        json!({"node-id": self.node_id(), "display-name": self.display_name})
    }

    fn make_ticket(&self) -> Result<Value, (i64, String)> {
        let s = self.session()?;

        let onion = s
            .shared
            .tor
            .lock()
            .unwrap()
            .as_ref()
            .map(|t| OnionEndpoint {
                addr: t.onion.clone(),
                cert: t.onion_cert.clone(),
            });
        let ticket = Ticket {
            master_pub: s.keys.master.public().to_bytes(),
            name: self.display_name.clone(),
            cert: s.keys.cert.clone(),
            addrs: net::direct_addrs(&s.shared.endpoint, &self.transport.advertised_addrs),
            onion,
        };
        Ok(json!({"ticket": ticket.encode()}))
    }

    fn add_ticket(&mut self, params: Value) -> Result<Value, (i64, String)> {
        let s = self.session()?;
        let raw = params["ticket"].as_str().unwrap_or("");
        let ticket = Ticket::decode(raw).map_err(|e| (-32602, e))?;
        let master = ed25519_dalek::VerifyingKey::from_bytes(&ticket.master_pub)
            .map_err(|_| (-32602, "bad master key".to_string()))?;
        if ticket.master_pub == s.keys.master.public().to_bytes() {
            return Err((-32602, "that is your own ticket".into()));
        }
        debug_assert!(ticket.cert.verify(&master, ROLE_IROH));
        let name = params["name"]
            .as_str()
            .filter(|n| !n.is_empty())
            .unwrap_or(&ticket.name)
            .to_string();
        let contact = Contact {
            id: node_id(&master),
            master_pub: ticket.master_pub,
            name: name.clone(),
            endpoint_id: ticket.cert.subkey,
            cert: postcard::to_stdvec(&ticket.cert).expect("cert encodes"),
            addrs: ticket.addrs.clone(),
            onion: ticket.onion.as_ref().map(|o| o.addr.clone()),
        };
        s.shared.store.lock().unwrap().upsert_contact(&contact);

        ensure_peer(&s.shared, &contact.id)
            .send(PeerCmd::Nudge)
            .ok();
        tracing::info!(id = %contact.id, name = %name, "contact added");
        Ok(json!({"id": contact.id, "name": name,
                  "online": s.shared.is_connected(&contact.id)}))
    }

    fn msg_send(&mut self, params: Value) -> Result<Value, (i64, String)> {
        let s = self.session()?;
        let to = params["to"].as_str().unwrap_or("");
        let contact = s
            .shared
            .store
            .lock()
            .unwrap()
            .contact(to)
            .ok_or((-32602, format!("unknown recipient {to}")))?;
        let kind = params["kind"].as_str().unwrap_or("chat");
        let body = params["body"].as_str().unwrap_or("");
        let (entry, online) = {
            let store = s.shared.store.lock().unwrap();
            let seq = store.frontier(&s.shared.my_master, &contact.master_pub) + 1;
            let entry = LogEntry::sign(
                &s.keys.master,
                contact.master_pub,
                seq,
                kind,
                body,
                now_ts(),
            );
            store.append(&entry);
            store.enqueue(&contact.master_pub, seq);
            (entry, s.shared.is_connected(&contact.id))
        };
        ensure_peer(&s.shared, &contact.id).send(PeerCmd::Push).ok();
        self.notifier.notify(
            "msg/sent",
            json!({
                "id": msg_id(entry.seq),
                "to": contact.id,
                "to-name": contact.name,
                "kind": kind,
                "body": body,
                "ts": entry.ts,
            }),
        );
        Ok(json!({
            "msg-id": msg_id(entry.seq),
            "status": if online { "sent" } else { "queued" },
        }))
    }

    fn doc_invite(&mut self, params: Value) -> Result<Value, (i64, String)> {
        let id = params["id"].as_str().unwrap_or("").to_string();
        let invited = crate::docs::rpc_invite(&self.session()?.shared, &params)?;
        for (to, name) in &invited {
            let body = json!({"id": id, "name": name}).to_string();
            self.msg_send(json!({"to": to, "kind": crate::docs::KIND_INVITE, "body": body}))?;
        }
        Ok(json!({"id": id, "invited": invited.iter().map(|(to, _)| to).collect::<Vec<_>>()}))
    }

    fn set_downloads_dir(&self, params: Value) -> Result<Value, (i64, String)> {
        let s = self.session()?;
        let path = params["path"]
            .as_str()
            .filter(|p| !p.is_empty())
            .ok_or((-32602, "setDownloadsDir needs path".to_string()))?;
        let path = std::path::PathBuf::from(shellexpand_home(path));
        std::fs::create_dir_all(&path)
            .map_err(|e| (-32603, format!("cannot create {}: {e}", path.display())))?;
        s.shared
            .store
            .lock()
            .unwrap()
            .meta_set("downloads_dir", &path.to_string_lossy());
        *s.shared.downloads_dir.lock().unwrap() = path.clone();
        Ok(json!({"downloads-dir": path.to_string_lossy()}))
    }

    fn set_file_policy(&self, params: Value) -> Result<Value, (i64, String)> {
        let s = self.session()?;
        let default = params["default"].as_str().unwrap_or("");
        if !matches!(default, "accept" | "reject" | "ask") {
            return Err((-32602, "default must be accept, reject, or ask".into()));
        }
        let store = s.shared.store.lock().unwrap();
        store.meta_set("files_default", default);
        Ok(file_policy_json(&store))
    }

    fn set_contact_file_policy(&self, params: Value) -> Result<Value, (i64, String)> {
        let s = self.session()?;
        let id = params["id"]
            .as_str()
            .filter(|i| !i.is_empty())
            .ok_or((-32602, "setContactFilePolicy needs id".to_string()))?;
        let policy = params["policy"].as_str().unwrap_or("");
        if !matches!(policy, "accept" | "reject" | "ask" | "default") {
            return Err((-32602, "policy must be accept, reject, ask, or default".into()));
        }
        s.shared
            .store
            .lock()
            .unwrap()
            .meta_set(&format!("filepol:{id}"), policy);
        Ok(json!({"id": id, "policy": policy}))
    }

    fn file_respond(&self, params: Value) -> Result<Value, (i64, String)> {
        let s = self.session()?;
        let id = params["id"]
            .as_str()
            .ok_or((-32602, "file/respond needs id".to_string()))?;
        let accept = params["accept"].as_bool().unwrap_or(false);
        let pending = s.shared.pending_files.lock().unwrap().remove(id);
        let Some(p) = pending else {
            return Err((-32602, format!("no pending file {id}")));
        };
        if accept {
            tokio::spawn(crate::blob::received(s.shared.clone(), p.peer, p.hash, p.name));
        } else {
            s.shared.notifier.notify(
                "file/declined",
                json!({"from": p.peer.id, "from-name": p.peer.name, "name": p.name}),
            );
        }
        Ok(json!({"id": id, "accepted": accept}))
    }

    fn msg_history(&self, params: Value) -> Result<Value, (i64, String)> {
        let s = self.session()?;
        let peer_id = params["peer-id"].as_str().unwrap_or("");
        let limit = params["limit"].as_u64().unwrap_or(100) as u32;
        let store = s.shared.store.lock().unwrap();
        let Some(contact) = store.contact(peer_id) else {
            return Ok(json!([]));
        };
        let my_id = self.node_id();
        let messages: Vec<Value> = store
            .history(&s.shared.my_master, &contact.master_pub, limit)
            .iter()
            .map(|e| {
                let mine = e.author == s.shared.my_master;
                json!({
                    "id": msg_id(e.seq),
                    "from": if mine { my_id.clone() } else { contact.id.clone() },
                    "from-name": if mine { self.display_name.clone() } else { contact.name.clone() },
                    "kind": e.kind,
                    "body": e.body,
                    "ts": e.ts,
                })
            })
            .collect();
        Ok(json!(messages))
    }

    fn blob_send(&self, params: Value) -> Result<Value, (i64, String)> {
        let s = self.session()?;
        let to = params["to"].as_str().unwrap_or("");
        let contact = s
            .shared
            .store
            .lock()
            .unwrap()
            .contact(to)
            .ok_or((-32602, format!("unknown recipient {to}")))?;
        let path = std::path::PathBuf::from(
            params["path"]
                .as_str()
                .ok_or((-32602, "blob/send needs path".to_string()))?,
        );
        let transfer_id = format!(
            "t{:04}",
            (crate::state::jitter_roll().abs() * 9999.0) as u32
        );
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| to.to_string());
        let (seq, ts) = {
            let store = s.shared.store.lock().unwrap();
            let seq = store.frontier(&s.shared.my_master, &contact.master_pub) + 1;
            let ts = now_ts();
            let entry = LogEntry::sign(&s.keys.master, contact.master_pub, seq, "file", &name, ts);
            store.append(&entry);
            store.enqueue(&contact.master_pub, seq);
            (seq, ts)
        };
        ensure_peer(&s.shared, &contact.id).send(PeerCmd::Push).ok();
        self.notifier.notify(
            "msg/sent",
            json!({
                "id": msg_id(seq),
                "to": contact.id,
                "to-name": contact.name,
                "kind": "file",
                "body": name,
                "ts": ts,
            }),
        );
        tokio::spawn(crate::blob::send(
            s.shared.clone(),
            contact,
            path,
            transfer_id.clone(),
        ));
        Ok(json!({"transfer-id": transfer_id}))
    }

    fn profile_export(&self, params: Value) -> Result<Value, (i64, String)> {
        let s = self.session()?;
        let path = params["path"]
            .as_str()
            .filter(|p| !p.is_empty())
            .ok_or((-32602, "profile/export needs path".to_string()))?;
        let path = std::path::PathBuf::from(shellexpand_home(path));
        let tmp = s.shared.data_dir.join(".export-snapshot.db");
        s.shared
            .store
            .lock()
            .unwrap()
            .snapshot(&tmp)
            .map_err(|e| (-32603, format!("database snapshot failed: {e}")))?;
        let read = |name: &str| std::fs::read(s.shared.data_dir.join(name));
        let db = std::fs::read(&tmp).map_err(|e| (-32603, e.to_string()));
        std::fs::remove_file(&tmp).ok();
        let archive = gossipd_core::profile::ProfileArchive {
            version: gossipd_core::profile::VERSION,
            identity: read("identity.key").map_err(|e| (-32603, e.to_string()))?,
            iroh: read("iroh.key").map_err(|e| (-32603, e.to_string()))?,
            tor: read("tor.key").map_err(|e| (-32603, e.to_string()))?,
            db: db.map_err(|e| (-32603, e.1))?,
        };
        std::fs::write(&path, archive.encode())
            .map_err(|e| (-32603, format!("cannot write {}: {e}", path.display())))?;
        Ok(json!({"path": path.to_string_lossy(), "node-id": self.node_id()}))
    }

    fn profile_import(&self, params: Value) -> Result<Value, (i64, String)> {
        let path = params["path"]
            .as_str()
            .filter(|p| !p.is_empty())
            .ok_or((-32602, "profile/import needs path".to_string()))?;
        let data_dir = params["data-dir"]
            .as_str()
            .filter(|p| !p.is_empty())
            .ok_or((-32602, "profile/import needs data-dir".to_string()))?;
        let dir = std::path::PathBuf::from(shellexpand_home(data_dir));
        if let Some(s) = &self.session {
            if s.shared.data_dir == dir {
                return Err((-32002, "cannot import over the running profile; import into \
                                     a fresh data-dir and start a daemon there"
                    .into()));
            }
        }
        if dir.join("identity.key").exists() && !params["overwrite"].as_bool().unwrap_or(false) {
            return Err((
                -32602,
                format!("{} already has a profile (pass overwrite to replace)", dir.display()),
            ));
        }
        let bytes = std::fs::read(shellexpand_home(path))
            .map_err(|e| (-32603, format!("cannot read {path}: {e}")))?;
        let archive = gossipd_core::profile::ProfileArchive::decode(&bytes).map_err(|e| (-32602, e))?;
        archive
            .unpack_into_dir(&dir)
            .map_err(|e| (-32603, format!("cannot write profile: {e}")))?;
        let node = node_id_of(&archive.identity)
            .ok_or((-32602, "archive has an invalid identity key".to_string()))?;
        Ok(json!({"data-dir": dir.to_string_lossy(), "node-id": node}))
    }

    fn net_check(&self) -> Result<Value, (i64, String)> {
        let s = self.session()?;
        let inbound = *s.shared.inbound_seen.lock().unwrap();
        if !inbound {
            self.notifier.notify(
                "log",
                json!({"level": "info",
                       "message": "no inbound direct connection observed this run, \
                                   if peers can't reach you, you are in dial-out-only mode"}),
            );
        }
        Ok(json!({"inbound-direct": inbound, "checked-via": "observed-traffic"}))
    }

    fn contact_list(&self) -> Vec<Value> {
        let Some(s) = &self.session else {
            return vec![];
        };
        let contacts = s.shared.store.lock().unwrap().contacts();
        contacts
            .iter()
            .map(|c| {
                json!({"id": c.id, "name": c.name,
                            "online": s.shared.is_connected(&c.id)})
            })
            .collect()
    }

    fn status(&self) -> Value {
        let relay = if !self.transport.allow_relays || self.transport.relay_urls.is_empty() {
            "disabled, direct connections only".to_string()
        } else {
            format!("{} (self-hosted)", self.transport.relay_urls.join(", "))
        };
        let tor = if !self.transport.tor.enabled {
            "off".to_string()
        } else {
            match &self.session {
                Some(s) => match s.shared.tor.lock().unwrap().as_ref() {
                    Some(t) => t.onion.clone(),
                    None => "bootstrapping".to_string(),
                },
                None => "bootstrapping".to_string(),
            }
        };
        let (queue, inbound) = match &self.session {
            Some(s) => {
                let store = s.shared.store.lock().unwrap();
                let now = now_ts();
                let queue: Vec<Value> = store
                    .queued()
                    .iter()
                    .map(|q| {
                        let name = store
                            .contact_by_master(&q.recipient)
                            .map(|c| c.name)
                            .unwrap_or_default();
                        json!({
                            "msg-id": msg_id(q.seq),
                            "to-name": name,
                            "attempts": q.attempts,
                            "next-in-seconds": ((q.next_at - now).max(0.0) * 10.0).round() / 10.0,
                        })
                    })
                    .collect();
                let inbound = if *s.shared.inbound_seen.lock().unwrap() {
                    json!(true)
                } else {
                    json!("unchecked")
                };
                (queue, inbound)
            }
            None => (vec![], json!("unchecked")),
        };
        let downloads = self
            .session
            .as_ref()
            .map(|s| s.shared.downloads_dir.lock().unwrap().to_string_lossy().into_owned())
            .unwrap_or_default();
        let files = self
            .session
            .as_ref()
            .map(|s| file_policy_json(&s.shared.store.lock().unwrap()))
            .unwrap_or_else(|| json!({"default": "accept"}));
        json!({
            "node-id": self.node_id(),
            "online": true,
            "relay": relay,
            "tor": tor,
            "inbound-direct": inbound,
            "advertised-addrs": self.transport.advertised_addrs,
            "downloads-dir": downloads,
            "files": files,
            "contacts": self.contact_list(),
            "queue": queue,
        })
    }
}

fn whoami() -> String {
    std::env::var("USER").unwrap_or_else(|_| "anon".into())
}

fn file_policy_json(store: &Store) -> Value {
    let default = store
        .meta_get("files_default")
        .unwrap_or_else(|| "accept".into());
    json!({ "default": default })
}

fn node_id_of(identity: &[u8]) -> Option<String> {
    let bytes: [u8; 32] = identity.try_into().ok()?;
    let master = gossipd_core::identity::MasterKey::from_bytes(&bytes);
    Some(node_id(&master.public()))
}

fn shellexpand_home(path: &str) -> String {
    match path.strip_prefix("~/") {
        Some(rest) => match std::env::var("HOME") {
            Ok(home) => format!("{home}/{rest}"),
            Err(_) => path.to_string(),
        },
        None => path.to_string(),
    }
}
