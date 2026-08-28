use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use gossip_client::serde_json::{json, Value};
use gossip_client::{Client, Notification};

#[derive(Message, Clone, Debug)]
pub enum GossipEvent {
    Received {
        from: String,
        from_name: String,
        kind: String,
        body: String,
        ts: f64,
    },
    Sent {
        to: String,
        to_name: String,
        kind: String,
        body: String,
        ts: f64,
    },
    Delivered {
        msg_id: String,
        to: String,
    },
    Log {
        message: String,
    },
    FileOffered {
        id: String,
        from: String,
        from_name: String,
        name: String,
        size: u64,
    },
    FileDeclined {
        from: String,
        from_name: String,
        name: String,
    },
    Ticket(String),
    ContactAdded {
        id: String,
        name: String,
    },
    Error(String),
}

type Inbox = Arc<Mutex<VecDeque<GossipEvent>>>;

#[derive(Resource)]
pub struct Gossip {
    client: Arc<Client>,
    node_id: String,
    inbox: Inbox,
}

impl Gossip {
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn send(&self, to: impl Into<String>, body: impl Into<String>) {
        self.fire("msg/send", json!({"to": to.into(), "body": body.into()}), |_| {
            None
        });
    }

    pub fn send_file(&self, to: impl Into<String>, path: impl Into<String>) {
        self.fire("blob/send", json!({"to": to.into(), "path": path.into()}), |_| {
            None
        });
    }

    pub fn add_contact(&self, ticket: impl Into<String>, name: impl Into<String>) {
        self.fire(
            "contact/addTicket",
            json!({"ticket": ticket.into(), "name": name.into()}),
            |v| {
                Some(GossipEvent::ContactAdded {
                    id: string(&v["id"]),
                    name: string(&v["name"]),
                })
            },
        );
    }

    pub fn make_ticket(&self) {
        self.fire("contact/makeTicket", json!({}), |v| {
            Some(GossipEvent::Ticket(string(&v["ticket"])))
        });
    }

    /// Answer a [`GossipEvent::FileOffered`] (policy "ask").
    pub fn respond_file(&self, id: impl Into<String>, accept: bool) {
        self.fire("file/respond", json!({"id": id.into(), "accept": accept}), |_| None);
    }

    fn fire(&self, method: &'static str, params: Value, on_ok: fn(&Value) -> Option<GossipEvent>) {
        let client = self.client.clone();
        let inbox = self.inbox.clone();
        std::thread::spawn(move || {
            let ev = match client.request(method, params) {
                Ok(v) => on_ok(&v),
                Err(e) => Some(GossipEvent::Error(e)),
            };
            if let Some(ev) = ev {
                inbox.lock().unwrap().push_back(ev);
            }
        });
    }
}

pub struct GossipPlugin {
    pub daemon: Vec<String>,
    pub data_dir: String,
    pub display_name: String,
}

impl Default for GossipPlugin {
    fn default() -> Self {
        Self {
            daemon: vec![std::env::var("GOSSIPD").unwrap_or_else(|_| "gossipd".into())],
            data_dir: std::env::var("GOSSIP_DATA_DIR").unwrap_or_else(|_| default_data_dir()),
            display_name: std::env::var("GOSSIP_NAME").unwrap_or_default(),
        }
    }
}

fn default_data_dir() -> String {
    if cfg!(windows) {
        if let Ok(appdata) = std::env::var("APPDATA") {
            return format!("{appdata}/gossip");
        }
    }
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        return format!("{xdg}/gossip");
    }
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| ".".into());
    format!("{home}/.local/share/gossip")
}

impl Plugin for GossipPlugin {
    fn build(&self, app: &mut App) {
        let control = format!("{}/gossip.port", self.data_dir);
        let (client, notifications) =
            Client::connect_or_spawn(&control, &self.daemon).expect("gossip daemon");
        let init = client
            .request(
                "init",
                json!({"data-dir": self.data_dir, "display-name": self.display_name}),
            )
            .expect("gossip init");
        let node_id = string(&init["node-id"]);

        let inbox: Inbox = Default::default();
        let merge = inbox.clone();
        std::thread::spawn(move || {
            for n in notifications {
                if let Some(ev) = notification_to_event(n) {
                    merge.lock().unwrap().push_back(ev);
                }
            }
        });

        app.insert_resource(Gossip {
            client: Arc::new(client),
            node_id,
            inbox,
        })
        .add_message::<GossipEvent>()
        .add_systems(Update, drain_inbox);
    }
}

fn drain_inbox(gossip: Res<Gossip>, mut writer: MessageWriter<GossipEvent>) {
    let mut inbox = gossip.inbox.lock().unwrap();
    for ev in inbox.drain(..) {
        writer.write(ev);
    }
}

fn string(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn notification_to_event(n: Notification) -> Option<GossipEvent> {
    let p = &n.params;
    match n.method.as_str() {
        "msg/received" => Some(GossipEvent::Received {
            from: string(&p["from"]),
            from_name: string(&p["from-name"]),
            kind: string(&p["kind"]),
            body: string(&p["body"]),
            ts: p["ts"].as_f64().unwrap_or(0.0),
        }),
        "msg/sent" => Some(GossipEvent::Sent {
            to: string(&p["to"]),
            to_name: string(&p["to-name"]),
            kind: string(&p["kind"]),
            body: string(&p["body"]),
            ts: p["ts"].as_f64().unwrap_or(0.0),
        }),
        "msg/delivered" => Some(GossipEvent::Delivered {
            msg_id: string(&p["msg-id"]),
            to: string(&p["to"]),
        }),
        "log" => p["message"]
            .as_str()
            .map(|m| GossipEvent::Log { message: m.into() }),
        "file/incoming" => Some(GossipEvent::FileOffered {
            id: string(&p["id"]),
            from: string(&p["from"]),
            from_name: string(&p["from-name"]),
            name: string(&p["name"]),
            size: p["size"].as_u64().unwrap_or(0),
        }),
        "file/declined" => Some(GossipEvent::FileDeclined {
            from: string(&p["from"]),
            from_name: string(&p["from-name"]),
            name: string(&p["name"]),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn note(method: &str, params: Value) -> Notification {
        Notification {
            method: method.to_string(),
            params,
        }
    }

    #[test]
    fn translates_known_notifications_only() {
        match notification_to_event(note(
            "msg/received",
            json!({"from": "gsp1-x", "from-name": "bob", "kind": "chat", "body": "hi", "ts": 5.0}),
        )) {
            Some(GossipEvent::Received { from_name, body, ts, .. }) => {
                assert_eq!(from_name, "bob");
                assert_eq!(body, "hi");
                assert_eq!(ts, 5.0);
            }
            _ => panic!("expected Received"),
        }
        assert!(matches!(
            notification_to_event(note("log", json!({"message": "up"}))),
            Some(GossipEvent::Log { .. })
        ));
        assert!(notification_to_event(note("queue/update", json!({}))).is_none());
    }
}
