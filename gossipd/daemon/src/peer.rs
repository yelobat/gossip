use std::sync::Arc;
use std::time::Duration;

use iroh::endpoint::Connection;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::state::{jitter_roll, msg_id, now_ts, PeerCmd, Shared};
use crate::{net, sync};
use sync::PeerInfo;

const DIAL_TIMEOUT: Duration = Duration::from_secs(5);

pub fn ensure_peer(shared: &Arc<Shared>, contact_id: &str) -> mpsc::UnboundedSender<PeerCmd> {
    let mut peers = shared.peers.lock().unwrap();
    if let Some(tx) = peers.get(contact_id) {
        if !tx.is_closed() {
            return tx.clone();
        }
    }
    let (tx, rx) = mpsc::unbounded_channel();
    peers.insert(contact_id.to_string(), tx.clone());
    tokio::spawn(run(shared.clone(), contact_id.to_string(), rx));
    tx
}

struct Link {
    conn: Connection,
    outbound: bool,
    driver: tokio::task::JoinHandle<()>,
}

enum DialOutcome {
    Direct(Connection),
    Tor,
}

enum Ev {
    Cmd(Option<PeerCmd>),
    DialTick,
    DialDone(Option<DialOutcome>),
    ConnClosed,
}

async fn run(shared: Arc<Shared>, contact_id: String, mut rx: mpsc::UnboundedReceiver<PeerCmd>) {
    let mut link: Option<Link> = None;
    let mut attempts: u32 = 0;

    let mut next_dial: Option<Instant> = Some(Instant::now());

    let mut dialing: Option<tokio::task::JoinHandle<Option<DialOutcome>>> = None;

    loop {
        let ev = {
            let dial_at = next_dial.filter(|_| link.is_none() && dialing.is_none());
            let dial_tick = async {
                match dial_at {
                    Some(t) => tokio::time::sleep_until(t).await,
                    None => std::future::pending().await,
                }
            };
            let conn_closed = async {
                match &link {
                    Some(l) => {
                        l.conn.closed().await;
                    }
                    None => std::future::pending().await,
                }
            };
            let dial_done = async {
                match &mut dialing {
                    Some(task) => task.await.unwrap_or(None),
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                cmd = rx.recv() => Ev::Cmd(cmd),
                _ = conn_closed => Ev::ConnClosed,
                conn = dial_done => Ev::DialDone(conn),
                _ = dial_tick => Ev::DialTick,
            }
        };

        match ev {
            Ev::Cmd(None) => return,
            Ev::Cmd(Some(PeerCmd::Inbound(conn))) => {
                adopt(&shared, &contact_id, &mut link, conn, false);
                if link.is_some() {
                    attempts = 0;
                    next_dial = None;
                }
            }
            Ev::Cmd(Some(PeerCmd::Push)) => match &link {
                Some(l) => {
                    if let Some(peer) = peer_info(&shared, &contact_id) {
                        let shared = shared.clone();
                        let conn = l.conn.clone();
                        tokio::spawn(async move {
                            let _ = sync::push_queued(&shared, &peer, &conn).await;
                        });
                    }
                }
                None => {
                    attempts = 0;
                    next_dial = Some(Instant::now());
                }
            },
            Ev::Cmd(Some(PeerCmd::Nudge)) => {
                attempts = 0;
                if link.is_none() {
                    next_dial = Some(Instant::now());
                }
            }
            Ev::ConnClosed => {
                let closed = link.take();
                if let Some(l) = &closed {
                    l.driver.abort();
                }

                {
                    let mut conns = shared.conns.lock().unwrap();
                    if let (Some(l), Some(current)) = (&closed, conns.get(&contact_id)) {
                        if current.stable_id() == l.conn.stable_id() {
                            conns.remove(&contact_id);
                        }
                    }
                }
                shared.notifier.notify(
                    "peer/presence",
                    json!({"peer-id": contact_id, "online": false}),
                );
                if has_queue(&shared, &contact_id) {
                    backoff_or_give_up(&shared, &contact_id, &mut attempts, &mut next_dial);
                } else {
                    attempts = 0;
                }
            }
            Ev::DialTick => {
                next_dial = None;
                tracing::debug!(peer = %contact_id, attempts, "dialing");
                let shared = shared.clone();
                let contact_id = contact_id.clone();
                dialing = Some(tokio::spawn(
                    async move { dial(&shared, &contact_id).await },
                ));
            }
            Ev::DialDone(result) => {
                dialing = None;
                match result {
                    Some(DialOutcome::Direct(conn)) => {
                        adopt(&shared, &contact_id, &mut link, conn, true);
                    }

                    Some(DialOutcome::Tor) => {
                        attempts = 0;
                        if has_queue(&shared, &contact_id) {
                            let delay = shared.backoff.delay_seconds(0, jitter_roll());
                            next_dial = Some(Instant::now() + Duration::from_secs_f64(delay));
                        }
                    }
                    None if link.is_some() => {}
                    None => {
                        backoff_or_give_up(&shared, &contact_id, &mut attempts, &mut next_dial)
                    }
                }
            }
        }
    }
}

fn backoff_or_give_up(
    shared: &Arc<Shared>,
    contact_id: &str,
    attempts: &mut u32,
    next_dial: &mut Option<Instant>,
) {
    let queued = queued_rows(shared, contact_id);
    if queued.1.is_empty() {
        return;
    }
    let delay = shared.backoff.delay_seconds(*attempts, jitter_roll());
    *attempts += 1;
    let (name, master) = queued.0;
    shared
        .store
        .lock()
        .unwrap()
        .queue_set_attempt(&master, *attempts, now_ts() + delay);
    if shared.backoff.gave_up(*attempts) {
        give_up(shared, contact_id, *attempts);
        return;
    }
    for seq in queued.1 {
        shared.notifier.notify(
            "queue/update",
            json!({
                "msg-id": msg_id(seq),
                "to": contact_id,
                "to-name": name,
                "attempts": *attempts,
                "delay-seconds": (delay * 100.0).round() / 100.0,
            }),
        );
    }
    tracing::debug!(peer = %contact_id, attempts = *attempts, delay, "delivery failed, backing off");
    *next_dial = Some(Instant::now() + Duration::from_secs_f64(delay));
}

fn give_up(shared: &Arc<Shared>, contact_id: &str, attempts: u32) {
    let name = peer_info(shared, contact_id)
        .map(|p| p.name)
        .unwrap_or_else(|| contact_id.to_string());
    tracing::info!(peer = %contact_id, attempts, "pausing redial after too many attempts");
    shared.notifier.notify(
        "log",
        json!({
            "level": "warn",
            "message": format!(
                "paused delivery to {name} after {attempts} attempts; \
                 will resume if they reconnect or you re-add their ticket"
            ),
        }),
    );
}

fn peer_info(shared: &Arc<Shared>, contact_id: &str) -> Option<PeerInfo> {
    let store = shared.store.lock().unwrap();
    store.contact(contact_id).map(|c| PeerInfo {
        id: c.id,
        name: c.name,
        master: c.master_pub,
    })
}

fn has_queue(shared: &Arc<Shared>, contact_id: &str) -> bool {
    !queued_rows(shared, contact_id).1.is_empty()
}

fn queued_rows(shared: &Arc<Shared>, contact_id: &str) -> ((String, [u8; 32]), Vec<u64>) {
    let store = shared.store.lock().unwrap();
    match store.contact(contact_id) {
        Some(c) => {
            let seqs = store
                .queued_for(&c.master_pub)
                .iter()
                .map(|q| q.seq)
                .collect();
            ((c.name, c.master_pub), seqs)
        }
        None => ((String::new(), [0; 32]), vec![]),
    }
}

async fn dial(shared: &Arc<Shared>, contact_id: &str) -> Option<DialOutcome> {
    if std::env::var("GOSSIPD_TOR_ONLY").is_err() {
        if let Some(conn) = dial_direct(shared, contact_id).await {
            return Some(DialOutcome::Direct(conn));
        }
    }

    if crate::tor::sync_over_tor(shared, contact_id).await.is_ok() {
        tracing::debug!(peer = %contact_id, "synced over tor");
        return Some(DialOutcome::Tor);
    }
    None
}

async fn dial_direct(shared: &Arc<Shared>, contact_id: &str) -> Option<Connection> {
    let (endpoint_id, addrs) = {
        let store = shared.store.lock().unwrap();
        let c = store.contact(contact_id)?;
        (c.endpoint_id, c.addrs)
    };
    let addr = net::endpoint_addr(&endpoint_id, &addrs).ok()?;
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(DIAL_TIMEOUT, shared.endpoint.connect(addr, net::ALPN)).await;
    match result {
        Ok(Ok(conn)) => {
            tracing::debug!(peer = %contact_id, elapsed = ?started.elapsed(), "dial ok");
            Some(conn)
        }
        Ok(Err(e)) => {
            tracing::debug!(peer = %contact_id, elapsed = ?started.elapsed(), "dial error: {e}");
            None
        }
        Err(_) => {
            tracing::debug!(peer = %contact_id, elapsed = ?started.elapsed(), "dial timed out");
            None
        }
    }
}

fn adopt(
    shared: &Arc<Shared>,
    contact_id: &str,
    link: &mut Option<Link>,
    conn: Connection,
    outbound: bool,
) {
    let Some(peer) = peer_info(shared, contact_id) else {
        conn.close(0u32.into(), b"unknown peer");
        return;
    };
    if let Some(existing) = link.as_ref() {
        let my_ep = *shared.endpoint.id().as_bytes();
        let store = shared.store.lock().unwrap();
        let peer_ep = store
            .contact(contact_id)
            .map(|c| c.endpoint_id)
            .unwrap_or([0; 32]);
        drop(store);
        let keep_outbound = my_ep < peer_ep;
        if existing.outbound == keep_outbound {
            conn.close(0u32.into(), b"duplicate connection");
            return;
        }
        existing.conn.close(0u32.into(), b"superseded");
        if let Some(old) = link.take() {
            old.driver.abort();
        }
    }
    let was_connected = shared
        .conns
        .lock()
        .unwrap()
        .insert(contact_id.to_string(), conn.clone())
        .is_some();
    if !was_connected {
        shared.notifier.notify(
            "peer/presence",
            json!({"peer-id": contact_id, "online": true,
                   "path": if outbound { "direct" } else { "direct-inbound" }}),
        );
    }
    let driver = tokio::spawn(drive(shared.clone(), peer, conn.clone()));
    *link = Some(Link {
        conn,
        outbound,
        driver,
    });
}

async fn drive(shared: Arc<Shared>, peer: PeerInfo, conn: Connection) {
    let initial = {
        let shared = shared.clone();
        let peer = peer.clone();
        let conn = conn.clone();
        async move {
            let _ = sync::pull(&shared, &peer, &conn).await;
            let _ = sync::push_queued(&shared, &peer, &conn).await;
        }
    };
    let accept = async {
        while let Ok((send, recv)) = conn.accept_bi().await {
            let shared = shared.clone();
            let peer = peer.clone();
            tokio::spawn(async move {
                sync::serve_stream(&shared, &peer, send, recv).await;
            });
        }
    };
    tokio::join!(initial, accept);
}
