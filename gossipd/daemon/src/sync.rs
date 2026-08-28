use std::sync::Arc;

use gossipd_core::log::LogEntry;
use iroh::endpoint::Connection;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::state::{msg_id, now_ts, FileDecision, Shared};

fn hex_id() -> String {
    let n = crate::state::random_nonce();
    format!("{:02x}{:02x}{:02x}{:02x}", n[0], n[1], n[2], n[3])
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "lowercase")]
enum WireMsg {
    Pull {
        frontier: u64,
    },
    Push,
    E {
        entry: LogEntry,
    },
    End,
    Ack {
        upto: u64,
    },

    Blob {
        hash: String,
        name: String,
        size: u64,
    },
}

#[derive(Clone)]
pub struct PeerInfo {
    pub id: String,
    pub name: String,
    pub master: [u8; 32],
}

async fn write_msg<W: AsyncWriteExt + Unpin>(w: &mut W, msg: &WireMsg) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(msg).expect("wire msg encodes");
    line.push(b'\n');
    w.write_all(&line).await
}

async fn read_msg<R: AsyncBufReadExt + Unpin>(r: &mut R) -> Option<WireMsg> {
    let mut line = String::new();
    match r.read_line(&mut line).await {
        Ok(0) | Err(_) => None,
        Ok(_) => {
            let parsed = serde_json::from_str(&line).ok();
            if parsed.is_none() {
                tracing::warn!("unparseable wire line: {line:?}");
            }
            tracing::trace!("wire <- {line:?}");
            parsed
        }
    }
}

pub async fn serve_stream(
    shared: &Arc<Shared>,
    peer: &PeerInfo,
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
) {
    let mut send = send;
    let mut recv = BufReader::new(recv);
    serve_inner(shared, peer, &mut send, &mut recv).await;
    send.finish().ok();

    let _ = send.stopped().await;
}

pub async fn serve_inner<W, R>(shared: &Arc<Shared>, peer: &PeerInfo, send: &mut W, recv: &mut R)
where
    W: AsyncWriteExt + Unpin,
    R: AsyncBufReadExt + Unpin,
{
    match read_msg(recv).await {
        Some(WireMsg::Pull { frontier }) => {
            let entries = {
                let store = shared.store.lock().unwrap();
                store.entries_after(&shared.my_master, &peer.master, frontier)
            };
            for entry in entries {
                if write_msg(send, &WireMsg::E { entry }).await.is_err() {
                    return;
                }
            }
            if write_msg(send, &WireMsg::End).await.is_err() {
                return;
            }
            if let Some(WireMsg::Ack { upto }) = read_msg(recv).await {
                handle_ack(shared, peer, upto);
            }
        }
        Some(WireMsg::Push) => {
            let upto = receive_entries(shared, peer, recv).await;
            write_msg(send, &WireMsg::Ack { upto }).await.ok();
        }
        Some(WireMsg::Blob { hash, name, size }) => {
            match shared.file_decision(&peer.id) {
                FileDecision::Accept => {
                    crate::blob::received(shared.clone(), peer.clone(), hash, name).await
                }
                FileDecision::Reject => shared.notifier.notify(
                    "file/declined",
                    json!({"from": peer.id, "from-name": peer.name, "name": name}),
                ),
                FileDecision::Ask => {
                    let id = format!("f{}", hex_id());
                    shared.pending_files.lock().unwrap().insert(
                        id.clone(),
                        crate::state::PendingFile {
                            hash,
                            name: name.clone(),
                            peer: peer.clone(),
                        },
                    );
                    shared.notifier.notify(
                        "file/incoming",
                        json!({"id": id, "from": peer.id, "from-name": peer.name,
                               "name": name, "size": size}),
                    );
                }
            }
            write_msg(send, &WireMsg::End).await.ok();
        }
        _ => {}
    }
}

pub async fn announce_blob(
    shared: &Arc<Shared>,
    peer: &PeerInfo,
    hash: &str,
    name: &str,
    size: u64,
) {
    tracing::debug!(peer = %peer.id, hash, "announcing blob");

    for _ in 0..20 {
        let Some(conn) = shared.live_connection(&peer.id) else {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        };
        let Ok((mut send, recv)) = conn.open_bi().await else {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        };
        let msg = WireMsg::Blob {
            hash: hash.to_string(),
            name: name.to_string(),
            size,
        };
        write_msg(&mut send, &msg).await.ok();
        send.finish().ok();
        let mut recv = BufReader::new(recv);
        let confirmed =
            tokio::time::timeout(std::time::Duration::from_secs(120), read_msg(&mut recv)).await;
        tracing::debug!(peer = %peer.id, ok = confirmed.is_ok(), "announce confirmed");
        return;
    }
    tracing::warn!(peer = %peer.id, "no live link to announce blob, peer will export on next sync");
}

pub async fn pull(shared: &Arc<Shared>, peer: &PeerInfo, conn: &Connection) -> Result<(), ()> {
    tracing::debug!(peer = %peer.id, "pull: opening stream");
    let (mut send, recv) = conn.open_bi().await.map_err(|e| {
        tracing::debug!(peer = %peer.id, "pull: open_bi failed: {e}");
    })?;
    let mut recv = BufReader::new(recv);
    pull_inner(shared, peer, &mut send, &mut recv).await?;
    send.finish().ok();
    let _ = send.stopped().await;
    Ok(())
}

pub async fn pull_inner<W, R>(
    shared: &Arc<Shared>,
    peer: &PeerInfo,
    send: &mut W,
    recv: &mut R,
) -> Result<(), ()>
where
    W: AsyncWriteExt + Unpin,
    R: AsyncBufReadExt + Unpin,
{
    let frontier = {
        let store = shared.store.lock().unwrap();
        store.frontier(&peer.master, &shared.my_master)
    };
    write_msg(send, &WireMsg::Pull { frontier })
        .await
        .map_err(|_| ())?;
    let upto = receive_entries(shared, peer, recv).await;
    write_msg(send, &WireMsg::Ack { upto })
        .await
        .map_err(|_| ())?;
    Ok(())
}

pub async fn push_queued(
    shared: &Arc<Shared>,
    peer: &PeerInfo,
    conn: &Connection,
) -> Result<(), ()> {
    if shared
        .store
        .lock()
        .unwrap()
        .queued_for(&peer.master)
        .is_empty()
    {
        return Ok(());
    }
    let (mut send, recv) = conn.open_bi().await.map_err(|e| {
        tracing::debug!(peer = %peer.id, "push: open_bi failed: {e}");
    })?;
    let mut recv = BufReader::new(recv);
    push_inner(shared, peer, &mut send, &mut recv).await?;
    send.finish().ok();
    Ok(())
}

pub async fn push_inner<W, R>(
    shared: &Arc<Shared>,
    peer: &PeerInfo,
    send: &mut W,
    recv: &mut R,
) -> Result<bool, ()>
where
    W: AsyncWriteExt + Unpin,
    R: AsyncBufReadExt + Unpin,
{
    let entries = {
        let store = shared.store.lock().unwrap();
        let queued = store.queued_for(&peer.master);
        match queued.iter().map(|q| q.seq).min() {
            Some(min) => store.entries_after(&shared.my_master, &peer.master, min - 1),
            None => return Ok(false),
        }
    };
    tracing::debug!(peer = %peer.id, n = entries.len(), "push: sending queued entries");
    write_msg(send, &WireMsg::Push).await.map_err(|_| ())?;
    for entry in entries {
        write_msg(send, &WireMsg::E { entry })
            .await
            .map_err(|_| ())?;
    }
    write_msg(send, &WireMsg::End).await.map_err(|_| ())?;
    match read_msg(recv).await {
        Some(WireMsg::Ack { upto }) => handle_ack(shared, peer, upto),
        _ => tracing::debug!(peer = %peer.id, "push: no ack"),
    }
    Ok(true)
}

async fn receive_entries<R: AsyncBufReadExt + Unpin>(
    shared: &Arc<Shared>,
    peer: &PeerInfo,
    recv: &mut R,
) -> u64 {
    while let Some(msg) = read_msg(recv).await {
        match msg {
            WireMsg::E { entry } => {
                if entry.author != peer.master
                    || entry.recipient != shared.my_master
                    || !entry.verify()
                {
                    tracing::warn!(
                        peer = %peer.id,
                        seq = entry.seq,
                        author_ok = entry.author == peer.master,
                        recipient_ok = entry.recipient == shared.my_master,
                        sig_ok = entry.verify(),
                        "dropping invalid entry"
                    );
                    tracing::warn!(
                        "bad entry: {}",
                        serde_json::to_string(&entry).unwrap_or_default()
                    );
                    continue;
                }
                let fresh = shared.store.lock().unwrap().append(&entry);
                if fresh {
                    shared.notifier.notify(
                        "msg/received",
                        json!({
                            "id": msg_id(entry.seq),
                            "from": peer.id,
                            "from-name": peer.name,
                            "kind": entry.kind,
                            "body": entry.body,
                            "ts": entry.ts,
                        }),
                    );
                }
            }
            WireMsg::End => break,
            _ => break,
        }
    }
    let store = shared.store.lock().unwrap();
    store.frontier(&peer.master, &shared.my_master)
}

fn handle_ack(shared: &Arc<Shared>, peer: &PeerInfo, upto: u64) {
    let flushed = {
        let store = shared.store.lock().unwrap();
        store.dequeue_up_to(&peer.master, upto)
    };
    for seq in flushed {
        shared.notifier.notify(
            "msg/delivered",
            json!({"msg-id": msg_id(seq), "to": peer.id, "ts": now_ts()}),
        );
    }
}
