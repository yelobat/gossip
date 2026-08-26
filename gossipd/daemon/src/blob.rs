use std::sync::Arc;

use futures_lite::StreamExt;
use iroh_blobs::api::remote::PushProgressItem;
use iroh_blobs::protocol::{ChunkRangesSeq, PushRequest};
use iroh_blobs::Hash;
use serde_json::json;

use crate::state::Shared;
use crate::sync::PeerInfo;
use crate::{net, store::Contact};

pub async fn send(
    shared: Arc<Shared>,
    contact: Contact,
    path: std::path::PathBuf,
    transfer_id: String,
) {
    let fail = |msg: String| {
        shared.notifier.notify(
            "log",
            json!({"level": "error", "message": format!("blob {transfer_id}: {msg}")}),
        );
    };
    let size = match std::fs::metadata(&path) {
        Ok(m) => m.len().max(1),
        Err(e) => return fail(format!("cannot read {}: {e}", path.display())),
    };
    let tag = match shared.blobs.blobs().add_path(&path).await {
        Ok(tag) => tag,
        Err(e) => return fail(format!("import failed: {e}")),
    };
    let hash: Hash = tag.hash;
    let addr = match net::endpoint_addr(&contact.endpoint_id, &contact.addrs) {
        Ok(a) => a,
        Err(e) => return fail(e),
    };
    let conn = match shared
        .endpoint
        .connect(addr, iroh_blobs::protocol::ALPN)
        .await
    {
        Ok(c) => c,
        Err(e) => return fail(format!("cannot reach {}: {e}", contact.name)),
    };

    let conn_keepalive = conn.clone();
    let mut progress = shared
        .blobs
        .remote()
        .execute_push(conn, PushRequest::new(hash, ChunkRangesSeq::root()))
        .stream();
    let mut last_percent = 0u64;
    while let Some(item) = progress.next().await {
        match item {
            PushProgressItem::Progress(bytes) => {
                let percent = (bytes * 100 / size).min(99);
                if percent > last_percent {
                    last_percent = percent;
                    shared.notifier.notify(
                        "transfer/progress",
                        json!({"transfer-id": transfer_id, "percent": percent}),
                    );
                }
            }
            PushProgressItem::Done(_) => {
                shared.notifier.notify(
                    "transfer/progress",
                    json!({"transfer-id": transfer_id, "percent": 100}),
                );

                let name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| hash.to_string());
                let peer = PeerInfo {
                    id: contact.id.clone(),
                    name: contact.name.clone(),
                    master: contact.master_pub,
                };
                crate::sync::announce_blob(&shared, &peer, &hash.to_string(), &name, size).await;
                conn_keepalive.close(0u32.into(), b"done");
                return;
            }
            PushProgressItem::Error(e) => return fail(format!("push failed: {e}")),
        }
    }
    fail("push ended without completing".into());
}

pub async fn received(shared: Arc<Shared>, peer: PeerInfo, hash: String, name: String) {
    let Ok(hash) = hash.parse::<Hash>() else {
        return;
    };

    let name = std::path::Path::new(&name)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| hash.to_string());
    let dir = shared.data_dir.join("downloads");
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    let target = dir.join(&name);

    for i in 0..100 {
        match shared.blobs.remote().local(hash).await {
            Ok(info) if info.is_complete() => {
                tracing::debug!(%hash, tries = i, "blob complete locally");
                break;
            }
            Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(300)).await,
            Err(e) => {
                tracing::debug!(%hash, "local check failed: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
        }
    }
    match shared.blobs.blobs().export(hash, &target).await {
        Ok(_) => shared.notifier.notify(
            "log",
            json!({"level": "info",
                   "message": format!("received file from {}: {}", peer.name, target.display())}),
        ),
        Err(e) => shared.notifier.notify(
            "log",
            json!({"level": "warn",
                   "message": format!("failed to export blob from {}: {e}", peer.name)}),
        ),
    }
}
