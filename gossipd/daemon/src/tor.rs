use std::sync::Arc;

use arti_client::config::TorClientConfigBuilder;
use arti_client::TorClient;
use ed25519_dalek::SigningKey;
use futures::StreamExt;
use gossipd_core::identity::{TransportCert, ROLE_ONION, ROLE_TOR};
use gossipd_core::torauth::{onion_pubkey, sign_auth, verify_auth};
use safelog::DisplayRedacted;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tor_hsservice::{config::OnionServiceConfigBuilder, handle_rend_requests, HsNickname};

use crate::state::Shared;
use crate::sync::{self, PeerInfo};

const VIRTUAL_PORT: u16 = 9999;
const NICKNAME: &str = "gossip";

type BoxWrite = Box<dyn tokio::io::AsyncWrite + Unpin + Send>;
type BoxRead = BufReader<Box<dyn tokio::io::AsyncRead + Unpin + Send>>;

pub struct TorState {
    client: Arc<TorClient<tor_rtcompat::PreferredRuntime>>,

    pub onion: String,
    pub onion_cert: TransportCert,

    tor_key: SigningKey,
    my_master: [u8; 32],
    tor_cert: TransportCert,
}

#[derive(Serialize, Deserialize)]
struct Challenge {
    nonce: [u8; 32],

    server_master: [u8; 32],
}

#[derive(Serialize, Deserialize)]
struct AuthReply {
    client_master: [u8; 32],
    cert: TransportCert,
    sig: Vec<u8>,
}

pub fn launch(
    shared: Arc<Shared>,
    master_secret: [u8; 32],
    tor_key: [u8; 32],
    tor_cert: TransportCert,
) {
    let my_master = gossipd_core::identity::MasterKey::from_bytes(&master_secret)
        .public()
        .to_bytes();
    tokio::spawn(async move {
        if let Err(e) = launch_inner(&shared, master_secret, tor_key, my_master, tor_cert).await {
            shared.notifier.notify(
                "log",
                serde_json::json!({"level": "error", "message": format!("tor: {e}")}),
            );
            shared.notifier.notify(
                "tor/status",
                serde_json::json!({"state": "failed", "percent": 0}),
            );
        }
    });
}

async fn launch_inner(
    shared: &Arc<Shared>,
    master_secret: [u8; 32],
    tor_key: [u8; 32],
    my_master: [u8; 32],
    tor_cert: TransportCert,
) -> Result<(), String> {
    let tor_dir = shared.data_dir.join("tor");
    let config =
        TorClientConfigBuilder::from_directories(tor_dir.join("state"), tor_dir.join("cache"))
            .build()
            .map_err(|e| format!("config: {e}"))?;

    let client = TorClient::builder()
        .config(config)
        .create_unbootstrapped()
        .map_err(|e| format!("create client: {e}"))?;

    {
        let mut events = client.bootstrap_events();
        let notifier = shared.notifier.clone();
        tokio::spawn(async move {
            while let Some(status) = events.next().await {
                let percent = (status.as_frac() * 100.0).round() as u32;
                let state = if status.ready_for_traffic() {
                    "done"
                } else {
                    "bootstrapping"
                };
                notifier.notify(
                    "tor/status",
                    serde_json::json!({"state": state, "percent": percent}),
                );
            }
        });
    }

    client
        .bootstrap()
        .await
        .map_err(|e| format!("bootstrap: {e}"))?;

    let nickname = HsNickname::new(NICKNAME.to_string()).map_err(|e| format!("nickname: {e}"))?;
    let svc_config = OnionServiceConfigBuilder::default()
        .nickname(nickname)
        .build()
        .map_err(|e| format!("onion config: {e}"))?;
    let (service, rend_requests) = client
        .launch_onion_service(svc_config)
        .map_err(|e| format!("launch onion: {e}"))?
        .ok_or("onion service disabled")?;

    let onion = wait_for_onion(&service).await?;
    let onion_key = onion_pubkey(&onion).ok_or("onion address has no key")?;

    let onion_cert = gossipd_core::identity::MasterKey::from_bytes(&master_secret)
        .certify(ROLE_ONION, onion_key);

    let tor = Arc::new(TorState {
        client,
        onion: onion.clone(),
        onion_cert,
        tor_key: SigningKey::from_bytes(&tor_key),
        my_master,
        tor_cert,
    });
    *shared.tor.lock().unwrap() = Some(tor.clone());
    shared.notifier.notify(
        "log",
        serde_json::json!({"level": "info", "message": format!("tor onion up: {onion}")}),
    );

    let mut streams = std::pin::pin!(handle_rend_requests(rend_requests));
    while let Some(req) = streams.next().await {
        let shared = shared.clone();
        let tor = tor.clone();
        tokio::spawn(async move {
            use tor_cell::relaycell::msg::Connected;
            let Ok(stream) = req.accept(Connected::new_empty()).await else {
                return;
            };
            serve_onion_stream(&shared, &tor, stream.compat()).await;
        });
    }

    drop(service);
    Ok(())
}

async fn wait_for_onion(
    service: &Arc<tor_hsservice::RunningOnionService>,
) -> Result<String, String> {
    for _ in 0..100 {
        if let Some(id) = service.onion_address() {
            return Ok(id.display_unredacted().to_string());
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    Err("onion address never assigned".into())
}

async fn serve_onion_stream<S>(shared: &Arc<Shared>, tor: &TorState, stream: S)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (recv, send) = tokio::io::split(stream);
    let mut send = send;
    let mut recv = BufReader::new(recv);

    let nonce = crate::state::random_nonce();
    let challenge = Challenge {
        nonce,
        server_master: tor.my_master,
    };
    if write_json(&mut send, &challenge).await.is_err() {
        return;
    }
    let Some(reply) = read_json::<_, AuthReply>(&mut recv).await else {
        return;
    };

    if !auth_ok(&tor.my_master, &nonce, &reply) {
        tracing::warn!("tor: rejecting stream with bad client auth");
        return;
    }
    let contact = shared
        .store
        .lock()
        .unwrap()
        .contact_by_master(&reply.client_master);
    let Some(contact) = contact else {
        tracing::warn!("tor: rejecting stream from non-contact");
        return;
    };
    *shared.inbound_seen.lock().unwrap() = true;
    let peer = PeerInfo {
        id: contact.id.clone(),
        name: contact.name,
        master: contact.master_pub,
    };
    shared.notifier.notify(
        "peer/presence",
        serde_json::json!({"peer-id": contact.id, "online": true, "path": "tor"}),
    );
    sync::serve_inner(shared, &peer, &mut send, &mut recv).await;
    let _ = send.shutdown().await;
}

pub async fn sync_over_tor(shared: &Arc<Shared>, contact_id: &str) -> Result<(), ()> {
    let tor = shared.tor.lock().unwrap().clone();
    let tor = tor.ok_or(())?;
    let (peer, onion) = {
        let store = shared.store.lock().unwrap();
        let c = store.contact(contact_id).ok_or(())?;
        let onion = c.onion.clone().ok_or(())?;
        (
            PeerInfo {
                id: c.id,
                name: c.name,
                master: c.master_pub,
            },
            onion,
        )
    };

    let (mut s, mut r) = tor.dial(&onion, &peer.master).await?;
    sync::pull_inner(shared, &peer, &mut s, &mut r).await?;
    let _ = s.shutdown().await;

    if !shared
        .store
        .lock()
        .unwrap()
        .queued_for(&peer.master)
        .is_empty()
    {
        let (mut s, mut r) = tor.dial(&onion, &peer.master).await?;
        sync::push_inner(shared, &peer, &mut s, &mut r).await?;
        let _ = s.shutdown().await;
    }

    shared.notifier.notify(
        "peer/presence",
        serde_json::json!({"peer-id": peer.id, "online": true, "path": "tor"}),
    );
    Ok(())
}

impl TorState {
    async fn dial(&self, onion: &str, server_master: &[u8; 32]) -> Result<(BoxWrite, BoxRead), ()> {
        let host = onion.trim_end_matches(".onion");
        let target = format!("{host}.onion:{VIRTUAL_PORT}");

        let stream = match tokio::time::timeout(
            std::time::Duration::from_secs(180),
            self.client.connect(target.as_str()),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                tracing::debug!("tor dial {onion}: {e}");
                return Err(());
            }
            Err(_) => {
                tracing::debug!("tor dial {onion}: timed out");
                return Err(());
            }
        };
        let (recv, send) = tokio::io::split(stream.compat());
        let mut send: BoxWrite = Box::new(send);
        let mut recv: BoxRead = BufReader::new(Box::new(recv));

        let challenge: Challenge = read_json(&mut recv).await.ok_or(())?;
        if &challenge.server_master != server_master {
            tracing::warn!("tor: onion presented an unexpected master key");
            return Err(());
        }
        let sig = sign_auth(
            &self.tor_key,
            server_master,
            &self.my_master,
            &challenge.nonce,
        );
        let reply = AuthReply {
            client_master: self.my_master,
            cert: self.tor_cert.clone(),
            sig: sig.to_vec(),
        };
        write_json(&mut send, &reply).await.map_err(|_| ())?;
        Ok((send, recv))
    }
}

fn auth_ok(my_master: &[u8; 32], nonce: &[u8; 32], reply: &AuthReply) -> bool {
    let Ok(master) = ed25519_dalek::VerifyingKey::from_bytes(&reply.client_master) else {
        return false;
    };
    reply.cert.verify(&master, ROLE_TOR)
        && verify_auth(
            &reply.cert.subkey,
            my_master,
            &reply.client_master,
            nonce,
            &reply.sig,
        )
}

async fn write_json<W: AsyncWriteExt + Unpin, T: Serialize>(
    w: &mut W,
    v: &T,
) -> std::io::Result<()> {
    let mut line = serde_json::to_vec(v).expect("auth encodes");
    line.push(b'\n');
    w.write_all(&line).await
}

async fn read_json<R: AsyncBufReadExt + Unpin, T: for<'de> Deserialize<'de>>(
    r: &mut R,
) -> Option<T> {
    let mut line = String::new();
    match r.read_line(&mut line).await {
        Ok(0) | Err(_) => None,
        Ok(_) => serde_json::from_str(&line).ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gossipd_core::identity::MasterKey;

    fn good_reply(
        client_secret: [u8; 32],
        tor_secret: [u8; 32],
        my_master: &[u8; 32],
        nonce: &[u8; 32],
    ) -> (AuthReply, [u8; 32]) {
        let master = MasterKey::from_bytes(&client_secret);
        let client_master = master.public().to_bytes();
        let tor_key = SigningKey::from_bytes(&tor_secret);
        let cert = master.certify(ROLE_TOR, tor_key.verifying_key().to_bytes());
        let sig = sign_auth(&tor_key, my_master, &client_master, nonce);
        (
            AuthReply {
                client_master,
                cert,
                sig: sig.to_vec(),
            },
            client_master,
        )
    }

    #[test]
    fn accepts_valid_and_rejects_tampering() {
        let my_master = [1u8; 32];
        let nonce = [2u8; 32];
        let (reply, _) = good_reply([3; 32], [4; 32], &my_master, &nonce);
        assert!(auth_ok(&my_master, &nonce, &reply));

        assert!(!auth_ok(&[9; 32], &nonce, &reply));
        assert!(!auth_ok(&my_master, &[9; 32], &reply));

        let mut forged = good_reply([3; 32], [4; 32], &my_master, &nonce).0;
        forged.cert = MasterKey::from_bytes(&[7; 32]).certify(ROLE_TOR, [4; 32]);
        assert!(!auth_ok(&my_master, &nonce, &forged));

        let mut wrong_role = good_reply([3; 32], [4; 32], &my_master, &nonce).0;
        wrong_role.cert =
            MasterKey::from_bytes(&[3; 32]).certify(gossipd_core::identity::ROLE_IROH, [4; 32]);
        assert!(!auth_ok(&my_master, &nonce, &wrong_role));
    }

    #[test]
    fn challenge_and_reply_survive_json_lines() {
        let ch = Challenge {
            nonce: [5; 32],
            server_master: [6; 32],
        };
        let line = serde_json::to_string(&ch).unwrap();
        let back: Challenge = serde_json::from_str(&line).unwrap();
        assert_eq!(back.nonce, ch.nonce);
        assert_eq!(back.server_master, ch.server_master);

        let (reply, _) = good_reply([3; 32], [4; 32], &[1; 32], &[2; 32]);
        let line = serde_json::to_string(&reply).unwrap();
        let back: AuthReply = serde_json::from_str(&line).unwrap();
        assert!(auth_ok(&[1; 32], &[2; 32], &back));
    }
}
