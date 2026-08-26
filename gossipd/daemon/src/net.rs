use std::net::SocketAddr;
use std::sync::Arc;

use futures_lite::StreamExt;
use iroh::endpoint::{presets, RelayMode};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMap, SecretKey, TransportAddr};
use iroh_mainline_address_lookup::DhtAddressLookup;
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};

use crate::peer::ensure_peer;
use crate::rpc::TransportCfg;
use crate::state::{PeerCmd, Shared};

pub const ALPN: &[u8] = b"gossip/0";

pub async fn build_endpoint(
    iroh_secret: &[u8; 32],
    transport: &TransportCfg,
) -> Result<(Endpoint, Option<MdnsAddressLookup>), String> {
    let quic = iroh::endpoint::QuicTransportConfig::builder()
        .keep_alive_interval(std::time::Duration::from_secs(3))
        .max_idle_timeout(Some(
            std::time::Duration::from_secs(10)
                .try_into()
                .expect("10s fits an idle timeout"),
        ))
        .build();
    let mut builder = Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::from_bytes(iroh_secret))
        .alpns(vec![ALPN.to_vec(), iroh_blobs::protocol::ALPN.to_vec()])
        .transport_config(quic)
        .relay_mode(relay_mode(transport));

    if let Ok(bind) = std::env::var("GOSSIPD_BIND") {
        let addr: SocketAddr = bind
            .parse()
            .map_err(|e| format!("GOSSIPD_BIND {bind:?}: {e}"))?;
        builder = builder
            .bind_addr(addr)
            .map_err(|e| format!("GOSSIPD_BIND {bind:?}: {e}"))?;
    }

    let discovery = std::env::var("GOSSIPD_NO_DISCOVERY").is_err();
    if discovery {
        builder = builder.address_lookup(
            DhtAddressLookup::builder().addr_filter(iroh::address_lookup::AddrFilter::ip_only()),
        );
    }

    let endpoint = builder
        .bind()
        .await
        .map_err(|e| format!("bind failed: {e}"))?;

    let mdns = if discovery {
        let mdns = MdnsAddressLookup::builder()
            .build(endpoint.id())
            .map_err(|e| format!("mdns: {e}"))?;
        endpoint
            .address_lookup()
            .map_err(|e| format!("address lookup: {e}"))?
            .add(mdns.clone());
        Some(mdns)
    } else {
        None
    };
    Ok((endpoint, mdns))
}

pub fn spawn_accept_loop(shared: Arc<Shared>) {
    tokio::spawn(async move {
        while let Some(incoming) = shared.endpoint.accept().await {
            let shared = shared.clone();
            tokio::spawn(async move {
                let Ok(conn) = incoming.await else { return };
                let remote = *conn.remote_id().as_bytes();
                let contact = {
                    let store = shared.store.lock().unwrap();
                    store
                        .contacts()
                        .into_iter()
                        .find(|c| c.endpoint_id == remote)
                };
                match contact {
                    Some(c) => {
                        *shared.inbound_seen.lock().unwrap() = true;
                        if conn.alpn() == iroh_blobs::protocol::ALPN {
                            use iroh::protocol::ProtocolHandler;
                            shared.blobs_proto.accept(conn).await.ok();
                        } else {
                            let tx = ensure_peer(&shared, &c.id);
                            tx.send(PeerCmd::Inbound(conn)).ok();
                        }
                    }
                    None => {
                        tracing::warn!("closing connection from unknown endpoint");
                        conn.close(0u32.into(), b"unknown peer");
                    }
                }
            });
        }
    });
}

pub fn spawn_discovery_nudges(shared: Arc<Shared>, mdns: MdnsAddressLookup) {
    tokio::spawn(async move {
        let mut events = mdns.subscribe().await;
        while let Some(event) = events.next().await {
            if let DiscoveryEvent::Discovered { endpoint_info, .. } = event {
                let seen = *endpoint_info.endpoint_id.as_bytes();
                let contact = {
                    let store = shared.store.lock().unwrap();
                    store.contacts().into_iter().find(|c| c.endpoint_id == seen)
                };
                if let Some(c) = contact {
                    tracing::debug!(peer = %c.id, "discovery evidence, nudging");
                    ensure_peer(&shared, &c.id).send(PeerCmd::Nudge).ok();
                }
            }
        }
    });
}

fn relay_mode(transport: &TransportCfg) -> RelayMode {
    if transport.allow_relays && !transport.relay_urls.is_empty() {
        match RelayMap::try_from_iter(transport.relay_urls.iter().map(String::as_str)) {
            Ok(map) => return RelayMode::Custom(map),
            Err(e) => tracing::warn!("bad relay-urls ({e}), relays stay disabled"),
        }
    }
    RelayMode::Disabled
}

pub fn direct_addrs(endpoint: &Endpoint, advertised: &[String]) -> Vec<String> {
    let mut addrs: Vec<String> = endpoint.addr().ip_addrs().map(|a| a.to_string()).collect();
    for a in advertised {
        if !addrs.contains(a) {
            addrs.push(a.clone());
        }
    }
    addrs
}

pub fn endpoint_addr(endpoint_id: &[u8; 32], addrs: &[String]) -> Result<EndpointAddr, String> {
    let id = EndpointId::from_bytes(endpoint_id).map_err(|e| format!("bad endpoint id: {e}"))?;
    let socket_addrs = addrs
        .iter()
        .filter_map(|a| a.parse::<SocketAddr>().ok())
        .map(TransportAddr::Ip);
    Ok(EndpointAddr::from_parts(id, socket_addrs))
}
