use futures_lite::StreamExt;
use iroh::endpoint::{presets, RelayMode};
use iroh::{Endpoint, EndpointAddr, SecretKey};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};

const ALPN: &[u8] = b"iroh-tour/0";

async fn make_endpoint(name: &str) -> (Endpoint, MdnsAddressLookup) {
    let secret = SecretKey::generate();

    let endpoint = Endpoint::builder(presets::Minimal)
        .secret_key(secret)
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .expect("bind");

    let mdns = MdnsAddressLookup::builder()
        .build(endpoint.id())
        .expect("mdns");
    endpoint.address_lookup().expect("lookup").add(mdns.clone());

    println!(
        "{name}: EndpointId {}, bound on {:?}",
        endpoint.id().fmt_short(),
        endpoint.addr().ip_addrs().collect::<Vec<_>>(),
    );
    (endpoint, mdns)
}

#[tokio::main]
async fn main() {
    let (alice, alice_mdns) = make_endpoint("alice").await;

    let mut events = alice_mdns.subscribe().await;

    let (bob, _bob_mdns) = make_endpoint("bob").await;
    let bob_id = bob.id();

    let server = tokio::spawn(async move {
        let conn = bob.accept().await.expect("incoming").await.expect("conn");
        println!(
            "bob: inbound connection from {} (authenticated by TLS, alpn {:?})",
            conn.remote_id().fmt_short(),
            String::from_utf8_lossy(conn.alpn()),
        );
        let (mut tx, mut rx) = conn.accept_bi().await.expect("stream");
        let question = rx.read_to_end(64).await.expect("read");
        assert_eq!(question, b"how do you find peers with no server?");
        tx.write_all(b"multicast: everyone on the LAN hears the question")
            .await
            .expect("write");
        tx.finish().expect("finish");
        conn.closed().await;
    });

    let heard = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while let Some(event) = events.next().await {
            if let DiscoveryEvent::Discovered { endpoint_info, .. } = event {
                if endpoint_info.endpoint_id == bob_id {
                    println!(
                        "alice: mDNS heard bob's announcement, {} is at {:?}",
                        endpoint_info.endpoint_id.fmt_short(),
                        endpoint_info.ip_addrs().collect::<Vec<_>>(),
                    );
                    return;
                }
            }
        }
    })
    .await;
    if heard.is_err() {
        println!("alice: no Discovered event surfaced (cache may fill silently), dialing anyway");
    }

    println!(
        "alice: dialing {} with no addresses at all...",
        bob_id.fmt_short()
    );
    let conn = alice
        .connect(EndpointAddr::new(bob_id), ALPN)
        .await
        .expect("mDNS discovery + dial (is multicast available on this network?)");
    println!("alice: connected, QUIC over UDP, end-to-end encrypted, no server involved");

    let (mut tx, mut rx) = conn.open_bi().await.expect("open stream");
    tx.write_all(b"how do you find peers with no server?")
        .await
        .expect("write");
    tx.finish().expect("finish");
    let answer = rx.read_to_end(64).await.expect("read");
    println!("alice: bob answers: {:?}", String::from_utf8_lossy(&answer));
    conn.close(0u32.into(), b"tour over");

    server.await.expect("server");
    println!("IROH TOUR OK");
}
