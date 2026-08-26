use arti_client::config::TorClientConfigBuilder;
use arti_client::TorClient;
use futures::StreamExt;
use safelog::DisplayRedacted;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tor_hsservice::{config::OnionServiceConfigBuilder, handle_rend_requests, HsNickname};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let base = std::env::temp_dir().join(format!("tor-loopback-{}", std::process::id()));

    let scfg = TorClientConfigBuilder::from_directories(base.join("s/state"), base.join("s/cache"))
        .build()
        .unwrap();
    let server = TorClient::builder()
        .config(scfg)
        .create_bootstrapped()
        .await
        .unwrap();
    let svc = OnionServiceConfigBuilder::default()
        .nickname(HsNickname::new("loop".into()).unwrap())
        .build()
        .unwrap();
    let (service, rends) = server.launch_onion_service(svc).unwrap().unwrap();
    let onion = loop {
        if let Some(id) = service.onion_address() {
            break id.display_unredacted().to_string();
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    };
    eprintln!("service onion: {onion}");

    tokio::spawn(async move {
        let mut streams = std::pin::pin!(handle_rend_requests(rends));
        while let Some(req) = streams.next().await {
            tokio::spawn(async move {
                use tor_cell::relaycell::msg::Connected;
                if let Ok(s) = req.accept(Connected::new_empty()).await {
                    let mut s = s.compat();
                    let mut b = [0u8; 1];
                    if s.read_exact(&mut b).await.is_ok() {
                        let _ = s.write_all(&b).await;
                        let _ = s.flush().await;
                    }
                }
            });
        }
    });

    let ccfg = TorClientConfigBuilder::from_directories(base.join("c/state"), base.join("c/cache"))
        .build()
        .unwrap();
    let client = TorClient::builder()
        .config(ccfg)
        .create_bootstrapped()
        .await
        .unwrap();

    eprintln!("connecting to {onion}:9999 ...");
    let start = std::time::Instant::now();
    let target = format!("{onion}:9999");
    match tokio::time::timeout(
        std::time::Duration::from_secs(180),
        client.connect(target.as_str()),
    )
    .await
    {
        Ok(Ok(stream)) => {
            let mut s = stream.compat();
            s.write_all(b"x").await.unwrap();
            s.flush().await.unwrap();
            let mut b = [0u8; 1];
            s.read_exact(&mut b).await.unwrap();
            println!(
                "LOOPBACK OK in {:?}, echoed {:?}",
                start.elapsed(),
                b[0] as char
            );
        }
        Ok(Err(e)) => println!("CONNECT ERROR after {:?}: {e}", start.elapsed()),
        Err(_) => println!("CONNECT TIMED OUT after {:?}", start.elapsed()),
    }
    drop(service);
}
