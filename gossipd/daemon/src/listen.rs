use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use gossipd_core::frame::{auth_frame_matches, FrameDecoder, FrameError};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::state::random_nonce;
use crate::{ClientHandle, Notifier, Requests};

fn drain_frames<F: FnMut(Value) -> bool>(decoder: &mut FrameDecoder, mut on_request: F) -> bool {
    loop {
        match decoder.next_frame() {
            Ok(Some(body)) => {
                if let Ok(req) = serde_json::from_slice::<Value>(&body) {
                    if !on_request(req) {
                        return false;
                    }
                }
            }
            Ok(None) => return true,
            Err(e @ (FrameError::MissingContentLength | FrameError::BadContentLength)) => {
                tracing::warn!("framing error: {e}, closing connection");
                return false;
            }
        }
    }
}

fn make_handle(notifier: &Notifier) -> (ClientHandle, mpsc::UnboundedReceiver<Arc<Vec<u8>>>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = ClientHandle { tx };
    notifier.register(handle.clone());
    (handle, rx)
}

pub fn serve_stdio(requests: Requests, notifier: &Notifier) {
    let (handle, mut rx) = make_handle(notifier);

    tokio::task::spawn_blocking(move || {
        let mut out = std::io::stdout();
        while let Some(bytes) = rx.blocking_recv() {
            if out.write_all(&bytes).and_then(|_| out.flush()).is_err() {
                break;
            }
        }
    });

    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut decoder = FrameDecoder::new();
        let mut chunk = [0u8; 8192];
        loop {
            let n = match stdin.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            };
            decoder.feed(&chunk[..n]);
            let alive = drain_frames(&mut decoder, |req| {
                requests.blocking_send((req, handle.clone())).is_ok()
            });
            if !alive {
                return;
            }
        }
    });
}

pub async fn serve_control(control_file: PathBuf, requests: Requests, notifier: Notifier) {
    if let Some(parent) = control_file.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return tracing::error!("cannot create {}: {e}", parent.display());
        }
    }
    let lock = match acquire_lock(&control_file.with_extension("lock")) {
        Some(l) => l,
        None => {
            tracing::info!("another gossipd already owns this profile, exiting");
            std::process::exit(0);
        }
    };
    let listener = match TcpListener::bind(("127.0.0.1", 0)).await {
        Ok(l) => l,
        Err(e) => return tracing::error!("cannot bind control port: {e}"),
    };
    let port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
    let token = Arc::new(hex(&random_nonce()));
    if let Err(e) = std::fs::write(&control_file, format!("{port}\n{token}\n")) {
        return tracing::error!("cannot write {}: {e}", control_file.display());
    }
    restrict(&control_file);
    tracing::info!(port, "listening for clients");

    tokio::spawn(async move {
        let _lock = lock;
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    stream.set_nodelay(true).ok();
                    tokio::spawn(serve_conn(
                        stream,
                        requests.clone(),
                        notifier.clone(),
                        token.clone(),
                    ));
                }
                Err(e) => tracing::warn!("accept failed: {e}"),
            }
        }
    });
}

async fn serve_conn(stream: TcpStream, requests: Requests, notifier: Notifier, token: Arc<String>) {
    let (mut read, mut write) = stream.into_split();
    let mut decoder = FrameDecoder::new();
    if !authenticate(&mut read, &mut decoder, &token).await {
        return;
    }

    let (handle, mut rx) = make_handle(&notifier);
    tokio::spawn(async move {
        while let Some(bytes) = rx.recv().await {
            if write.write_all(&bytes).await.is_err() {
                break;
            }
        }
    });

    let mut chunk = [0u8; 8192];
    loop {
        let mut pending = Vec::new();
        let alive = drain_frames(&mut decoder, |req| {
            pending.push(req);
            true
        });
        for req in pending {
            if requests.send((req, handle.clone())).await.is_err() {
                return;
            }
        }
        if !alive {
            return;
        }
        let n = match read.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        decoder.feed(&chunk[..n]);
    }
}

async fn authenticate<R>(read: &mut R, decoder: &mut FrameDecoder, token: &str) -> bool
where
    R: AsyncReadExt + Unpin,
{
    let mut chunk = [0u8; 8192];
    loop {
        match decoder.next_frame() {
            Ok(Some(body)) => return auth_frame_matches(&body, token),
            Ok(None) => {}
            Err(_) => return false,
        }
        let n = match read.read(&mut chunk).await {
            Ok(0) | Err(_) => return false,
            Ok(n) => n,
        };
        decoder.feed(&chunk[..n]);
    }
}

fn acquire_lock(path: &Path) -> Option<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .ok()?;
    file.try_lock().ok().map(|()| file)
}

fn hex(bytes: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(bytes)
}

#[cfg(unix)]
fn restrict(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).ok();
}

#[cfg(not(unix))]
fn restrict(_path: &Path) {}
