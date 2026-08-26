use std::io;
use std::path::Path;

use gossipd_core::identity::{MasterKey, TransportCert, ROLE_IROH, ROLE_TOR};

pub struct Keys {
    pub master: MasterKey,
    pub iroh_secret: [u8; 32],

    pub cert: TransportCert,

    pub tor_secret: [u8; 32],
    pub tor_cert: TransportCert,
}

impl Keys {
    pub fn load_or_create(dir: &Path) -> io::Result<Self> {
        let master = MasterKey::from_bytes(&load_or_create_key(&dir.join("identity.key"))?);
        let iroh_secret = load_or_create_key(&dir.join("iroh.key"))?;

        let iroh_public = ed25519_dalek::SigningKey::from_bytes(&iroh_secret)
            .verifying_key()
            .to_bytes();
        let cert = master.certify(ROLE_IROH, iroh_public);
        let tor_secret = load_or_create_key(&dir.join("tor.key"))?;
        let tor_public = ed25519_dalek::SigningKey::from_bytes(&tor_secret)
            .verifying_key()
            .to_bytes();
        let tor_cert = master.certify(ROLE_TOR, tor_public);
        Ok(Self {
            master,
            iroh_secret,
            cert,
            tor_secret,
            tor_cert,
        })
    }
}

fn load_or_create_key(path: &Path) -> io::Result<[u8; 32]> {
    match std::fs::read(path) {
        Ok(bytes) => bytes
            .as_slice()
            .try_into()
            .map_err(|_| io::Error::other(format!("{}: not a 32-byte key", path.display()))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let mut key = [0u8; 32];
            getrandom::fill(&mut key).map_err(|e| io::Error::other(e.to_string()))?;
            write_private(path, &key)?;
            Ok(key)
        }
        Err(e) => Err(e),
    }
}

#[cfg(unix)]
fn write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(bytes)
}

#[cfg(not(unix))]
fn write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    std::fs::write(path, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gossipd_core::identity::ROLE_IROH;

    #[test]
    fn persists_and_reloads_same_identity() {
        let dir = std::env::temp_dir().join(format!("gossipd-keys-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let a = Keys::load_or_create(&dir).unwrap();
        let b = Keys::load_or_create(&dir).unwrap();
        assert_eq!(a.master.to_bytes(), b.master.to_bytes());
        assert_eq!(a.iroh_secret, b.iroh_secret);
        assert!(a.cert.verify(&a.master.public(), ROLE_IROH));
        assert_ne!(
            a.cert.subkey,
            a.master.public().to_bytes(),
            "endpoint key must not be the identity key"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
