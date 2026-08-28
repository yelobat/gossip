use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ProfileArchive {
    pub version: u32,
    pub identity: Vec<u8>,
    pub iroh: Vec<u8>,
    pub tor: Vec<u8>,
    pub db: Vec<u8>,
}

pub const VERSION: u32 = 1;

impl ProfileArchive {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("archive encodes")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let archive: ProfileArchive =
            postcard::from_bytes(bytes).map_err(|e| format!("not a gossip profile: {e}"))?;
        if archive.version != VERSION {
            return Err(format!(
                "unsupported profile version {} (this build reads {VERSION})",
                archive.version
            ));
        }
        Ok(archive)
    }

    pub fn unpack_into_dir(&self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dir)?;
        write_private(&dir.join("identity.key"), &self.identity)?;
        write_private(&dir.join("iroh.key"), &self.iroh)?;
        write_private(&dir.join("tor.key"), &self.tor)?;
        std::fs::write(dir.join("gossip.db"), &self.db)?;
        for sidecar in ["gossip.db-wal", "gossip.db-shm"] {
            let _ = std::fs::remove_file(dir.join(sidecar));
        }
        Ok(())
    }
}

fn write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    std::fs::write(path, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_and_rejects_bad_version() {
        let a = ProfileArchive {
            version: VERSION,
            identity: vec![1; 32],
            iroh: vec![2; 32],
            tor: vec![3; 32],
            db: b"sqlite".to_vec(),
        };
        let back = ProfileArchive::decode(&a.encode()).unwrap();
        assert_eq!(back.identity, a.identity);
        assert_eq!(back.db, a.db);

        let bad = ProfileArchive { version: 999, ..a };
        assert!(ProfileArchive::decode(&bad.encode()).is_err());
        assert!(ProfileArchive::decode(b"garbage").is_err());
    }
}
