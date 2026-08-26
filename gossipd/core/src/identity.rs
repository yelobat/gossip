use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

pub const ROLE_IROH: &str = "gossip-transport:iroh";

pub const ROLE_ONION: &str = "gossip-transport:onion";

pub const ROLE_TOR: &str = "gossip-transport:tor";
const CERT_DOMAIN: &[u8] = b"gossip-cert:v1";

pub struct MasterKey(SigningKey);

impl MasterKey {
    pub fn from_bytes(bytes: &[u8; 32]) -> Self {
        Self(SigningKey::from_bytes(bytes))
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        self.0.to_bytes()
    }

    pub fn public(&self) -> VerifyingKey {
        self.0.verifying_key()
    }

    pub fn certify(&self, role: &str, subkey: [u8; 32]) -> TransportCert {
        let sig = self.0.sign(&cert_message(role, &subkey));
        TransportCert {
            role: role.to_string(),
            subkey,
            sig: sig.to_bytes().to_vec(),
        }
    }

    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        self.0.sign(message).to_bytes()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransportCert {
    pub role: String,
    pub subkey: [u8; 32],
    pub sig: Vec<u8>,
}

impl TransportCert {
    pub fn verify(&self, master: &VerifyingKey, expected_role: &str) -> bool {
        if self.role != expected_role {
            return false;
        }
        let Ok(sig) = Signature::from_slice(&self.sig) else {
            return false;
        };
        master
            .verify_strict(&cert_message(&self.role, &self.subkey), &sig)
            .is_ok()
    }
}

fn cert_message(role: &str, subkey: &[u8; 32]) -> Vec<u8> {
    let mut m = CERT_DOMAIN.to_vec();
    m.push(b':');
    m.extend_from_slice(role.as_bytes());
    m.push(b':');
    m.extend_from_slice(subkey);
    m
}

pub fn node_id(master: &VerifyingKey) -> String {
    let b32 = data_encoding::BASE32_NOPAD.encode(master.as_bytes());
    format!("gsp1-{}", b32.to_lowercase())
}

pub fn parse_node_id(id: &str) -> Option<VerifyingKey> {
    let b32 = id.strip_prefix("gsp1-")?;
    let bytes = data_encoding::BASE32_NOPAD
        .decode(b32.to_uppercase().as_bytes())
        .ok()?;
    VerifyingKey::from_bytes(bytes.as_slice().try_into().ok()?).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> MasterKey {
        MasterKey::from_bytes(&[seed; 32])
    }

    #[test]
    fn cert_roundtrip() {
        let master = key(1);
        let cert = master.certify(ROLE_IROH, [7; 32]);
        assert!(cert.verify(&master.public(), ROLE_IROH));
    }

    #[test]
    fn cert_rejects_wrong_master_role_or_subkey() {
        let master = key(1);
        let cert = master.certify(ROLE_IROH, [7; 32]);
        assert!(!cert.verify(&key(2).public(), ROLE_IROH));
        assert!(!cert.verify(&master.public(), "gossip-transport:onion"));
        let mut forged = cert.clone();
        forged.subkey = [8; 32];
        assert!(!forged.verify(&master.public(), ROLE_IROH));

        let mut relabeled = cert;
        relabeled.role = "gossip-transport:onion".into();
        assert!(!relabeled.verify(&master.public(), "gossip-transport:onion"));
    }

    #[test]
    fn node_id_roundtrip() {
        let master = key(3);
        let id = node_id(&master.public());
        assert!(id.starts_with("gsp1-"));
        assert_eq!(parse_node_id(&id).unwrap(), master.public());
        assert_eq!(parse_node_id("gsp1-notbase32!!"), None);
        assert_eq!(parse_node_id("nope"), None);
    }
}
