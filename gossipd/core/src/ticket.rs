use serde::{Deserialize, Serialize};

use crate::identity::{TransportCert, ROLE_IROH, ROLE_ONION};
use crate::torauth::onion_pubkey;

const PREFIX: &str = "gossip:";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OnionEndpoint {
    pub addr: String,
    pub cert: TransportCert,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Ticket {
    pub master_pub: [u8; 32],
    pub name: String,
    pub cert: TransportCert,
    pub addrs: Vec<String>,
    pub onion: Option<OnionEndpoint>,
}

impl Ticket {
    pub fn encode(&self) -> String {
        let bytes = postcard::to_stdvec(self).expect("ticket encoding cannot fail");
        format!(
            "{PREFIX}{}",
            data_encoding::BASE32_NOPAD.encode(&bytes).to_lowercase()
        )
    }

    pub fn decode(s: &str) -> Result<Self, String> {
        let b32 = s.trim().strip_prefix(PREFIX).ok_or("not a gossip ticket")?;
        let bytes = data_encoding::BASE32_NOPAD
            .decode(b32.to_uppercase().as_bytes())
            .map_err(|_| "ticket is not valid base32")?;
        let ticket: Ticket =
            postcard::from_bytes(&bytes).map_err(|_| "ticket payload is malformed")?;
        let master = ed25519_dalek::VerifyingKey::from_bytes(&ticket.master_pub)
            .map_err(|_| "ticket master key is invalid")?;
        if !ticket.cert.verify(&master, ROLE_IROH) {
            return Err("ticket transport cert is not vouched by its master key".into());
        }
        if let Some(onion) = &ticket.onion {
            if !onion.cert.verify(&master, ROLE_ONION) {
                return Err("ticket onion cert is not vouched by its master key".into());
            }
            if onion_pubkey(&onion.addr) != Some(onion.cert.subkey) {
                return Err("ticket onion address does not match its certified key".into());
            }
        }
        Ok(ticket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::MasterKey;

    fn ticket() -> Ticket {
        let master = MasterKey::from_bytes(&[5; 32]);
        Ticket {
            master_pub: master.public().to_bytes(),
            name: "luk".into(),
            cert: master.certify(ROLE_IROH, [6; 32]),
            addrs: vec!["192.168.1.20:41641".into()],
            onion: None,
        }
    }

    fn onion_addr_for(key: [u8; 32]) -> String {
        let mut raw = key.to_vec();
        raw.extend_from_slice(&[0, 0]);
        raw.push(3);
        format!(
            "{}.onion",
            data_encoding::BASE32_NOPAD.encode(&raw).to_lowercase()
        )
    }

    #[test]
    fn onion_endpoint_roundtrip_and_forgery() {
        let master = MasterKey::from_bytes(&[5; 32]);
        let onion_key = [8; 32];
        let mut t = ticket();
        t.onion = Some(OnionEndpoint {
            addr: onion_addr_for(onion_key),
            cert: master.certify(crate::identity::ROLE_ONION, onion_key),
        });
        assert_eq!(Ticket::decode(&t.encode()).unwrap(), t);

        let mut swapped = t.clone();
        swapped.onion.as_mut().unwrap().addr = onion_addr_for([9; 32]);
        assert!(Ticket::decode(&swapped.encode())
            .unwrap_err()
            .contains("does not match"));

        let mut forged = t;
        forged.onion.as_mut().unwrap().cert =
            MasterKey::from_bytes(&[9; 32]).certify(crate::identity::ROLE_ONION, onion_key);
        assert!(Ticket::decode(&forged.encode())
            .unwrap_err()
            .contains("not vouched"));
    }

    #[test]
    fn roundtrip() {
        let t = ticket();
        let s = t.encode();
        assert!(s.starts_with("gossip:"));
        assert_eq!(Ticket::decode(&s).unwrap(), t);
    }

    #[test]
    fn rejects_garbage_and_forgery() {
        assert!(Ticket::decode("nope").is_err());
        assert!(Ticket::decode("gossip:!!!").is_err());
        assert!(Ticket::decode("gossip:hello").is_err());

        let mut forged = ticket();
        forged.cert = MasterKey::from_bytes(&[9; 32]).certify(ROLE_IROH, [6; 32]);
        assert!(Ticket::decode(&forged.encode())
            .unwrap_err()
            .contains("not vouched"));
    }
}
