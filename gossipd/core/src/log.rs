use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::identity::MasterKey;

const LOG_DOMAIN: &str = "gossip-log:v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub author: [u8; 32],
    pub recipient: [u8; 32],
    pub seq: u64,
    pub kind: String,
    pub body: String,

    #[serde(with = "f64_bits")]
    pub ts: f64,
    pub sig: Vec<u8>,
}

mod f64_bits {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &f64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(v.to_bits())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
        Ok(f64::from_bits(u64::deserialize(d)?))
    }
}

fn signable(
    author: &[u8; 32],
    recipient: &[u8; 32],
    seq: u64,
    kind: &str,
    body: &str,
    ts: f64,
) -> Vec<u8> {
    postcard::to_stdvec(&(LOG_DOMAIN, author, recipient, seq, kind, body, ts.to_bits()))
        .expect("postcard encoding of primitives cannot fail")
}

impl LogEntry {
    pub fn sign(
        author: &MasterKey,
        recipient: [u8; 32],
        seq: u64,
        kind: &str,
        body: &str,
        ts: f64,
    ) -> Self {
        let author_pub = author.public().to_bytes();
        let sig = author.sign(&signable(&author_pub, &recipient, seq, kind, body, ts));
        Self {
            author: author_pub,
            recipient,
            seq,
            kind: kind.to_string(),
            body: body.to_string(),
            ts,
            sig: sig.to_vec(),
        }
    }

    pub fn verify(&self) -> bool {
        let Ok(author) = VerifyingKey::from_bytes(&self.author) else {
            return false;
        };
        let Ok(sig) = Signature::from_slice(&self.sig) else {
            return false;
        };
        author
            .verify_strict(
                &signable(
                    &self.author,
                    &self.recipient,
                    self.seq,
                    &self.kind,
                    &self.body,
                    self.ts,
                ),
                &sig,
            )
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_verify_roundtrip() {
        let author = MasterKey::from_bytes(&[1; 32]);
        let entry = LogEntry::sign(&author, [9; 32], 3, "chat", "hi there", 1_755_000_000.25);
        assert!(entry.verify());
    }

    #[test]
    fn tampering_breaks_verification() {
        let author = MasterKey::from_bytes(&[1; 32]);
        let entry = LogEntry::sign(&author, [9; 32], 3, "chat", "hi", 1.0);
        for mutate in [
            |e: &mut LogEntry| e.body = "bye".into(),
            |e: &mut LogEntry| e.seq = 4,
            |e: &mut LogEntry| e.recipient = [8; 32],
            |e: &mut LogEntry| e.kind = "evil".into(),
            |e: &mut LogEntry| e.ts = 2.0,
            |e: &mut LogEntry| e.author = [2; 32],
        ] {
            let mut bad = entry.clone();
            mutate(&mut bad);
            assert!(!bad.verify());
        }
    }
}
