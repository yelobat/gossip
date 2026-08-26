use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};

const AUTH_DOMAIN: &[u8] = b"gossip-tor-auth:v1";

fn auth_message(server_master: &[u8; 32], client_master: &[u8; 32], nonce: &[u8; 32]) -> Vec<u8> {
    let mut m = AUTH_DOMAIN.to_vec();
    m.extend_from_slice(server_master);
    m.extend_from_slice(client_master);
    m.extend_from_slice(nonce);
    m
}

pub fn sign_auth(
    tor_subkey: &SigningKey,
    server_master: &[u8; 32],
    client_master: &[u8; 32],
    nonce: &[u8; 32],
) -> [u8; 64] {
    tor_subkey
        .sign(&auth_message(server_master, client_master, nonce))
        .to_bytes()
}

pub fn verify_auth(
    tor_subkey_pub: &[u8; 32],
    server_master: &[u8; 32],
    client_master: &[u8; 32],
    nonce: &[u8; 32],
    sig: &[u8],
) -> bool {
    let Ok(key) = VerifyingKey::from_bytes(tor_subkey_pub) else {
        return false;
    };
    let Ok(sig) = Signature::from_slice(sig) else {
        return false;
    };
    key.verify_strict(&auth_message(server_master, client_master, nonce), &sig)
        .is_ok()
}

pub fn onion_pubkey(addr: &str) -> Option<[u8; 32]> {
    let b32 = addr.trim().strip_suffix(".onion").unwrap_or(addr.trim());
    let bytes = data_encoding::BASE32_NOPAD
        .decode(b32.to_uppercase().as_bytes())
        .ok()?;
    if bytes.len() != 35 || bytes[34] != 3 {
        return None;
    }
    bytes[..32].try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_roundtrip_and_bindings() {
        let subkey = SigningKey::from_bytes(&[7; 32]);
        let (server, client, nonce) = ([1; 32], [2; 32], [3; 32]);
        let sig = sign_auth(&subkey, &server, &client, &nonce);
        let pubkey = subkey.verifying_key().to_bytes();
        assert!(verify_auth(&pubkey, &server, &client, &nonce, &sig));

        assert!(!verify_auth(&[9; 32], &server, &client, &nonce, &sig));
        assert!(!verify_auth(&pubkey, &server, &client, &[4; 32], &sig));
        assert!(!verify_auth(&pubkey, &[9; 32], &client, &nonce, &sig));
        assert!(!verify_auth(&pubkey, &server, &[9; 32], &nonce, &sig));
        assert!(!verify_auth(&pubkey, &server, &client, &nonce, &[0; 64]));
    }

    #[test]
    fn onion_pubkey_extraction() {
        let key = [5u8; 32];
        let mut raw = key.to_vec();
        raw.extend_from_slice(&[0xaa, 0xbb]);
        raw.push(3);
        let addr = format!(
            "{}.onion",
            data_encoding::BASE32_NOPAD.encode(&raw).to_lowercase()
        );
        assert_eq!(onion_pubkey(&addr), Some(key));
        assert_eq!(onion_pubkey("tooshort.onion"), None);
        assert_eq!(onion_pubkey("not base32 at all"), None);
    }
}
