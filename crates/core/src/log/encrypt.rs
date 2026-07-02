use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit};
use bytes::Bytes;
use fast32::base32::CROCKFORD_LOWER;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub const KEY_LEN: usize = 32;
/// Crockford base32 length of a 32-byte key: ceil(32 * 8 / 5).
pub const KEY_ENCODED_LEN: usize = 52;
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
pub const CIPHERTEXT_OVERHEAD_LEN: usize = NONCE_LEN + TAG_LEN;

/// 256-bit key for client-side AES-256-GCM encryption.
///
/// The key never leaves the client; S2 only sees ciphertext.
#[derive(Clone)]
pub struct CipherKey([u8; KEY_LEN]);

impl CipherKey {
    pub fn generate() -> Self {
        Self(rand::random::<[u8; KEY_LEN]>())
    }

    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }
}

impl fmt::Debug for CipherKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CipherKey(***)")
    }
}

impl fmt::Display for CipherKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&CROCKFORD_LOWER.encode(&self.0))
    }
}

#[derive(Debug)]
pub struct CipherKeyParseError;

impl fmt::Display for CipherKeyParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid cipher key: expected {KEY_ENCODED_LEN} crockford base32 characters ({KEY_LEN} bytes)"
        )
    }
}

impl std::error::Error for CipherKeyParseError {}

impl FromStr for CipherKey {
    type Err = CipherKeyParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let decoded = CROCKFORD_LOWER
            .decode_str(s.trim())
            .map_err(|_| CipherKeyParseError)?;
        let key: [u8; KEY_LEN] = decoded.try_into().map_err(|_| CipherKeyParseError)?;
        Ok(Self(key))
    }
}

/// Source of AES-GCM nonces. GCM nonces need uniqueness, not secrecy, so a
/// seeded stream is safe where reproducibility matters.
#[derive(Debug, Clone)]
pub enum NonceRng {
    /// Process entropy — production.
    Os,
    /// Deterministic stream so simulation runs replay byte-identically.
    Seeded(Arc<Mutex<StdRng>>),
}

impl NonceRng {
    pub fn seeded(seed: u64) -> Self {
        Self::Seeded(Arc::new(Mutex::new(StdRng::seed_from_u64(seed))))
    }

    fn next_nonce(&self) -> [u8; NONCE_LEN] {
        match self {
            NonceRng::Os => rand::random(),
            NonceRng::Seeded(rng) => {
                let mut nonce = [0u8; NONCE_LEN];
                rng.lock().expect("nonce rng lock").fill_bytes(&mut nonce);
                nonce
            }
        }
    }
}

/// Encrypt plaintext with AES-256-GCM. Returns `nonce || ciphertext`.
pub fn encrypt(key: &CipherKey, nonce_rng: &NonceRng, plaintext: &[u8]) -> eyre::Result<Bytes> {
    let cipher = Aes256Gcm::new_from_slice(&key.0).expect("key is 32 bytes");
    let nonce_bytes = nonce_rng.next_nonce();
    let nonce = aes_gcm::Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| eyre::eyre!("encryption failed: {e}"))?;
    let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(Bytes::from(out))
}

/// Decrypt `nonce || ciphertext` with AES-256-GCM.
pub fn decrypt(key: &CipherKey, data: &[u8]) -> eyre::Result<Bytes> {
    if data.len() < NONCE_LEN {
        return Err(eyre::eyre!("decryption failed: ciphertext too short"));
    }
    let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
    let nonce = aes_gcm::Nonce::from_slice(nonce_bytes);
    let cipher = Aes256Gcm::new_from_slice(&key.0).expect("key is 32 bytes");
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| eyre::eyre!("decryption failed (wrong key or corrupted data)"))?;
    Ok(Bytes::from(plaintext))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let key = CipherKey::generate();
        let plaintext = b"hello world";
        let encrypted = encrypt(&key, &NonceRng::Os, plaintext).unwrap();
        assert_ne!(encrypted.as_ref(), plaintext);
        let decrypted = decrypt(&key, &encrypted).unwrap();
        assert_eq!(decrypted.as_ref(), plaintext);
    }

    #[test]
    fn wrong_key_fails() {
        let key1 = CipherKey::generate();
        let key2 = CipherKey::generate();
        let encrypted = encrypt(&key1, &NonceRng::Os, b"secret").unwrap();
        assert!(decrypt(&key2, &encrypted).is_err());
    }

    #[test]
    fn encoding_round_trip() {
        let key = CipherKey::generate();
        let encoded = key.to_string();
        assert_eq!(encoded.len(), KEY_ENCODED_LEN);
        let parsed: CipherKey = encoded.parse().unwrap();
        assert_eq!(key.0, parsed.0);
    }

    #[test]
    fn parse_rejects_invalid_input_without_panicking() {
        assert!("".parse::<CipherKey>().is_err());
        assert!("not a key".parse::<CipherKey>().is_err());
        // Multi-byte UTF-8 must return an error, not panic on a char boundary.
        let multibyte = "é".repeat(KEY_ENCODED_LEN / 2);
        assert!(multibyte.parse::<CipherKey>().is_err());
        // Wrong length decodes to the wrong byte count.
        assert!("abc".parse::<CipherKey>().is_err());
    }

    #[test]
    fn seeded_nonce_rng_is_deterministic() {
        let key = CipherKey::from_bytes([7u8; KEY_LEN]);
        let a = encrypt(&key, &NonceRng::seeded(42), b"payload").unwrap();
        let b = encrypt(&key, &NonceRng::seeded(42), b"payload").unwrap();
        assert_eq!(a, b);
        let c = encrypt(&key, &NonceRng::seeded(43), b"payload").unwrap();
        assert_ne!(a, c);
    }
}
