use aes_gcm::aead::{Aead, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, KeyInit};
use bytes::Bytes;
use std::fmt;
use std::str::FromStr;

pub const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;

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
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct CipherKeyParseError;

impl fmt::Display for CipherKeyParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid cipher key: expected 64 hex characters (32 bytes)")
    }
}

impl std::error::Error for CipherKeyParseError {}

impl FromStr for CipherKey {
    type Err = CipherKeyParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.len() != KEY_LEN * 2 {
            return Err(CipherKeyParseError);
        }
        let mut key = [0u8; KEY_LEN];
        for (i, byte) in key.iter_mut().enumerate() {
            *byte =
                u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).map_err(|_| CipherKeyParseError)?;
        }
        Ok(Self(key))
    }
}

/// Encrypt plaintext with AES-256-GCM. Returns `nonce || ciphertext`.
pub fn encrypt(key: &CipherKey, plaintext: &[u8]) -> eyre::Result<Bytes> {
    let cipher = Aes256Gcm::new_from_slice(&key.0).expect("key is 32 bytes");
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| eyre::eyre!("encryption failed: {e}"))?;
    let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    out.extend_from_slice(&nonce);
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
        let encrypted = encrypt(&key, plaintext).unwrap();
        assert_ne!(encrypted.as_ref(), plaintext);
        let decrypted = decrypt(&key, &encrypted).unwrap();
        assert_eq!(decrypted.as_ref(), plaintext);
    }

    #[test]
    fn wrong_key_fails() {
        let key1 = CipherKey::generate();
        let key2 = CipherKey::generate();
        let encrypted = encrypt(&key1, b"secret").unwrap();
        assert!(decrypt(&key2, &encrypted).is_err());
    }

    #[test]
    fn hex_round_trip() {
        let key = CipherKey::generate();
        let hex = key.to_string();
        assert_eq!(hex.len(), 64);
        let parsed: CipherKey = hex.parse().unwrap();
        assert_eq!(key.0, parsed.0);
    }
}
