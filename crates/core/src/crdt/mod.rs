pub mod namespace;
pub mod text_doc;
pub mod types;

/// Yrs exposes ClientID as u64, but very large ids can produce updates that
/// fail decode in this dependency stack. Keep deterministic writer-derived
/// ids inside the JS-safe integer range used by Yjs implementations.
pub const MAX_SAFE_CLIENT_ID: u64 = (1u64 << 53) - 1;

/// Derive a stable Yjs client id from a daemon writer id.
///
/// Writer ids are already random bytes; take the first 8 directly rather
/// than hashing, since std's DefaultHasher is not stable across Rust
/// releases. The result is masked to [`MAX_SAFE_CLIENT_ID`] (2^53 − 1).
pub fn client_id_for_writer(writer_id: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    let len = writer_id.len().min(8);
    bytes[..len].copy_from_slice(&writer_id[..len]);
    let id = u64::from_be_bytes(bytes) & MAX_SAFE_CLIENT_ID;
    if id == 0 { 1 } else { id }
}
