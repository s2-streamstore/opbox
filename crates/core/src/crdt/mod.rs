pub mod namespace;
pub mod text_doc;
pub mod types;

/// Yrs exposes ClientID as u64, but this dependency stack does not reliably
/// round-trip delete updates from large ids. Keep writer-derived ids in the
/// u32 range used by the original Yjs wire format.
pub const MAX_SAFE_CLIENT_ID: u64 = u32::MAX as u64;

/// Derive a stable Yjs client id from a daemon writer id.
///
/// Writer ids are already random bytes; take the first 4 directly rather
/// than hashing, since std's DefaultHasher is not stable across Rust
/// releases. The result is kept inside `1..=u32::MAX`.
pub fn client_id_for_writer(writer_id: &[u8]) -> u64 {
    let mut bytes = [0u8; 4];
    let len = writer_id.len().min(4);
    bytes[..len].copy_from_slice(&writer_id[..len]);
    let id = u32::from_be_bytes(bytes) as u64;
    if id == 0 { 1 } else { id }
}
