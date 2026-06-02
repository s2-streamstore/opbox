//! Per-object text CRDT wrapper: one Y.Doc per shared text object.
//!
//! Local edits are captured by diffing one accepted text snapshot to the next
//! and replaying the diff as Yjs insert/delete operations.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use eyre::{Result, WrapErr};
use similar::{ChangeTag, TextDiff};
use tracing::trace;
use yrs::block::ClientID;
use yrs::types::text::TextRef;
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, Options, ReadTxn, StateVector, Text, Transact, Update};

const MAX_SAFE_CLIENT_ID: u64 = (1u64 << 53) - 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextDocState {
    pub text: String,
    pub full_state_bytes: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextCapture {
    pub update_bytes: Bytes,
    pub full_state_bytes: Bytes,
    pub text: String,
}

pub fn client_id_for_writer(writer_id: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    writer_id.hash(&mut hasher);
    // Yrs exposes ClientID as u64, but very large ids can produce updates that
    // fail decode in this dependency stack. Keep deterministic writer-derived
    // ids inside the JS-safe integer range used by Yjs implementations.
    let id = hasher.finish() & MAX_SAFE_CLIENT_ID;
    if id == 0 { 1 } else { id }
}

static CONVENIENCE_CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(10_000_000);

fn fresh_convenience_client_id() -> u64 {
    CONVENIENCE_CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub struct TextObjectDoc {
    doc: Doc,
    text: TextRef,
}

impl TextObjectDoc {
    pub fn new(client_id: u64) -> Self {
        let doc = Doc::with_options(Options {
            client_id,
            skip_gc: false,
            ..Default::default()
        });
        let text = doc.get_or_insert_text("text");
        Self { doc, text }
    }

    pub fn from_text(client_id: u64, initial: &str) -> Self {
        let doc = Self::new(client_id);
        if !initial.is_empty() {
            let mut txn = doc.doc.transact_mut();
            doc.text.insert(&mut txn, 0, initial);
        }
        doc
    }

    pub fn from_full_state(client_id: ClientID, state_bytes: &[u8]) -> Result<Self> {
        if state_bytes.is_empty() {
            return Ok(Self::new(client_id));
        }

        let doc = Doc::with_options(Options {
            client_id,
            skip_gc: false,
            ..Default::default()
        });
        {
            // V2 is required: V1 truncates client ids to u32 during decode.
            let update = Update::decode_v2(state_bytes).wrap_err("text_doc: decode full state")?;
            let mut txn = doc.transact_mut();
            txn.apply_update(update)
                .wrap_err("text_doc: apply full state")?;
        }
        let text = doc.get_or_insert_text("text");
        Ok(Self { doc, text })
    }

    pub fn get_text(&self) -> String {
        let txn = self.doc.transact();
        self.text.get_string(&txn)
    }

    pub fn state_vector(&self) -> StateVector {
        let txn = self.doc.transact();
        txn.state_vector()
    }

    pub fn encode_full_state(&self) -> Bytes {
        self.encode_update_since(&StateVector::default())
    }

    pub fn encode_update_since(&self, sv: &StateVector) -> Bytes {
        let txn = self.doc.transact();
        Bytes::from(txn.encode_state_as_update_v2(sv))
    }

    pub fn apply_update(&self, update_bytes: &[u8]) -> Result<()> {
        let update = Update::decode_v2(update_bytes).wrap_err("text_doc: decode update")?;
        let mut txn = self.doc.transact_mut();
        txn.apply_update(update)
            .wrap_err("text_doc: apply update")?;
        trace!("text_doc: applied update ({} bytes)", update_bytes.len());
        Ok(())
    }

    pub fn capture_text_change(
        &self,
        old_text: &str,
        new_text: &str,
    ) -> Result<Option<TextCapture>> {
        if old_text == new_text {
            return Ok(None);
        }

        let sv_before = self.state_vector();
        self.apply_string_diff(old_text, new_text);

        let update_bytes = self.encode_update_since(&sv_before);
        let full_state_bytes = self.encode_full_state();
        let text = self.get_text();

        Ok(Some(TextCapture {
            update_bytes,
            full_state_bytes,
            text,
        }))
    }

    fn apply_string_diff(&self, old: &str, new: &str) {
        let diff = TextDiff::from_chars(old, new);
        let changes: Vec<_> = diff.iter_all_changes().collect();

        let mut txn = self.doc.transact_mut();
        let mut pos: u32 = 0;
        let mut i = 0;

        while i < changes.len() {
            match changes[i].tag() {
                ChangeTag::Equal => {
                    let mut byte_count = 0u32;
                    while i < changes.len() && changes[i].tag() == ChangeTag::Equal {
                        byte_count += changes[i].value().len() as u32;
                        i += 1;
                    }
                    pos += byte_count;
                }
                ChangeTag::Delete => {
                    let mut byte_count = 0u32;
                    while i < changes.len() && changes[i].tag() == ChangeTag::Delete {
                        byte_count += changes[i].value().len() as u32;
                        i += 1;
                    }
                    self.text.remove_range(&mut txn, pos, byte_count);
                }
                ChangeTag::Insert => {
                    let mut buf = String::new();
                    while i < changes.len() && changes[i].tag() == ChangeTag::Insert {
                        buf.push_str(changes[i].value());
                        i += 1;
                    }
                    let byte_count = buf.len() as u32;
                    self.text.insert(&mut txn, pos, &buf);
                    pos += byte_count;
                }
            }
        }
    }
}

pub fn empty_text_state(client_id: u64) -> Bytes {
    TextObjectDoc::new(client_id).encode_full_state()
}

pub fn text_state_from_content(client_id: u64, content: &str) -> Bytes {
    TextObjectDoc::from_text(client_id, content).encode_full_state()
}

pub fn decode_text_state(client_id: u64, state_bytes: &[u8]) -> Result<TextDocState> {
    let doc = TextObjectDoc::from_full_state(client_id, state_bytes)?;
    Ok(TextDocState {
        text: doc.get_text(),
        full_state_bytes: doc.encode_full_state(),
    })
}

pub fn apply_text_update(base_state: &[u8], update_bytes: &[u8]) -> Result<TextDocState> {
    let doc = TextObjectDoc::from_full_state(fresh_convenience_client_id(), base_state)?;
    doc.apply_update(update_bytes)?;
    Ok(TextDocState {
        text: doc.get_text(),
        full_state_bytes: doc.encode_full_state(),
    })
}

pub fn encode_state_delta(base_state: &[u8], next_state: &[u8]) -> Result<Bytes> {
    let base_doc = TextObjectDoc::from_full_state(fresh_convenience_client_id(), base_state)?;
    let next_doc = TextObjectDoc::from_full_state(fresh_convenience_client_id(), next_state)?;
    Ok(next_doc.encode_update_since(&base_doc.state_vector()))
}

pub fn capture_text_change(
    base_state: &[u8],
    old_text: &str,
    new_text: &str,
) -> Result<Option<TextCapture>> {
    let doc = TextObjectDoc::from_full_state(fresh_convenience_client_id(), base_state)?;
    doc.capture_text_change(old_text, new_text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captures_incremental_text_update() -> Result<()> {
        let base = text_state_from_content(1, "hello\n");
        let capture =
            capture_text_change(base.as_ref(), "hello\n", "hello world\n")?.expect("text changed");

        assert_eq!(capture.text, "hello world\n");

        let applied = apply_text_update(base.as_ref(), capture.update_bytes.as_ref())?;
        assert_eq!(applied.text, "hello world\n");

        Ok(())
    }

    #[test]
    fn full_state_from_large_client_id_is_valid_update() -> Result<()> {
        let update = text_state_from_content(MAX_SAFE_CLIENT_ID, "from a\n");
        let base = empty_text_state(1);
        let applied = apply_text_update(base.as_ref(), update.as_ref())?;

        assert_eq!(applied.text, "from a\n");

        Ok(())
    }
}
