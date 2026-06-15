//! Per-object text CRDT wrapper: one Y.Doc per shared text object.
//!
//! Local edits are captured by diffing one accepted text snapshot to the next
//! and replaying the diff as Yjs insert/delete operations.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use eyre::{Result, WrapErr};
use similar::{ChangeTag, TextDiff};
use yrs::block::ClientID;
use yrs::types::text::TextRef;
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, Options, ReadTxn, StateVector, Text, Transact, Update};

pub use super::client_id_for_writer;

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

static READ_ONLY_CLIENT_ID_COUNTER: AtomicU64 = AtomicU64::new(10_000_000);

/// Client id for docs that only decode/apply/encode existing state. A doc's
/// client id never appears in encoded output unless ops are generated under
/// it, so these ids are safe ONLY for paths that never mint ops. Any local
/// mutation that will be published must use [`client_id_for_writer`], or the
/// resulting op ids can collide across daemons and silently diverge.
fn read_only_client_id() -> u64 {
    READ_ONLY_CLIENT_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
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
        let total_started = perf_start!();
        let decode_started = perf_start!();
        let update = Update::decode_v2(update_bytes).wrap_err("text_doc: decode update")?;
        trace_perf!(
            decode_started,
            update_bytes = update_bytes.len(),
            "text_doc: decoded update"
        );
        let apply_started = perf_start!();
        let mut txn = self.doc.transact_mut();
        txn.apply_update(update)
            .wrap_err("text_doc: apply update")?;
        trace_perf!(
            apply_started,
            update_bytes = update_bytes.len(),
            "text_doc: applied decoded update"
        );
        trace_perf!(
            total_started,
            update_bytes = update_bytes.len(),
            "text_doc: apply_update complete"
        );
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

        let total_started = perf_start!();
        let sv_before = self.state_vector();
        let diff_started = perf_start!();
        self.apply_string_diff(old_text, new_text);
        trace_perf!(
            diff_started,
            old_bytes = old_text.len(),
            new_bytes = new_text.len(),
            "text_doc: apply_string_diff complete"
        );

        let update_started = perf_start!();
        let update_bytes = self.encode_update_since(&sv_before);
        trace_perf!(
            update_started,
            update_bytes = update_bytes.len(),
            "text_doc: encoded update since state vector"
        );
        let full_state_started = perf_start!();
        let full_state_bytes = self.encode_full_state();
        trace_perf!(
            full_state_started,
            full_state_bytes = full_state_bytes.len(),
            "text_doc: encoded full state after capture"
        );
        let get_text_started = perf_start!();
        let text = self.get_text();
        trace_perf!(
            get_text_started,
            text_bytes = text.len(),
            "text_doc: read captured text"
        );
        trace_perf!(
            total_started,
            old_bytes = old_text.len(),
            new_bytes = new_text.len(),
            update_bytes = update_bytes.len(),
            full_state_bytes = full_state_bytes.len(),
            "text_doc: capture_text_change complete"
        );

        Ok(Some(TextCapture {
            update_bytes,
            full_state_bytes,
            text,
        }))
    }

    /// Minimum length (in bytes) of a contiguous equal run to be considered
    /// "significant" rather than noise from coincidental character matches.
    const MIN_SIGNIFICANT_RUN: usize = 3;

    /// If no single equal run covers at least this fraction of old_len, AND
    /// the total significant-run bytes are below
    /// [`Self::CHAR_DIFF_RETAIN_THRESHOLD`], fall back to blunt mode.
    const SUBSTANTIAL_RUN_FRACTION: f64 = 1.0 / 3.0;

    /// Minimum fraction of old-text characters (in significant contiguous
    /// equal runs) that must be retained for us to use the fine-grained diff.
    const CHAR_DIFF_RETAIN_THRESHOLD: f64 = 0.5;

    fn apply_string_diff(&self, old: &str, new: &str) {
        let diff = TextDiff::from_chars(old, new);
        let changes: Vec<_> = diff.iter_all_changes().collect();

        let old_len = old.len();
        if old_len > 0 {
            // Measure contiguous equal runs: their maximum length and total
            // significant bytes. A single long run proves real shared content;
            // many short scattered runs are coincidental LCS noise.
            let mut max_run: usize = 0;
            let mut significant_equal_bytes: usize = 0;
            let mut current_run: usize = 0;
            for c in &changes {
                if c.tag() == ChangeTag::Equal {
                    current_run += c.value().len();
                } else {
                    if current_run > max_run {
                        max_run = current_run;
                    }
                    if current_run >= Self::MIN_SIGNIFICANT_RUN {
                        significant_equal_bytes += current_run;
                    }
                    current_run = 0;
                }
            }
            if current_run > max_run {
                max_run = current_run;
            }
            if current_run >= Self::MIN_SIGNIFICANT_RUN {
                significant_equal_bytes += current_run;
            }

            // If there's at least one run covering a substantial portion of
            // old text, the diff has genuine shared structure → fine-grained.
            let has_substantial_run =
                max_run as f64 >= old_len as f64 * Self::SUBSTANTIAL_RUN_FRACTION;
            if !has_substantial_run {
                let retained = significant_equal_bytes as f64 / old_len as f64;
                if retained < Self::CHAR_DIFF_RETAIN_THRESHOLD {
                    let mut txn = self.doc.transact_mut();
                    self.text.remove_range(&mut txn, 0, old_len as u32);
                    if !new.is_empty() {
                        self.text.insert(&mut txn, 0, new);
                    }
                    return;
                }
            }
        }

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
    let total_started = perf_start!();
    let decode_started = perf_start!();
    let doc = TextObjectDoc::from_full_state(client_id, state_bytes)?;
    trace_perf!(
        decode_started,
        state_bytes = state_bytes.len(),
        "text_doc: decode_text_state full-state decode complete"
    );
    let get_text_started = perf_start!();
    let text = doc.get_text();
    trace_perf!(
        get_text_started,
        text_bytes = text.len(),
        "text_doc: decode_text_state get_text complete"
    );
    let encode_started = perf_start!();
    let full_state_bytes = doc.encode_full_state();
    trace_perf!(
        encode_started,
        full_state_bytes = full_state_bytes.len(),
        "text_doc: decode_text_state full-state encode complete"
    );
    trace_perf!(
        total_started,
        state_bytes = state_bytes.len(),
        text_bytes = text.len(),
        full_state_bytes = full_state_bytes.len(),
        "text_doc: decode_text_state complete"
    );
    Ok(TextDocState {
        text,
        full_state_bytes,
    })
}

pub fn apply_text_update(base_state: &[u8], update_bytes: &[u8]) -> Result<TextDocState> {
    let total_started = perf_start!();
    let decode_started = perf_start!();
    let doc = TextObjectDoc::from_full_state(read_only_client_id(), base_state)?;
    trace_perf!(
        decode_started,
        base_state_bytes = base_state.len(),
        update_bytes = update_bytes.len(),
        "text_doc: apply_text_update base decode complete"
    );
    let apply_started = perf_start!();
    doc.apply_update(update_bytes)?;
    trace_perf!(
        apply_started,
        base_state_bytes = base_state.len(),
        update_bytes = update_bytes.len(),
        "text_doc: apply_text_update update apply complete"
    );
    let get_text_started = perf_start!();
    let text = doc.get_text();
    trace_perf!(
        get_text_started,
        text_bytes = text.len(),
        "text_doc: apply_text_update get_text complete"
    );
    let encode_started = perf_start!();
    let full_state_bytes = doc.encode_full_state();
    trace_perf!(
        encode_started,
        full_state_bytes = full_state_bytes.len(),
        "text_doc: apply_text_update full-state encode complete"
    );
    trace_perf!(
        total_started,
        base_state_bytes = base_state.len(),
        update_bytes = update_bytes.len(),
        text_bytes = text.len(),
        full_state_bytes = full_state_bytes.len(),
        "text_doc: apply_text_update complete"
    );
    Ok(TextDocState {
        text,
        full_state_bytes,
    })
}

pub fn encode_state_delta(base_state: &[u8], next_state: &[u8]) -> Result<Bytes> {
    let base_doc = TextObjectDoc::from_full_state(read_only_client_id(), base_state)?;
    let next_doc = TextObjectDoc::from_full_state(read_only_client_id(), next_state)?;
    Ok(next_doc.encode_update_since(&base_doc.state_vector()))
}

/// Captures a local edit as Yjs ops minted under `client_id`, which MUST be
/// the writer-derived id from [`client_id_for_writer`]: these ops are
/// published to the shared log, and op ids must be globally unique per
/// (client, clock) or replicas silently diverge.
pub fn capture_text_change(
    client_id: u64,
    base_state: &[u8],
    old_text: &str,
    new_text: &str,
) -> Result<Option<TextCapture>> {
    let total_started = perf_start!();
    let decode_started = perf_start!();
    let doc = TextObjectDoc::from_full_state(client_id, base_state)?;
    trace_perf!(
        decode_started,
        base_state_bytes = base_state.len(),
        old_bytes = old_text.len(),
        new_bytes = new_text.len(),
        "text_doc: capture_text_change base decode complete"
    );
    let capture = doc.capture_text_change(old_text, new_text)?;
    trace_perf!(
        total_started,
        base_state_bytes = base_state.len(),
        old_bytes = old_text.len(),
        new_bytes = new_text.len(),
        changed = capture.is_some(),
        "text_doc: capture_text_change wrapper complete"
    );
    Ok(capture)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::MAX_SAFE_CLIENT_ID;

    #[test]
    fn captures_incremental_text_update() -> Result<()> {
        let base = text_state_from_content(1, "hello\n");
        let capture = capture_text_change(7, base.as_ref(), "hello\n", "hello world\n")?
            .expect("text changed");

        assert_eq!(capture.text, "hello world\n");

        let applied = apply_text_update(base.as_ref(), capture.update_bytes.as_ref())?;
        assert_eq!(applied.text, "hello world\n");

        Ok(())
    }

    #[test]
    fn captures_clear_to_empty() -> Result<()> {
        let base = text_state_from_content(1, "hello world\n");
        let capture =
            capture_text_change(7, base.as_ref(), "hello world\n", "")?.expect("text changed");

        assert_eq!(capture.text, "");

        let applied = apply_text_update(base.as_ref(), capture.update_bytes.as_ref())?;
        assert_eq!(applied.text, "");

        Ok(())
    }

    #[test]
    fn captures_clear_to_empty_when_base_uses_same_client_id() -> Result<()> {
        let base = text_state_from_content(7, "hello world\n");
        let capture =
            capture_text_change(7, base.as_ref(), "hello world\n", "")?.expect("text changed");

        assert_eq!(capture.text, "");

        let applied = apply_text_update(base.as_ref(), capture.update_bytes.as_ref())?;
        assert_eq!(applied.text, "");

        Ok(())
    }

    #[test]
    fn captures_clear_to_empty_with_writer_derived_client_id() -> Result<()> {
        let client_id = client_id_for_writer(&[
            0x13, 0xb3, 0x3c, 0x85, 0xed, 0x72, 0x2a, 0x17, 0x38, 0x20, 0x3a, 0x77, 0x8c, 0xfd,
            0x5c, 0x99,
        ]);
        let text = "hi there\nthis is\na new file\n";
        let base = text_state_from_content(client_id, text);
        let capture =
            capture_text_change(client_id, base.as_ref(), text, "")?.expect("text changed");

        assert_eq!(capture.text, "");

        let applied = apply_text_update(base.as_ref(), capture.update_bytes.as_ref())?;
        assert_eq!(applied.text, "");

        Ok(())
    }

    #[test]
    fn delete_update_targets_source_doc_structs() -> Result<()> {
        let prior = text_state_from_content(7, "hello world\n");
        let stable = text_state_from_content(8, "hello world\n");
        let prior_capture =
            capture_text_change(7, prior.as_ref(), "hello world\n", "")?.expect("text changed");
        let stable_capture =
            capture_text_change(7, stable.as_ref(), "hello world\n", "")?.expect("text changed");

        let prior_update_on_stable =
            apply_text_update(stable.as_ref(), prior_capture.update_bytes.as_ref())?;
        assert_eq!(prior_update_on_stable.text, "hello world\n");

        let stable_update_on_stable =
            apply_text_update(stable.as_ref(), stable_capture.update_bytes.as_ref())?;
        assert_eq!(stable_update_on_stable.text, "");

        Ok(())
    }

    #[test]
    fn capture_mints_ops_under_writer_client_id() -> Result<()> {
        let writer_id = [0xA7u8; 16];
        let client_id = client_id_for_writer(&writer_id);
        let base = text_state_from_content(42, "base\n");

        let capture = capture_text_change(client_id, base.as_ref(), "base\n", "base\nedit\n")?
            .expect("text changed");

        let doc = TextObjectDoc::from_full_state(1, capture.full_state_bytes.as_ref())?;
        let clock = doc.state_vector().get(&client_id);
        assert!(
            clock > 0,
            "capture ops must be attributed to the writer-derived client id, got clock {clock}"
        );
        Ok(())
    }

    #[test]
    fn concurrent_captures_from_distinct_writers_both_survive() -> Result<()> {
        let base = text_state_from_content(42, "base\n");
        let client_a = client_id_for_writer(&[1u8; 16]);
        let client_b = client_id_for_writer(&[2u8; 16]);
        assert_ne!(client_a, client_b);

        let capture_a = capture_text_change(client_a, base.as_ref(), "base\n", "base\nfrom a\n")?
            .expect("text changed");
        let capture_b = capture_text_change(client_b, base.as_ref(), "base\n", "base\nfrom b\n")?
            .expect("text changed");

        let one = apply_text_update(base.as_ref(), capture_a.update_bytes.as_ref())?;
        let both = apply_text_update(
            one.full_state_bytes.as_ref(),
            capture_b.update_bytes.as_ref(),
        )?;
        assert!(both.text.contains("from a"));
        assert!(both.text.contains("from b"));

        let other_one = apply_text_update(base.as_ref(), capture_b.update_bytes.as_ref())?;
        let other_both = apply_text_update(
            other_one.full_state_bytes.as_ref(),
            capture_a.update_bytes.as_ref(),
        )?;
        assert_eq!(both.text, other_both.text);
        Ok(())
    }

    #[test]
    fn concurrent_full_overwrites_do_not_interleave() -> Result<()> {
        let base = text_state_from_content(42, "shared document\n");
        let client_a = client_id_for_writer(&[1u8; 16]);
        let client_b = client_id_for_writer(&[2u8; 16]);
        assert_ne!(client_a, client_b);

        let capture_a = capture_text_change(
            client_a,
            base.as_ref(),
            "shared document\n",
            "edited by node-a\n",
        )?
        .expect("text changed");
        let capture_b = capture_text_change(
            client_b,
            base.as_ref(),
            "shared document\n",
            "edited by node-b\n",
        )?
        .expect("text changed");

        // Apply A then B.
        let ab = {
            let one = apply_text_update(base.as_ref(), capture_a.update_bytes.as_ref())?;
            apply_text_update(
                one.full_state_bytes.as_ref(),
                capture_b.update_bytes.as_ref(),
            )?
        };
        // Apply B then A.
        let ba = {
            let one = apply_text_update(base.as_ref(), capture_b.update_bytes.as_ref())?;
            apply_text_update(
                one.full_state_bytes.as_ref(),
                capture_a.update_bytes.as_ref(),
            )?
        };
        // Both orders must converge.
        assert_eq!(ab.text, ba.text);
        // The merged text must be one full version concatenated with the other
        // (no character-level interleaving). One of the two orderings is valid.
        let valid_a_first = "edited by node-a\nedited by node-b\n";
        let valid_b_first = "edited by node-b\nedited by node-a\n";
        assert!(
            ab.text == valid_a_first || ab.text == valid_b_first,
            "expected one clean ordering, got: {:?}",
            ab.text,
        );
        Ok(())
    }

    #[test]
    fn small_edit_uses_fine_grained_diff() -> Result<()> {
        let base = text_state_from_content(1, "hello world\n");
        let capture =
            capture_text_change(7, base.as_ref(), "hello world\n", "hello brave world\n")?
                .expect("text changed");
        assert_eq!(capture.text, "hello brave world\n");
        Ok(())
    }

    #[test]
    fn client_id_for_writer_is_stable_and_in_safe_range() {
        // Pin the derivation: first 4 writer-id bytes, big-endian, nonzero.
        // A silent change here would re-identify every writer.
        assert_eq!(client_id_for_writer(&[0xFFu8; 16]), MAX_SAFE_CLIENT_ID);
        assert_eq!(
            client_id_for_writer(&[0x01, 0, 0, 0, 0, 0, 0, 0x02, 0xEE, 0xEE]),
            0x0100_0000
        );
        assert_eq!(client_id_for_writer(&[0u8; 16]), 1);
    }

    #[test]
    fn full_state_from_max_safe_client_id_is_valid_update() -> Result<()> {
        let update = text_state_from_content(MAX_SAFE_CLIENT_ID, "from a\n");
        let base = empty_text_state(1);
        let applied = apply_text_update(base.as_ref(), update.as_ref())?;

        assert_eq!(applied.text, "from a\n");

        Ok(())
    }
}
