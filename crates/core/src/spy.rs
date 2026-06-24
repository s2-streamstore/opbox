use crate::crdt::namespace::{self, NamespaceDoc};
use crate::crdt::types::SharedMessage;
use crate::log::types::SharedMessageEnvelope;
use eyre::WrapErr;
use std::collections::HashSet;
use tokio::sync::broadcast;
use yrs::Update;
use yrs::updates::decoder::Decode;

const TEXT_PREVIEW_CHARS: usize = 120;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SpyEvent {
    SharedMessage(SpySharedMessage),
    Lagged { skipped: u64 },
    NamespaceSnapshot { yjs_state_b64: String },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SpySharedMessage {
    pub sequence_number: u64,
    pub timestamp_ns: i64,
    pub origin_writer_id_b64: String,
    pub origin_outbox_id: u64,
    pub message: SpySharedMessageKind,
    pub payload_size_bytes: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SpySharedMessageKind {
    NamespaceUpdate {
        yjs_update_b64: String,
        summary: Option<NamespaceUpdateSummary>,
    },
    TextUpdate {
        object_id_b64: String,
        summary: Option<TextUpdateSummary>,
    },
    BinaryPut {
        object_id_b64: String,
        wall_time_ns: i64,
        writer_id_b64: String,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NamespaceUpdateSummary {
    pub added_claims: Vec<NamespaceClaimSummary>,
    pub removed_claims: Vec<NamespaceClaimSummary>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NamespaceClaimSummary {
    pub path: String,
    pub object_id_b64: String,
    pub kind: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TextUpdateSummary {
    pub inserted_chars: usize,
    pub deleted_items: u64,
    pub inserted_preview: Option<String>,
    pub preview_truncated: bool,
}

#[derive(Debug)]
pub struct SpyOpen {
    pub namespace_snapshot_b64: String,
    pub events: broadcast::Receiver<SpyEvent>,
}

impl SpyEvent {
    pub fn shared_message(envelope: &SharedMessageEnvelope) -> Self {
        Self::shared_message_with_namespace_summary(envelope, None)
    }

    pub fn shared_message_with_namespace_summary(
        envelope: &SharedMessageEnvelope,
        namespace_summary: Option<NamespaceUpdateSummary>,
    ) -> Self {
        Self::SharedMessage(SpySharedMessage {
            sequence_number: envelope.sequence_number,
            timestamp_ns: nanos_i64(envelope.timestamp),
            origin_writer_id_b64: envelope.origin.daemon_writer_id.encode_b64(),
            origin_outbox_id: envelope.origin.outbox_id.get(),
            message: SpySharedMessageKind::from_shared_message(
                &envelope.shared_message,
                namespace_summary,
            ),
            payload_size_bytes: envelope.shared_message.approximate_size_bytes(),
        })
    }
}

impl SpySharedMessageKind {
    fn from_shared_message(
        message: &SharedMessage,
        namespace_summary: Option<NamespaceUpdateSummary>,
    ) -> Self {
        match message {
            SharedMessage::NamespaceUpdate { yjs_update } => Self::NamespaceUpdate {
                yjs_update_b64: base64_encode(yjs_update.as_ref()),
                summary: namespace_summary,
            },
            SharedMessage::TextObjectUpdate {
                object_id,
                yjs_update,
            } => Self::TextUpdate {
                object_id_b64: object_id.encode_b64(),
                summary: decode_text_update_summary(yjs_update.as_ref()),
            },
            SharedMessage::BinaryObjectPut {
                object_id,
                wall_time,
                writer_id,
                ..
            } => Self::BinaryPut {
                object_id_b64: object_id.encode_b64(),
                wall_time_ns: nanos_i64(*wall_time),
                writer_id_b64: writer_id.encode_b64(),
            },
        }
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn base64_decode(s: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.decode(s).ok()
}

/// Accumulates namespace CRDT state across updates to produce accurate diffs.
///
/// A single namespace delta applied to a fresh doc only works for creates
/// (self-contained). Removals have causal dependencies on prior state and
/// require the full history to resolve. This tracker maintains a running
/// doc so both creates and deletes produce meaningful summaries with paths.
pub struct NamespaceSpyTracker {
    doc: NamespaceDoc,
    known_active_ids: HashSet<String>,
    known_removed_ids: HashSet<String>,
}

impl NamespaceSpyTracker {
    pub fn new() -> Self {
        Self {
            doc: NamespaceDoc::new(namespace::read_only_client_id()),
            known_active_ids: HashSet::new(),
            known_removed_ids: HashSet::new(),
        }
    }

    /// Seed the tracker with a full namespace doc state (from DB snapshot).
    /// This brings the tracker up to date without producing a diff.
    pub fn seed_b64(&mut self, yjs_state_b64: &str) {
        if let Some(bytes) = base64_decode(yjs_state_b64) {
            let _ = self.seed(&bytes);
        }
    }

    /// Seed the tracker with full namespace doc state bytes.
    pub fn seed(&mut self, state_bytes: &[u8]) -> eyre::Result<()> {
        let doc = NamespaceDoc::from_full_state(namespace::read_only_client_id(), state_bytes)
            .wrap_err("seed namespace spy tracker")?;
        let active = doc
            .active_claims()
            .wrap_err("read active namespace claims")?;
        self.known_active_ids = active.iter().map(|c| c.claim_id.encode_b64()).collect();

        let removed = doc.removed_claim_ids();
        self.known_removed_ids = removed.iter().map(|id| id.encode_b64()).collect();

        self.doc = doc;
        Ok(())
    }

    pub fn snapshot_b64(&self) -> String {
        base64_encode(self.doc.encode_full_state().as_ref())
    }

    pub fn apply_b64(&mut self, yjs_update_b64: &str) -> Option<NamespaceUpdateSummary> {
        let bytes = base64_decode(yjs_update_b64)?;
        self.apply_update(&bytes)
    }

    pub fn apply_update(&mut self, update_bytes: &[u8]) -> Option<NamespaceUpdateSummary> {
        self.try_apply_update(update_bytes).ok()
    }

    pub fn try_apply_update(
        &mut self,
        update_bytes: &[u8],
    ) -> eyre::Result<NamespaceUpdateSummary> {
        self.doc
            .apply_update(update_bytes)
            .wrap_err("apply namespace update to spy tracker")?;

        let current_active = self
            .doc
            .active_claims()
            .wrap_err("read active namespace claims")?;
        let current_active_ids: HashSet<String> = current_active
            .iter()
            .map(|c| c.claim_id.encode_b64())
            .collect();

        let added_claims: Vec<_> = current_active
            .iter()
            .filter(|c| !self.known_active_ids.contains(&c.claim_id.encode_b64()))
            .map(|claim| self.claim_summary(claim))
            .collect();

        let current_removed = self.doc.removed_claim_ids();
        let current_removed_ids: HashSet<String> =
            current_removed.iter().map(|id| id.encode_b64()).collect();

        let removed_claims: Vec<_> = current_removed
            .iter()
            .filter(|id| !self.known_removed_ids.contains(&id.encode_b64()))
            .filter_map(|id| {
                let record = self.doc.get_claim(id)?;
                let kind = self
                    .doc
                    .get_object(&record.object_id)
                    .map(|meta| {
                        let s: &'static str = meta.kind.into();
                        s.to_string()
                    })
                    .unwrap_or_default();
                Some(NamespaceClaimSummary {
                    path: record.path.to_string(),
                    object_id_b64: record.object_id.encode_b64(),
                    kind,
                })
            })
            .collect();

        self.known_active_ids = current_active_ids;
        self.known_removed_ids = current_removed_ids;

        Ok(NamespaceUpdateSummary {
            added_claims,
            removed_claims,
        })
    }

    fn claim_summary(&self, claim: &crate::crdt::namespace::ActiveClaim) -> NamespaceClaimSummary {
        let kind = self
            .doc
            .get_object(&claim.record.object_id)
            .map(|meta| {
                let s: &'static str = meta.kind.into();
                s.to_string()
            })
            .unwrap_or_default();
        NamespaceClaimSummary {
            path: claim.record.path.to_string(),
            object_id_b64: claim.record.object_id.encode_b64(),
            kind,
        }
    }
}

fn decode_text_update_summary(update_bytes: &[u8]) -> Option<TextUpdateSummary> {
    let update = Update::decode_v2(update_bytes).ok()?;
    let deleted_items = update
        .delete_set()
        .iter()
        .flat_map(|(_, ranges)| ranges.iter())
        .map(|range| u64::from(range.end - range.start))
        .sum();

    // Yrs keeps decoded block contents private, but its Display output includes
    // string block contents. Treat this strictly as a spy/debug preview: it is
    // useful for human inspection, not part of the sync protocol.
    let inserted_text = extract_inserted_text_from_update_display(&update.to_string());
    let inserted_chars = inserted_text.chars().count();
    let (inserted_preview, preview_truncated) = if inserted_text.is_empty() {
        (None, false)
    } else {
        let preview = escape_preview(&inserted_text);
        let (preview, truncated) = truncate_chars(&preview, TEXT_PREVIEW_CHARS);
        (Some(preview), truncated)
    };

    Some(TextUpdateSummary {
        inserted_chars,
        deleted_items,
        inserted_preview,
        preview_truncated,
    })
}

fn extract_inserted_text_from_update_display(display: &str) -> String {
    let mut out = String::new();
    let mut cursor = 0;
    while let Some(relative_start) = display[cursor..].find(": '") {
        let start = cursor + relative_start + ": '".len();
        let Some(relative_end) = display[start..].find("')") else {
            break;
        };
        let end = start + relative_end;
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&display[start..end]);
        cursor = end + "')".len();
    }
    out
}

fn escape_preview(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        match ch {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push(' '),
            ch => out.push(ch),
        }
    }
    out
}

fn truncate_chars(value: &str, max_chars: usize) -> (String, bool) {
    let mut chars = value.chars();
    let truncated = value.chars().count() > max_chars;
    let mut out = chars.by_ref().take(max_chars).collect::<String>();
    if truncated {
        out.push_str("...");
    }
    (out, truncated)
}

fn nanos_i64(timestamp: time::OffsetDateTime) -> i64 {
    i64::try_from(timestamp.unix_timestamp_nanos()).expect("timestamp nanos fit in i64")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crdt::text_doc;

    #[test]
    fn text_update_summary_reports_insert_preview() -> eyre::Result<()> {
        let base = text_doc::empty_text_state(1);
        let capture = text_doc::capture_text_change(7, base.as_ref(), "", "hello world\n")?
            .expect("text changed");

        let summary =
            decode_text_update_summary(capture.update_bytes.as_ref()).expect("decoded summary");

        assert_eq!(summary.inserted_chars, "hello world\n".chars().count());
        assert_eq!(summary.deleted_items, 0);
        assert_eq!(summary.inserted_preview.as_deref(), Some("hello world\\n"));
        assert!(!summary.preview_truncated);
        Ok(())
    }

    #[test]
    fn text_update_summary_reports_delete_count() -> eyre::Result<()> {
        let base = text_doc::text_state_from_content(1, "hello world\n");
        let capture = text_doc::capture_text_change(7, base.as_ref(), "hello world\n", "hello\n")?
            .expect("text changed");

        let summary =
            decode_text_update_summary(capture.update_bytes.as_ref()).expect("decoded summary");

        assert_eq!(summary.inserted_chars, 0);
        assert_eq!(summary.deleted_items, " world".chars().count() as u64);
        assert_eq!(summary.inserted_preview, None);
        Ok(())
    }

    #[test]
    fn namespace_tracker_reports_new_claims() -> eyre::Result<()> {
        use crate::crdt::namespace;
        use crate::crdt::types::{NamespaceClaimId, ObjectId, ObjectKind};
        use crate::fs::types::RelativePath;
        use crate::types::DaemonWriterId;
        use bytes::Bytes;

        let doc = namespace::NamespaceDoc::new(1);
        let sv = doc.state_vector();

        let object_id = ObjectId(Bytes::from(vec![42u8; 16]));
        let claim_id = NamespaceClaimId(Bytes::from(vec![43u8; 16]));
        let writer_id = DaemonWriterId(Bytes::from(vec![7u8; 16]));
        let path = RelativePath::parse("src/main.rs")?;

        doc.add_new_object(&object_id, ObjectKind::Text, &writer_id);
        doc.add_new_claim(&claim_id, &object_id, &path);
        let update = doc.encode_update_since(&sv);

        let mut tracker = NamespaceSpyTracker::new();
        let summary = tracker
            .apply_update(update.as_ref())
            .expect("decoded namespace summary");

        assert_eq!(summary.added_claims.len(), 1);
        assert_eq!(summary.added_claims[0].path, "src/main.rs");
        assert_eq!(
            summary.added_claims[0].object_id_b64,
            object_id.encode_b64()
        );
        assert_eq!(summary.added_claims[0].kind, "text");
        assert_eq!(summary.removed_claims.len(), 0);
        Ok(())
    }

    #[test]
    fn namespace_tracker_reports_removed_claims() -> eyre::Result<()> {
        use crate::crdt::namespace;
        use crate::crdt::types::{NamespaceClaimId, ObjectId, ObjectKind};
        use crate::fs::types::RelativePath;
        use crate::types::DaemonWriterId;
        use bytes::Bytes;

        let doc = namespace::NamespaceDoc::new(1);
        let object_id = ObjectId(Bytes::from(vec![42u8; 16]));
        let claim_id = NamespaceClaimId(Bytes::from(vec![43u8; 16]));
        let writer_id = DaemonWriterId(Bytes::from(vec![7u8; 16]));
        let path = RelativePath::parse("old.txt")?;

        doc.add_new_object(&object_id, ObjectKind::Text, &writer_id);
        doc.add_new_claim(&claim_id, &object_id, &path);

        // Feed the create update first so the tracker has context.
        let create_update = doc.encode_update_since(&yrs::StateVector::default());
        let mut tracker = NamespaceSpyTracker::new();
        let create_summary = tracker
            .apply_update(create_update.as_ref())
            .expect("create summary");
        assert_eq!(create_summary.added_claims.len(), 1);

        // Now remove the claim and feed the delta.
        let sv = doc.state_vector();
        doc.remove_claim(&claim_id);
        let remove_update = doc.encode_update_since(&sv);

        let summary = tracker
            .apply_update(remove_update.as_ref())
            .expect("remove summary");

        assert_eq!(summary.added_claims.len(), 0);
        assert_eq!(summary.removed_claims.len(), 1);
        assert_eq!(summary.removed_claims[0].path, "old.txt");
        assert_eq!(
            summary.removed_claims[0].object_id_b64,
            object_id.encode_b64()
        );
        assert_eq!(summary.removed_claims[0].kind, "text");
        Ok(())
    }
}
