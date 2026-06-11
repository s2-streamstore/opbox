use crate::crdt::types::SharedMessage;
use crate::log::types::SharedMessageEnvelope;
use yrs::Update;
use yrs::updates::decoder::Decode;

const TEXT_PREVIEW_CHARS: usize = 120;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SpyEvent {
    SharedMessage(SpySharedMessage),
    Lagged { skipped: u64 },
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
    NamespaceUpdate,
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
pub struct TextUpdateSummary {
    pub inserted_chars: usize,
    pub deleted_items: u64,
    pub inserted_preview: Option<String>,
    pub preview_truncated: bool,
}

impl SpyEvent {
    pub fn shared_message(envelope: &SharedMessageEnvelope) -> Self {
        Self::SharedMessage(SpySharedMessage {
            sequence_number: envelope.sequence_number,
            timestamp_ns: nanos_i64(envelope.timestamp),
            origin_writer_id_b64: envelope.origin.daemon_writer_id.encode_b64(),
            origin_outbox_id: envelope.origin.outbox_id.get(),
            message: SpySharedMessageKind::from_shared_message(&envelope.shared_message),
            payload_size_bytes: envelope.shared_message.approximate_size_bytes(),
        })
    }
}

impl SpySharedMessageKind {
    fn from_shared_message(message: &SharedMessage) -> Self {
        match message {
            SharedMessage::NamespaceUpdate { .. } => Self::NamespaceUpdate,
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
}
