use crate::types::{DaemonWriterId, OutboxId, WorkspaceId};
use fast32::base32;

use crate::crdt::types::{ObjectId, SharedMessage};
use crate::log::encrypt;
use crate::log::types::SharedMessageOrigin;
use bytes::Bytes;
use s2_sdk::types::{AppendRecord, Header, MeteredBytes, SequencedRecord, StreamName};
use std::str::FromStr;
use time::OffsetDateTime;
use xxhash_rust::xxh3::xxh3_64;

pub enum S2Package {
    Inlined {
        record: AppendRecord,
    },
    Pointer {
        pointer_record: AppendRecord,
        pointer: ObjectPointer,
        parts: Vec<AppendRecord>,
    },
}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub struct ObjectPointerId(pub String);

impl ObjectPointerId {
    fn generate() -> Self {
        let id_bytes = rand::random::<[u8; 10]>();
        let object_pointer = base32::CROCKFORD_LOWER.encode(id_bytes.as_ref());
        assert_eq!(object_pointer.len(), 16);
        Self(object_pointer)
    }
}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub struct ObjectPointer {
    object_pointer_id: ObjectPointerId,
    creation_time: OffsetDateTime,
    pub checksum: u64,
    pub size_bytes: u64,
    pub n_records: usize,
}

impl ObjectPointer {
    pub fn stream_name(&self, workspace_id: WorkspaceId) -> eyre::Result<StreamName> {
        let fmt = format!(
            "{}/objects/{}:{}",
            workspace_id.0,
            self.creation_time.unix_timestamp_nanos(),
            self.object_pointer_id.0
        );
        Ok(StreamName::from_str(&fmt)?)
    }
}

#[cfg(feature = "sim")]
const MAX_INLINE_RECORD_SIZE: usize = 2 * 1024;

#[cfg(not(feature = "sim"))]
const MAX_INLINE_RECORD_SIZE: usize = (1024 * 1024) - (1024);
const S2_MAX_RECORD_METERED_BYTES: usize = 1024 * 1024;

pub const fn max_inline_record_size() -> usize {
    MAX_INLINE_RECORD_SIZE
}

fn encrypted_record_metered_bytes(
    plaintext_body_len: usize,
    headers: &[Header],
) -> eyre::Result<usize> {
    let header_record = AppendRecord::new(Bytes::new())?.with_headers(headers.to_vec())?;
    Ok(header_record.metered_bytes() + plaintext_body_len + encrypt::CIPHERTEXT_OVERHEAD_LEN)
}

pub fn header_value<'a>(headers: &'a [Header], name: &str) -> Option<&'a Bytes> {
    headers
        .iter()
        .find(|header| header.name.as_ref() == name.as_bytes())
        .map(|header| &header.value)
}

pub fn header_str<'a>(headers: &'a [Header], name: &str) -> eyre::Result<Option<&'a str>> {
    header_value(headers, name)
        .map(|value| std::str::from_utf8(value.as_ref()).map_err(Into::into))
        .transpose()
}

fn encode_common_headers(
    msg: &SharedMessage,
    origin: &SharedMessageOrigin,
) -> eyre::Result<Vec<Header>> {
    let mut headers = vec![];
    let message_kind: &'static str = msg.kind().into();

    headers.push(Header::new("message_kind", message_kind));
    headers.push(Header::new(
        "daemon_writer_id",
        origin.daemon_writer_id.0.clone(),
    ));
    headers.push(Header::new("outbox_id", origin.outbox_id.get().to_string()));

    match msg {
        SharedMessage::NamespaceUpdate { yjs_update: _ } => {}
        SharedMessage::TextObjectUpdate {
            object_id,
            yjs_update: _,
        } => {
            headers.push(Header::new("object_id", object_id.0.clone()));
        }
        SharedMessage::BinaryObjectPut {
            object_id,
            wall_time,
            writer_id,
            blob: _,
        } => {
            headers.push(Header::new("object_id", object_id.0.clone()));
            headers.push(Header::new("writer_id", writer_id.0.clone()));
            headers.push(Header::new(
                "wall_time_ns",
                wall_time.unix_timestamp_nanos().to_string(),
            ));
        }
    }

    Ok(headers)
}

pub fn decode_origin(headers: &[Header]) -> eyre::Result<SharedMessageOrigin> {
    let daemon_writer_id = DaemonWriterId(
        header_value(headers, "daemon_writer_id")
            .ok_or_else(|| eyre::eyre!("log record missing daemon_writer_id header"))?
            .clone(),
    );
    let outbox_id = OutboxId::new(
        header_str(headers, "outbox_id")?
            .ok_or_else(|| eyre::eyre!("log record missing outbox_id header"))?
            .parse::<u64>()?,
    );
    Ok(SharedMessageOrigin {
        daemon_writer_id,
        outbox_id,
    })
}

fn decode_object_id(headers: &[Header]) -> eyre::Result<ObjectId> {
    Ok(ObjectId(
        header_value(headers, "object_id")
            .ok_or_else(|| eyre::eyre!("log record missing object_id header"))?
            .clone(),
    ))
}

fn decode_daemon_writer_id(headers: &[Header]) -> eyre::Result<DaemonWriterId> {
    Ok(DaemonWriterId(
        header_value(headers, "writer_id")
            .ok_or_else(|| eyre::eyre!("log record missing writer_id header"))?
            .clone(),
    ))
}

pub fn s2_payload_to_shared_message(
    headers: &[Header],
    payload: Bytes,
) -> eyre::Result<SharedMessage> {
    let kind = header_str(headers, "message_kind")?
        .ok_or_else(|| eyre::eyre!("log record missing message_kind header"))?;

    let msg = match kind {
        "namespace_update" => SharedMessage::NamespaceUpdate {
            yjs_update: payload,
        },
        "text_update" => SharedMessage::TextObjectUpdate {
            object_id: decode_object_id(headers)?,
            yjs_update: payload,
        },
        "binary_put" => SharedMessage::BinaryObjectPut {
            object_id: decode_object_id(headers)?,
            writer_id: decode_daemon_writer_id(headers)?,
            wall_time: OffsetDateTime::from_unix_timestamp_nanos(
                header_str(headers, "wall_time_ns")?
                    .ok_or_else(|| eyre::eyre!("binary record missing wall_time_ns header"))?
                    .parse::<i128>()?,
            )?,
            blob: payload,
        },
        other => {
            eyre::bail!("unknown log record message_kind header: {other}");
        }
    };

    Ok(msg)
}

pub fn inline_record_to_shared_message(
    record: SequencedRecord,
) -> eyre::Result<Option<SharedMessage>> {
    if header_value(&record.headers, "pointer").is_some() {
        return Ok(None);
    }

    Ok(Some(s2_payload_to_shared_message(
        &record.headers,
        record.body,
    )?))
}

pub fn shared_to_s2_package(
    msg: SharedMessage,
    origin: &SharedMessageOrigin,
) -> eyre::Result<S2Package> {
    let payload = msg.payload().clone();
    let payload_size = payload.len();
    let pointer_headers = encode_common_headers(&msg, origin)?;
    if payload_size < MAX_INLINE_RECORD_SIZE
        && encrypted_record_metered_bytes(payload_size, &pointer_headers)
            .map_err(|err| eyre::eyre!("inline record metered size check failed: {err}"))?
            <= S2_MAX_RECORD_METERED_BYTES
    {
        // Inline it.

        Ok(S2Package::Inlined {
            record: AppendRecord::new(payload)?.with_headers(pointer_headers)?,
        })
    } else {
        // Split it.

        let object_pointer_id = ObjectPointerId::generate();
        let creation_time = OffsetDateTime::now_utc();
        let checksum = xxh3_64(payload.as_ref());

        let mut start = 0;
        let chunk_size = MAX_INLINE_RECORD_SIZE;
        let mut parts: Vec<AppendRecord> = vec![];
        while start < payload.len() {
            let end = (start + chunk_size).min(payload.len());
            let part = payload.slice(start..end);
            let record = AppendRecord::new(part)?;
            parts.push(record);
            start = end;
        }

        let pointer = ObjectPointer {
            object_pointer_id,
            creation_time,
            checksum,
            size_bytes: payload_size as u64,
            n_records: parts.len(),
        };

        let mut headers = vec![Header::new("pointer", serde_json::to_string(&pointer)?)];
        headers.extend(pointer_headers);

        let pointer_record =
            AppendRecord::new(bytes::Bytes::from_static(&[]))?.with_headers(headers)?;

        Ok(S2Package::Pointer {
            pointer_record,
            pointer,
            parts,
        })
    }
}
