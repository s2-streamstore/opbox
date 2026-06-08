use crate::crdt::types::{ObjectId, SharedMessage, SharedMessageKind};
use crate::semantic::table::datetime_from_unix_ns;
use crate::types::OutboxId;
use bytes::Bytes;
use std::str::FromStr;
use time::OffsetDateTime;

#[derive(Debug)]
pub struct Row {
    pub outbox_id: OutboxId,
    pub record_kind: SharedMessageKind,
    pub object_id: Option<ObjectId>,
    pub payload: Bytes,
    pub created_at: OffsetDateTime,
    pub inflight: bool,
}

impl Row {
    pub fn from_sql_row(row: &turso::Row) -> eyre::Result<Self> {
        let outbox_id_raw = row.get::<i64>(0)?;
        let record_kind_raw = row.get::<String>(1)?;
        let object_id = row.get::<Option<Vec<u8>>>(2)?.map(|bytes| {
            let bytes = Bytes::from(bytes);
            ObjectId(bytes)
        });
        let payload = Bytes::from(row.get::<Vec<u8>>(3)?);
        let created_at_ns = row.get::<i64>(4)?;
        let inflight_raw = row.get::<i64>(5)?;

        let outbox_id = u64::try_from(outbox_id_raw)
            .map(OutboxId::new)
            .map_err(|_| eyre::eyre!("outbox.outbox_id negative: {outbox_id_raw}"))?;
        let record_kind = SharedMessageKind::from_str(&record_kind_raw)
            .map_err(|_| eyre::eyre!("invalid outbox.record_kind: {record_kind_raw:?}"))?;

        match record_kind {
            SharedMessageKind::NamespaceUpdate => {
                if object_id.is_some() {
                    eyre::bail!("namespace outbox row unexpectedly had object_id");
                }
            }
            SharedMessageKind::TextUpdate => {
                let object_id = object_id
                    .as_ref()
                    .ok_or_else(|| eyre::eyre!("text outbox row missing object_id"))?;
                if object_id.0.is_empty() {
                    eyre::bail!("text outbox row object_id missing");
                }
            }
            SharedMessageKind::BinaryPut => {
                eyre::bail!("binary outbox rows are not supported in v0");
            }
        }

        Ok(Self {
            outbox_id,
            record_kind,
            object_id,
            payload,
            created_at: datetime_from_unix_ns("outbox.created_at_ns", created_at_ns)?,
            inflight: match inflight_raw {
                0 => false,
                1 => true,
                _ => eyre::bail!("invalid outbox.inflight: {inflight_raw}"),
            },
        })
    }

    pub fn into_shared_message(self) -> eyre::Result<(OutboxId, SharedMessage)> {
        let Self {
            outbox_id,
            record_kind,
            object_id,
            payload,
            created_at: _,
            inflight: _,
        } = self;

        let message = match record_kind {
            SharedMessageKind::NamespaceUpdate => SharedMessage::NamespaceUpdate {
                yjs_update: payload,
            },
            SharedMessageKind::TextUpdate => SharedMessage::TextObjectUpdate {
                object_id: object_id
                    .ok_or_else(|| eyre::eyre!("text outbox row missing object_id"))?,
                yjs_update: payload,
            },
            SharedMessageKind::BinaryPut => {
                eyre::bail!("binary outbox rows are not supported in v0");
            }
        };

        Ok((outbox_id, message))
    }
}
