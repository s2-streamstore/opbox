use crate::crdt::types::{ObjectId, ObjectKind};
use crate::semantic::table::datetime_from_unix_ns;
use crate::types::DaemonWriterId;
use bytes::Bytes;
use std::str::FromStr;
use time::OffsetDateTime;

#[derive(Debug)]
pub struct Row {
    pub object_id: ObjectId,
    pub object_kind: ObjectKind,
    pub creator_writer_id: DaemonWriterId,
    pub created_at: OffsetDateTime,
}

impl Row {
    pub fn from_sql_row(row: &turso::Row) -> eyre::Result<Self> {
        let object_id = ObjectId(Bytes::from(row.get::<Vec<u8>>(0)?));
        let object_kind_raw = row.get::<String>(1)?;
        let creator_writer_id = DaemonWriterId(Bytes::from(row.get::<Vec<u8>>(2)?));
        let created_at_ns = row.get::<i64>(3)?;

        if object_id.0.is_empty() {
            eyre::bail!("objects.object_id missing");
        }
        if creator_writer_id.0.is_empty() {
            eyre::bail!("objects.creator_writer_id missing");
        }

        let object_kind = ObjectKind::from_str(&object_kind_raw)
            .map_err(|_| eyre::eyre!("invalid objects.object_kind: {object_kind_raw:?}"))?;

        Ok(Self {
            object_id,
            object_kind,
            creator_writer_id,
            created_at: datetime_from_unix_ns("objects.created_at_ns", created_at_ns)?,
        })
    }
}
