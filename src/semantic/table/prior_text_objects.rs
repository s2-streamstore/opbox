use crate::crdt::types::ObjectId;
use crate::semantic::table::datetime_from_unix_ns;
use bytes::Bytes;
use time::OffsetDateTime;

#[derive(Debug)]
pub struct Row {
    pub object_id: ObjectId,
    pub doc_blob: Bytes,
    pub accepted_at: OffsetDateTime,
}

impl Row {
    pub fn from_sql_row(row: &turso::Row) -> eyre::Result<Self> {
        let object_id = ObjectId(Bytes::from(row.get::<Vec<u8>>(0)?));
        if object_id.0.is_empty() {
            eyre::bail!("prior_text_objects.object_id missing");
        }

        let accepted_at_ns = row.get::<i64>(2)?;
        Ok(Self {
            object_id,
            doc_blob: Bytes::from(row.get::<Vec<u8>>(1)?),
            accepted_at: datetime_from_unix_ns(
                "prior_text_objects.accepted_at_ns",
                accepted_at_ns,
            )?,
        })
    }
}
