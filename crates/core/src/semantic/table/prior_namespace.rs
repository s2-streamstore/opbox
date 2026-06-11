use crate::semantic::table::datetime_from_unix_ns;
use bytes::Bytes;
use time::OffsetDateTime;

#[derive(Debug)]
pub struct Row {
    pub doc_blob: Bytes,
    pub accepted_at: OffsetDateTime,
}

impl Row {
    pub fn from_sql_row(row: &turso::Row) -> eyre::Result<Self> {
        let id = row.get::<i64>(0)?;
        if id != 1 {
            eyre::bail!("prior_namespace.id must be 1, got {id}");
        }

        let accepted_at_ns = row.get::<i64>(2)?;
        Ok(Self {
            doc_blob: Bytes::from(row.get::<Vec<u8>>(1)?),
            accepted_at: datetime_from_unix_ns("prior_namespace.accepted_at_ns", accepted_at_ns)?,
        })
    }
}
