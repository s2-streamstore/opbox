use crate::crdt::types::ObjectId;
use crate::fs::types::RelativePath;
use crate::semantic::table::datetime_from_unix_ns;
use bytes::Bytes;
use time::OffsetDateTime;

#[derive(Debug)]
pub struct Row {
    pub path: RelativePath,
    pub claimed_path: RelativePath,
    pub claim_id: Bytes,
    pub object_id: ObjectId,
    pub updated_at: OffsetDateTime,
}

impl Row {
    pub fn from_sql_row(row: &turso::Row) -> eyre::Result<Self> {
        let path_raw = row.get::<String>(0)?;
        let claimed_path_raw = row.get::<String>(1)?;
        let claim_id = Bytes::from(row.get::<Vec<u8>>(2)?);
        let object_id = ObjectId(Bytes::from(row.get::<Vec<u8>>(3)?));
        let updated_at_ns = row.get::<i64>(4)?;

        let path = RelativePath::parse(&path_raw)?;
        let claimed_path = RelativePath::parse(&claimed_path_raw)?;
        if claim_id.is_empty() {
            eyre::bail!("stable_tree.claim_id missing");
        }
        if object_id.0.is_empty() {
            eyre::bail!("stable_tree.object_id missing");
        }

        Ok(Self {
            path,
            claimed_path,
            claim_id,
            object_id,
            updated_at: datetime_from_unix_ns("stable_tree.updated_at_ns", updated_at_ns)?,
        })
    }
}
