use crate::crdt::types::ObjectId;
use crate::fs::types::{
    FileContentFingerprint, FileFingerprint, FileHash, FileKey, FileStatFingerprint, RelativePath,
};
use crate::semantic::table::datetime_from_unix_ns;
use bytes::Bytes;
use time::OffsetDateTime;

#[derive(Debug)]
pub struct Row {
    pub path: RelativePath,
    pub claimed_path: RelativePath,
    pub claim_id: Bytes,
    pub object_id: ObjectId,
    pub fingerprint: FileFingerprint,
    pub accepted_at: OffsetDateTime,
    pub deleted_at: Option<OffsetDateTime>,
}

impl Row {
    pub fn from_sql_row(row: &turso::Row) -> eyre::Result<Self> {
        let path_raw = row.get::<String>(0)?;
        let claimed_path_raw = row.get::<String>(1)?;
        let claim_id = Bytes::from(row.get::<Vec<u8>>(2)?);
        let object_id = ObjectId(Bytes::from(row.get::<Vec<u8>>(3)?));
        let file_key_raw = row.get::<String>(4)?;
        let size_bytes_raw = row.get::<i64>(5)?;
        let mtime_ns = row.get::<i64>(6)?;
        let hash = row.get::<Option<Vec<u8>>>(7)?.map(Bytes::from);
        let accepted_at_ns = row.get::<i64>(8)?;
        let deleted_at_ns = row.get::<Option<i64>>(9)?;

        let path = RelativePath::parse(&path_raw)?;
        let claimed_path = RelativePath::parse(&claimed_path_raw)?;
        if claim_id.is_empty() {
            eyre::bail!("prior_tree.claim_id missing");
        }
        if object_id.0.is_empty() {
            eyre::bail!("prior_tree.object_id missing");
        }
        if file_key_raw.is_empty() {
            eyre::bail!("prior_tree.file_key missing");
        }

        let file_key = FileKey::decode(&file_key_raw)?;
        let size_bytes = u64::try_from(size_bytes_raw)
            .map_err(|_| eyre::eyre!("prior_tree.size_bytes negative: {size_bytes_raw}"))?;
        let stat = FileStatFingerprint::new(
            file_key,
            size_bytes,
            datetime_from_unix_ns("prior_tree.mtime_ns", mtime_ns)?,
        );
        let fingerprint = match hash {
            Some(hash) => FileFingerprint::StatAndContent {
                stat,
                content: FileContentFingerprint::new(FileHash::new(hash)),
            },
            None => FileFingerprint::StatOnly(stat),
        };
        let accepted_at = datetime_from_unix_ns("prior_tree.accepted_at_ns", accepted_at_ns)?;
        let deleted_at = deleted_at_ns
            .map(|ns| datetime_from_unix_ns("prior_tree.deleted_at_ns", ns))
            .transpose()?;

        if let Some(deleted_at) = deleted_at
            && deleted_at < accepted_at
        {
            eyre::bail!("prior_tree.deleted_at is before accepted_at");
        }

        Ok(Self {
            path,
            claimed_path,
            claim_id,
            object_id,
            fingerprint,
            accepted_at,
            deleted_at,
        })
    }
}
