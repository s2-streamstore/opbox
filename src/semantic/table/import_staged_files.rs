use crate::crdt::types::{NamespaceClaimId, ObjectId};
use crate::fs::types::{
    FileContentFingerprint, FileFingerprint, FileHash, FileKey, FileStatFingerprint, RelativePath,
};
use crate::semantic::table::datetime_from_unix_ns;
use crate::semantic::types::ImportEpoch;
use bytes::Bytes;
use std::str::FromStr;
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr, strum::EnumString)]
pub enum StageKind {
    #[strum(serialize = "new")]
    New,
    #[strum(serialize = "update")]
    Update,
}

#[derive(Debug)]
pub struct Row {
    pub import_epoch: ImportEpoch,
    pub action_seq: u64,
    pub path: RelativePath,
    pub object_id: ObjectId,
    pub claim_id: NamespaceClaimId,
    pub stage_kind: StageKind,
    pub fingerprint: FileFingerprint,
    pub prior_doc_blob: Bytes,
    pub text_update: Bytes,
    pub staged_at: OffsetDateTime,
}

impl Row {
    pub fn from_sql_row(row: &turso::Row) -> eyre::Result<Self> {
        let import_epoch_raw = row.get::<i64>(0)?;
        let action_seq_raw = row.get::<i64>(1)?;
        let path_raw = row.get::<String>(2)?;
        let object_id = ObjectId(Bytes::from(row.get::<Vec<u8>>(3)?));
        let claim_id = NamespaceClaimId(Bytes::from(row.get::<Vec<u8>>(4)?));
        let stage_kind_raw = row.get::<String>(5)?;
        let file_key_raw = row.get::<String>(6)?;
        let size_bytes_raw = row.get::<i64>(7)?;
        let mtime_ns = row.get::<i64>(8)?;
        let hash = Bytes::from(row.get::<Vec<u8>>(9)?);
        let prior_doc_blob = Bytes::from(row.get::<Vec<u8>>(10)?);
        let text_update = Bytes::from(row.get::<Vec<u8>>(11)?);
        let staged_at_ns = row.get::<i64>(12)?;

        if object_id.0.is_empty() {
            eyre::bail!("import_staged_files.object_id missing");
        }
        if claim_id.0.is_empty() {
            eyre::bail!("import_staged_files.claim_id missing");
        }
        if file_key_raw.is_empty() {
            eyre::bail!("import_staged_files.file_key missing");
        }
        if hash.is_empty() {
            eyre::bail!("import_staged_files.hash missing");
        }

        let import_epoch = u64::try_from(import_epoch_raw)
            .map(ImportEpoch::new)
            .map_err(|_| eyre::eyre!("import_staged_files.import_epoch negative"))?;
        let action_seq = u64::try_from(action_seq_raw)
            .map_err(|_| eyre::eyre!("import_staged_files.action_seq negative"))?;
        let path = RelativePath::parse(&path_raw)?;
        let stage_kind = StageKind::from_str(&stage_kind_raw)
            .map_err(|_| eyre::eyre!("invalid import_staged_files.stage_kind: {stage_kind_raw}"))?;
        let file_key = FileKey::decode(&file_key_raw)?;
        let size_bytes = u64::try_from(size_bytes_raw).map_err(|_| {
            eyre::eyre!("import_staged_files.size_bytes negative: {size_bytes_raw}")
        })?;
        let stat = FileStatFingerprint::new(
            file_key,
            size_bytes,
            datetime_from_unix_ns("import_staged_files.mtime_ns", mtime_ns)?,
        );
        let fingerprint = FileFingerprint::StatAndContent {
            stat,
            content: FileContentFingerprint::new(FileHash::new(hash)),
        };

        Ok(Self {
            import_epoch,
            action_seq,
            path,
            object_id,
            claim_id,
            stage_kind,
            fingerprint,
            prior_doc_blob,
            text_update,
            staged_at: datetime_from_unix_ns("import_staged_files.staged_at_ns", staged_at_ns)?,
        })
    }
}
