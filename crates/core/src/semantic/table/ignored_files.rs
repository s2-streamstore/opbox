use crate::fs::types::{FileKey, FileStatFingerprint, RelativePath};
use crate::semantic::table::datetime_from_unix_ns;
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reason {
    NonUtf8,
    PermissionDenied,
    TooLarge,
}

impl Reason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Reason::NonUtf8 => "non_utf8",
            Reason::PermissionDenied => "permission_denied",
            Reason::TooLarge => "too_large",
        }
    }

    pub fn parse(s: &str) -> eyre::Result<Self> {
        match s {
            "non_utf8" => Ok(Reason::NonUtf8),
            "permission_denied" => Ok(Reason::PermissionDenied),
            "too_large" => Ok(Reason::TooLarge),
            other => eyre::bail!("unknown ignored_files reason: {other:?}"),
        }
    }
}

#[derive(Debug)]
pub struct Row {
    pub path: RelativePath,
    pub reason: Reason,
    pub stat: FileStatFingerprint,
    pub ignored_at: OffsetDateTime,
}

impl Row {
    pub fn from_sql_row(row: &turso::Row) -> eyre::Result<Self> {
        let path_raw = row.get::<String>(0)?;
        let reason_raw = row.get::<String>(1)?;
        let file_key_raw = row.get::<String>(2)?;
        let size_bytes_raw = row.get::<i64>(3)?;
        let mtime_ns = row.get::<i64>(4)?;
        let ignored_at_ns = row.get::<i64>(5)?;

        let path = RelativePath::parse(&path_raw)?;
        let reason = Reason::parse(&reason_raw)?;

        if file_key_raw.is_empty() {
            eyre::bail!("ignored_files.file_key missing");
        }
        let file_key = FileKey::decode(&file_key_raw)?;
        let size_bytes = u64::try_from(size_bytes_raw)
            .map_err(|_| eyre::eyre!("ignored_files.size_bytes negative: {size_bytes_raw}"))?;
        let stat = FileStatFingerprint::new(
            file_key,
            size_bytes,
            datetime_from_unix_ns("ignored_files.mtime_ns", mtime_ns)?,
        );
        let ignored_at = datetime_from_unix_ns("ignored_files.ignored_at_ns", ignored_at_ns)?;

        Ok(Self {
            path,
            reason,
            stat,
            ignored_at,
        })
    }
}
