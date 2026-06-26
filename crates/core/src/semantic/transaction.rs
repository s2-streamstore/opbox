use crate::crdt::types::{ObjectId, ObjectKind, SharedMessageKind};
use crate::fs::types::{FileFingerprint, FileKey, RelativePath};
use crate::log::types::SequenceNumber;
use crate::semantic::service::TursoConnectionManager;
use crate::semantic::table::{
    datetime_to_unix_ns, import_staged_files, outbox, prior_namespace, prior_tree,
    stable_namespace, stable_tree,
};
use crate::semantic::types::ImportEpoch;
use crate::types::{DaemonWriterId, OutboxId};
use bb8::PooledConnection;
use bytes::Bytes;
use eyre::eyre;
use std::ops::{RangeInclusive, RangeTo};
use time::OffsetDateTime;

pub struct SemanticTransaction<'a> {
    conn: &'a PooledConnection<'a, TursoConnectionManager>,
}

#[derive(Debug)]
pub enum SemanticTransactionError {
    Fatal(eyre::Report),
    InvariantViolation(String),
    TursoRetryable(String),
}

impl From<turso::Error> for SemanticTransactionError {
    fn from(err: turso::Error) -> Self {
        match err {
            turso::Error::Busy(reason) | turso::Error::BusySnapshot(reason) => {
                Self::TursoRetryable(reason)
            }
            turso::Error::Error(reason) if is_retryable_turso_error(&reason) => {
                Self::TursoRetryable(reason)
            }
            err => Self::Fatal(eyre!("fatal turso: {err}")),
        }
    }
}

fn is_retryable_turso_error(reason: &str) -> bool {
    // Turso currently maps some MVCC retry conditions through the generic
    // error variant, so classify by the stable core error strings here.
    matches!(
        reason,
        "Write-write conflict" | "Commit dependency aborted" | "Transaction terminated"
    )
}

impl From<eyre::Report> for SemanticTransactionError {
    fn from(err: eyre::Report) -> Self {
        Self::Fatal(err)
    }
}

impl SemanticTransactionError {
    pub fn into_report(self) -> eyre::Report {
        match self {
            Self::Fatal(err) => err,
            Self::InvariantViolation(reason) => eyre!("invariant violation: {reason}"),
            Self::TursoRetryable(reason) => eyre!("retryable turso conflict: {reason}"),
        }
    }
}

impl<'a> SemanticTransaction<'a> {
    pub async fn begin(
        conn: &'a PooledConnection<'a, TursoConnectionManager>,
    ) -> turso::Result<Self> {
        conn.execute("BEGIN CONCURRENT", ()).await?;
        Ok(Self { conn })
    }

    pub async fn commit(&self) -> turso::Result<()> {
        self.conn.execute("COMMIT", ()).await?;
        Ok(())
    }

    pub async fn rollback(&self) -> turso::Result<()> {
        self.conn.execute("ROLLBACK", ()).await?;
        Ok(())
    }

    pub async fn ping(&self) -> eyre::Result<()> {
        let _rows = self.conn.query("SELECT 1", ()).await?;
        Ok(())
    }

    pub async fn select_stable_cursor(
        &self,
    ) -> Result<RangeTo<SequenceNumber>, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query("SELECT stable_cursor FROM daemon_state WHERE id = 1", ())
            .await?;
        let row = rows.next().await?.ok_or_else(|| {
            SemanticTransactionError::InvariantViolation("missing singleton row".to_string())
        })?;
        let cursor = u64::try_from(row.get::<i64>(0)?).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!("invalid stable_cursor: {err}"))
        })?;

        Ok(..cursor)
    }

    pub async fn update_stable_cursor(
        &self,
        stable: RangeInclusive<SequenceNumber>,
    ) -> Result<(), SemanticTransactionError> {
        let rows = self
            .conn
            .execute(
                "UPDATE daemon_state SET stable_cursor = ?1 WHERE id = 1 AND stable_cursor = ?2",
                (stable.end() + 1, *stable.start()),
            )
            .await?;
        if rows != 1 {
            return Err(SemanticTransactionError::InvariantViolation(
                "update stable_cursor failed".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn select_daemon_writer_id(
        &self,
    ) -> Result<DaemonWriterId, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query("SELECT writer_id FROM daemon_state WHERE id = 1", ())
            .await?;
        let row = rows.next().await?.ok_or_else(|| {
            SemanticTransactionError::InvariantViolation("missing daemon_state row".to_string())
        })?;
        let writer_id = DaemonWriterId(Bytes::from(row.get::<Vec<u8>>(0)?));
        if writer_id.0.is_empty() {
            return Err(SemanticTransactionError::InvariantViolation(
                "daemon_state.writer_id missing".to_string(),
            ));
        }
        Ok(writer_id)
    }

    pub async fn select_stable_namespace(
        &self,
    ) -> Result<Option<stable_namespace::Row>, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query(
                "SELECT id, doc_blob, updated_at_ns FROM stable_namespace WHERE id = 1",
                (),
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(stable_namespace::Row::from_sql_row(&row)?))
    }

    pub async fn select_prior_namespace(
        &self,
    ) -> Result<Option<prior_namespace::Row>, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query(
                "SELECT id, doc_blob, accepted_at_ns FROM prior_namespace WHERE id = 1",
                (),
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(prior_namespace::Row::from_sql_row(&row)?))
    }

    pub async fn insert_stable_namespace(
        &self,
        doc_blob: Bytes,
        updated_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let updated_at_ns = datetime_to_unix_ns("stable_namespace.updated_at", updated_at)?;
        self.conn
            .execute(
                "INSERT INTO stable_namespace (id, doc_blob, updated_at_ns)
                 VALUES (1, ?1, ?2)",
                (doc_blob.as_ref(), updated_at_ns),
            )
            .await?;
        Ok(())
    }

    pub async fn insert_prior_namespace(
        &self,
        doc_blob: Bytes,
        accepted_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let accepted_at_ns = datetime_to_unix_ns("prior_namespace.accepted_at", accepted_at)?;
        self.conn
            .execute(
                "INSERT INTO prior_namespace (id, doc_blob, accepted_at_ns)
                 VALUES (1, ?1, ?2)",
                (doc_blob.as_ref(), accepted_at_ns),
            )
            .await?;
        Ok(())
    }

    pub async fn update_stable_namespace(
        &self,
        doc_blob: Bytes,
        updated_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let updated_at_ns = datetime_to_unix_ns("stable_namespace.updated_at", updated_at)?;
        let rows = self
            .conn
            .execute(
                "UPDATE stable_namespace
                 SET doc_blob = ?1, updated_at_ns = ?2
                 WHERE id = 1",
                (doc_blob.as_ref(), updated_at_ns),
            )
            .await?;
        if rows != 1 {
            return Err(SemanticTransactionError::InvariantViolation(
                "update stable_namespace failed".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn update_prior_namespace(
        &self,
        doc_blob: Bytes,
        accepted_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let accepted_at_ns = datetime_to_unix_ns("prior_namespace.accepted_at", accepted_at)?;
        let rows = self
            .conn
            .execute(
                "UPDATE prior_namespace
                 SET doc_blob = ?1, accepted_at_ns = ?2
                 WHERE id = 1",
                (doc_blob.as_ref(), accepted_at_ns),
            )
            .await?;
        if rows != 1 {
            return Err(SemanticTransactionError::InvariantViolation(
                "update prior_namespace failed".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn insert_object(
        &self,
        object_id: &ObjectId,
        object_kind: ObjectKind,
        creator_writer_id: &DaemonWriterId,
        created_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let object_kind: &'static str = object_kind.into();
        let created_at_ns = datetime_to_unix_ns("objects.created_at", created_at)?;
        self.conn
            .execute(
                "INSERT INTO objects (
                    object_id,
                    object_kind,
                    creator_writer_id,
                    created_at_ns
                ) VALUES (?1, ?2, ?3, ?4)",
                (
                    object_id.0.as_ref(),
                    object_kind,
                    creator_writer_id.0.as_ref(),
                    created_at_ns,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn insert_or_ignore_object(
        &self,
        object_id: &ObjectId,
        object_kind: ObjectKind,
        creator_writer_id: &DaemonWriterId,
        created_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let object_kind: &'static str = object_kind.into();
        let created_at_ns = datetime_to_unix_ns("objects.created_at", created_at)?;
        self.conn
            .execute(
                "INSERT OR IGNORE INTO objects (
                    object_id,
                    object_kind,
                    creator_writer_id,
                    created_at_ns
                ) VALUES (?1, ?2, ?3, ?4)",
                (
                    object_id.0.as_ref(),
                    object_kind,
                    creator_writer_id.0.as_ref(),
                    created_at_ns,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn insert_stable_text_object(
        &self,
        object_id: &ObjectId,
        doc_blob: Bytes,
        updated_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let updated_at_ns = datetime_to_unix_ns("stable_text_objects.updated_at", updated_at)?;
        self.conn
            .execute(
                "INSERT INTO stable_text_objects (object_id, doc_blob, updated_at_ns)
                 VALUES (?1, ?2, ?3)",
                (object_id.0.as_ref(), doc_blob.as_ref(), updated_at_ns),
            )
            .await?;
        Ok(())
    }

    pub async fn upsert_stable_text_object(
        &self,
        object_id: &ObjectId,
        doc_blob: Bytes,
        updated_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let updated_at_ns = datetime_to_unix_ns("stable_text_objects.updated_at", updated_at)?;
        self.conn
            .execute(
                "INSERT INTO stable_text_objects (object_id, doc_blob, updated_at_ns)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(object_id) DO UPDATE SET
                    doc_blob = excluded.doc_blob,
                    updated_at_ns = excluded.updated_at_ns",
                (object_id.0.as_ref(), doc_blob.as_ref(), updated_at_ns),
            )
            .await?;
        Ok(())
    }

    pub async fn select_stable_text_object_doc(
        &self,
        object_id: &ObjectId,
    ) -> Result<Option<Bytes>, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query(
                "SELECT doc_blob FROM stable_text_objects WHERE object_id = ?1",
                (object_id.0.as_ref(),),
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(Bytes::from(row.get::<Vec<u8>>(0)?)))
    }

    pub async fn select_stable_tree_files(
        &self,
    ) -> Result<Vec<stable_tree::Row>, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query(
                "SELECT path, claimed_path, claim_id, object_id, updated_at_ns
                 FROM stable_tree
                 ORDER BY path ASC",
                (),
            )
            .await?;

        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(stable_tree::Row::from_sql_row(&row)?);
        }
        Ok(result)
    }

    pub async fn select_stable_tree_file(
        &self,
        path: &RelativePath,
    ) -> Result<Option<stable_tree::Row>, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query(
                "SELECT path, claimed_path, claim_id, object_id, updated_at_ns
                 FROM stable_tree
                 WHERE path = ?1",
                (path.to_db_path(),),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(stable_tree::Row::from_sql_row(&row)?))
    }

    pub async fn select_prior_tree_live_files(
        &self,
    ) -> Result<Vec<prior_tree::Row>, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query(
                "SELECT
                    path,
                    claimed_path,
                    claim_id,
                    object_id,
                    file_key,
                    size_bytes,
                    mtime_ns,
                    hash,
                    accepted_at_ns,
                    deleted_at_ns
                 FROM prior_tree
                 WHERE deleted_at_ns IS NULL
                 ORDER BY path ASC",
                (),
            )
            .await?;

        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(prior_tree::Row::from_sql_row(&row)?);
        }
        Ok(result)
    }

    pub async fn select_prior_tree_live_file(
        &self,
        path: &RelativePath,
    ) -> Result<Option<prior_tree::Row>, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query(
                "SELECT
                    path,
                    claimed_path,
                    claim_id,
                    object_id,
                    file_key,
                    size_bytes,
                    mtime_ns,
                    hash,
                    accepted_at_ns,
                    deleted_at_ns
                 FROM prior_tree
                 WHERE path = ?1 AND deleted_at_ns IS NULL",
                (path.to_db_path(),),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let result = prior_tree::Row::from_sql_row(&row)?;
        if rows.next().await?.is_some() {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "multiple live prior_tree rows for path {path}"
            )));
        }
        Ok(Some(result))
    }

    pub async fn select_recent_tombstoned_prior_tree_file(
        &self,
        path: &RelativePath,
        cutoff: OffsetDateTime,
    ) -> Result<Option<prior_tree::Row>, SemanticTransactionError> {
        let cutoff_ns = datetime_to_unix_ns("prior_tree.deleted_at cutoff", cutoff)?;
        let mut rows = self
            .conn
            .query(
                "SELECT
                    path,
                    claimed_path,
                    claim_id,
                    object_id,
                    file_key,
                    size_bytes,
                    mtime_ns,
                    hash,
                    accepted_at_ns,
                    deleted_at_ns
                 FROM prior_tree
                 WHERE path = ?1
                    AND deleted_at_ns IS NOT NULL
                    AND deleted_at_ns >= ?2
                 ORDER BY deleted_at_ns DESC
                 LIMIT 1",
                (path.to_db_path(), cutoff_ns),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(prior_tree::Row::from_sql_row(&row)?))
    }

    pub async fn select_recent_tombstoned_prior_tree_file_by_file_key(
        &self,
        file_key: &FileKey,
        cutoff: OffsetDateTime,
    ) -> Result<Option<prior_tree::Row>, SemanticTransactionError> {
        let cutoff_ns = datetime_to_unix_ns("prior_tree.deleted_at cutoff", cutoff)?;
        let mut rows = self
            .conn
            .query(
                "SELECT
                    path,
                    claimed_path,
                    claim_id,
                    object_id,
                    file_key,
                    size_bytes,
                    mtime_ns,
                    hash,
                    accepted_at_ns,
                    deleted_at_ns
                 FROM prior_tree
                 WHERE file_key = ?1
                    AND deleted_at_ns IS NOT NULL
                    AND deleted_at_ns >= ?2
                 ORDER BY deleted_at_ns DESC, path ASC
                 LIMIT 1",
                (file_key.encode(), cutoff_ns),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(prior_tree::Row::from_sql_row(&row)?))
    }

    pub async fn insert_prior_text_object(
        &self,
        object_id: &ObjectId,
        doc_blob: Bytes,
        accepted_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let accepted_at_ns = datetime_to_unix_ns("prior_text_objects.accepted_at", accepted_at)?;
        self.conn
            .execute(
                "INSERT INTO prior_text_objects (object_id, doc_blob, accepted_at_ns)
                 VALUES (?1, ?2, ?3)",
                (object_id.0.as_ref(), doc_blob.as_ref(), accepted_at_ns),
            )
            .await?;
        Ok(())
    }

    pub async fn select_prior_text_object_doc(
        &self,
        object_id: &ObjectId,
    ) -> Result<Option<Bytes>, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query(
                "SELECT doc_blob FROM prior_text_objects WHERE object_id = ?1",
                (object_id.0.as_ref(),),
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(Bytes::from(row.get::<Vec<u8>>(0)?)))
    }

    pub async fn upsert_prior_text_object(
        &self,
        object_id: &ObjectId,
        doc_blob: Bytes,
        accepted_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let accepted_at_ns = datetime_to_unix_ns("prior_text_objects.accepted_at", accepted_at)?;
        self.conn
            .execute(
                "INSERT INTO prior_text_objects (object_id, doc_blob, accepted_at_ns)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(object_id) DO UPDATE SET
                    doc_blob = excluded.doc_blob,
                    accepted_at_ns = excluded.accepted_at_ns",
                (object_id.0.as_ref(), doc_blob.as_ref(), accepted_at_ns),
            )
            .await?;
        Ok(())
    }

    pub async fn insert_stable_tree_file(
        &self,
        path: &RelativePath,
        claimed_path: &RelativePath,
        claim_id: &crate::crdt::types::NamespaceClaimId,
        object_id: &ObjectId,
        updated_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let updated_at_ns = datetime_to_unix_ns("stable_tree.updated_at", updated_at)?;
        self.conn
            .execute(
                "INSERT INTO stable_tree (
                    path,
                    claimed_path,
                    claim_id,
                    object_id,
                    updated_at_ns
                ) VALUES (?1, ?2, ?3, ?4, ?5)",
                (
                    path.to_db_path(),
                    claimed_path.to_db_path(),
                    claim_id.0.as_ref(),
                    object_id.0.as_ref(),
                    updated_at_ns,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn delete_stable_tree(&self) -> Result<(), SemanticTransactionError> {
        self.conn.execute("DELETE FROM stable_tree", ()).await?;
        Ok(())
    }

    pub async fn delete_stable_tree_file(
        &self,
        path: &RelativePath,
    ) -> Result<(), SemanticTransactionError> {
        self.conn
            .execute(
                "DELETE FROM stable_tree WHERE path = ?1",
                (path.to_db_path(),),
            )
            .await?;
        Ok(())
    }

    pub async fn insert_prior_tree_file(
        &self,
        path: &RelativePath,
        claimed_path: &RelativePath,
        claim_id: &crate::crdt::types::NamespaceClaimId,
        object_id: &ObjectId,
        fingerprint: &crate::fs::types::FileFingerprint,
        accepted_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let FileFingerprintParts {
            file_key,
            size_bytes,
            mtime_ns,
            hash,
        } = staged_file_fingerprint_parts(fingerprint)?;
        let size_bytes = i64::try_from(size_bytes).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "prior_tree.size_bytes out of range: {err}"
            ))
        })?;
        let accepted_at_ns = datetime_to_unix_ns("prior_tree.accepted_at", accepted_at)?;
        self.conn
            .execute(
                "INSERT INTO prior_tree (
                    path,
                    claimed_path,
                    claim_id,
                    object_id,
                    file_key,
                    size_bytes,
                    mtime_ns,
                    hash,
                    accepted_at_ns,
                    deleted_at_ns
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL)",
                (
                    path.to_db_path(),
                    claimed_path.to_db_path(),
                    claim_id.0.as_ref(),
                    object_id.0.as_ref(),
                    file_key,
                    size_bytes,
                    mtime_ns,
                    hash.as_ref(),
                    accepted_at_ns,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn upsert_prior_tree_file(
        &self,
        path: &RelativePath,
        claimed_path: &RelativePath,
        claim_id: &crate::crdt::types::NamespaceClaimId,
        object_id: &ObjectId,
        fingerprint: &crate::fs::types::FileFingerprint,
        accepted_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let FileFingerprintParts {
            file_key,
            size_bytes,
            mtime_ns,
            hash,
        } = staged_file_fingerprint_parts(fingerprint)?;
        let size_bytes = i64::try_from(size_bytes).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "prior_tree.size_bytes out of range: {err}"
            ))
        })?;
        let accepted_at_ns = datetime_to_unix_ns("prior_tree.accepted_at", accepted_at)?;
        self.conn
            .execute(
                "INSERT INTO prior_tree (
                    path,
                    claimed_path,
                    claim_id,
                    object_id,
                    file_key,
                    size_bytes,
                    mtime_ns,
                    hash,
                    accepted_at_ns,
                    deleted_at_ns
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL)
                ON CONFLICT(path) DO UPDATE SET
                    claimed_path = excluded.claimed_path,
                    claim_id = excluded.claim_id,
                    object_id = excluded.object_id,
                    file_key = excluded.file_key,
                    size_bytes = excluded.size_bytes,
                    mtime_ns = excluded.mtime_ns,
                    hash = excluded.hash,
                    accepted_at_ns = excluded.accepted_at_ns,
                    deleted_at_ns = NULL",
                (
                    path.to_db_path(),
                    claimed_path.to_db_path(),
                    claim_id.0.as_ref(),
                    object_id.0.as_ref(),
                    file_key,
                    size_bytes,
                    mtime_ns,
                    hash.as_ref(),
                    accepted_at_ns,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn tombstone_prior_tree_file(
        &self,
        path: &RelativePath,
        deleted_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let deleted_at_ns = datetime_to_unix_ns("prior_tree.deleted_at", deleted_at)?;
        let rows = self
            .conn
            .execute(
                "UPDATE prior_tree
                 SET deleted_at_ns = ?2
                 WHERE path = ?1 AND deleted_at_ns IS NULL",
                (path.to_db_path(), deleted_at_ns),
            )
            .await?;
        if rows > 1 {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "tombstone_prior_tree_file updated too many rows for path {path}: {rows}"
            )));
        }
        Ok(())
    }

    pub async fn upsert_projection_write_intent(
        &self,
        path: &RelativePath,
        target_hash: &Bytes,
        object_id: &ObjectId,
        target_doc_blob: &Bytes,
        created_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let created_at_ns = datetime_to_unix_ns("projection_write_intents.created_at", created_at)?;
        self.conn
            .execute(
                "INSERT INTO projection_write_intents (
                    path, target_hash, object_id, target_doc_blob, created_at_ns
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT (path, target_hash) DO UPDATE SET
                    object_id = excluded.object_id,
                    target_doc_blob = excluded.target_doc_blob,
                    created_at_ns = excluded.created_at_ns",
                (
                    path.to_db_path(),
                    target_hash.as_ref(),
                    object_id.0.as_ref(),
                    target_doc_blob.as_ref(),
                    created_at_ns,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn mark_projection_write_intent_applied(
        &self,
        path: &RelativePath,
        fingerprint: &FileFingerprint,
    ) -> Result<(), SemanticTransactionError> {
        let FileFingerprintParts {
            file_key,
            size_bytes,
            mtime_ns,
            hash,
        } = staged_file_fingerprint_parts(fingerprint)?;
        let size_bytes = i64::try_from(size_bytes).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "projection_applied_write_intents.size_bytes out of range: {err}"
            ))
        })?;

        // A planned write intent only becomes self-echo evidence after FsActor
        // reports the exact fingerprint that landed on disk. If the planned
        // intent was already removed, this intentionally records nothing.
        self.conn
            .execute(
                "INSERT OR REPLACE INTO projection_applied_write_intents (
                    path, target_hash, file_key, size_bytes, mtime_ns
                )
                 SELECT path, target_hash, ?3, ?4, ?5
                 FROM projection_write_intents
                 WHERE path = ?1 AND target_hash = ?2",
                (
                    path.to_db_path(),
                    hash.as_ref(),
                    file_key,
                    size_bytes,
                    mtime_ns,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn select_applied_projection_write_intent(
        &self,
        path: &RelativePath,
        fingerprint: &FileFingerprint,
    ) -> Result<Option<(ObjectId, Bytes)>, SemanticTransactionError> {
        let FileFingerprintParts {
            file_key,
            size_bytes,
            mtime_ns,
            hash,
        } = staged_file_fingerprint_parts(fingerprint)?;
        let size_bytes = i64::try_from(size_bytes).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "projection_applied_write_intents.size_bytes out of range: {err}"
            ))
        })?;

        let mut rows = self
            .conn
            .query(
                "SELECT p.object_id, p.target_doc_blob
                 FROM projection_write_intents p
                 JOIN projection_applied_write_intents a
                   ON a.path = p.path AND a.target_hash = p.target_hash
                 WHERE p.path = ?1
                   AND p.target_hash = ?2
                   AND a.file_key = ?3
                   AND a.size_bytes = ?4
                   AND a.mtime_ns = ?5",
                (
                    path.to_db_path(),
                    hash.as_ref(),
                    file_key,
                    size_bytes,
                    mtime_ns,
                ),
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let object_id = ObjectId(Bytes::from(row.get::<Vec<u8>>(0)?));
        let target_doc_blob = Bytes::from(row.get::<Vec<u8>>(1)?);
        Ok(Some((object_id, target_doc_blob)))
    }

    pub async fn delete_projection_write_intents_for_path(
        &self,
        path: &RelativePath,
    ) -> Result<(), SemanticTransactionError> {
        self.conn
            .execute(
                "DELETE FROM projection_applied_write_intents WHERE path = ?1",
                (path.to_db_path(),),
            )
            .await?;
        self.conn
            .execute(
                "DELETE FROM projection_write_intents WHERE path = ?1",
                (path.to_db_path(),),
            )
            .await?;
        Ok(())
    }

    pub async fn insert_import_staged_file(
        &self,
        row: &import_staged_files::Row,
    ) -> Result<(), SemanticTransactionError> {
        let import_epoch = i64::try_from(row.import_epoch.get()).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "import_staged_files.import_epoch out of range: {err}"
            ))
        })?;
        let action_seq = i64::try_from(row.action_seq).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "import_staged_files.action_seq out of range: {err}"
            ))
        })?;
        let FileFingerprintParts {
            file_key,
            size_bytes,
            mtime_ns,
            hash,
        } = staged_file_fingerprint_parts(&row.fingerprint)?;
        let size_bytes = i64::try_from(size_bytes).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "import_staged_files.size_bytes out of range: {err}"
            ))
        })?;
        let staged_at_ns = datetime_to_unix_ns("import_staged_files.staged_at", row.staged_at)?;

        self.conn
            .execute(
                "INSERT INTO import_staged_files (
                    import_epoch,
                    action_seq,
                    path,
                    object_id,
                    claim_id,
                    stage_kind,
                    file_key,
                    size_bytes,
                    mtime_ns,
                    hash,
                    prior_doc_blob,
                    text_update,
                    staged_at_ns
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                (
                    import_epoch,
                    action_seq,
                    row.path.to_db_path(),
                    row.object_id.0.as_ref(),
                    row.claim_id.0.as_ref(),
                    Into::<&'static str>::into(row.stage_kind),
                    file_key,
                    size_bytes,
                    mtime_ns,
                    hash.as_ref(),
                    row.prior_doc_blob.as_ref(),
                    row.text_update.as_ref(),
                    staged_at_ns,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn reserve_outbox_ids(
        &self,
        count: u64,
    ) -> Result<Option<OutboxId>, SemanticTransactionError> {
        if count == 0 {
            return Ok(None);
        }
        let count_i64 = i64::try_from(count).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "outbox id allocation count out of range: {err}"
            ))
        })?;
        let mut rows = self
            .conn
            .query("SELECT next_outbox_id FROM daemon_state WHERE id = 1", ())
            .await?;
        let row = rows.next().await?.ok_or_else(|| {
            SemanticTransactionError::InvariantViolation("missing daemon_state row".to_string())
        })?;
        let first_raw = row.get::<i64>(0)?;
        let first = u64::try_from(first_raw).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "daemon_state.next_outbox_id out of range: {err}"
            ))
        })?;
        let next_raw = first_raw.checked_add(count_i64).ok_or_else(|| {
            SemanticTransactionError::InvariantViolation("next_outbox_id overflow".to_string())
        })?;
        let rows = self
            .conn
            .execute(
                "UPDATE daemon_state SET next_outbox_id = ?1 WHERE id = 1 AND next_outbox_id = ?2",
                (next_raw, first_raw),
            )
            .await?;
        if rows != 1 {
            return Err(SemanticTransactionError::InvariantViolation(
                "reserve outbox ids failed".to_string(),
            ));
        }
        Ok(Some(OutboxId::new(first)))
    }

    pub async fn insert_outbox_message(
        &self,
        outbox_id: OutboxId,
        record_kind: SharedMessageKind,
        object_id: Option<&ObjectId>,
        payload: Bytes,
        created_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let record_kind: &'static str = record_kind.into();
        let created_at_ns = datetime_to_unix_ns("outbox.created_at", created_at)?;
        self.conn
            .execute(
                "INSERT INTO outbox (
                    outbox_id,
                    record_kind,
                    object_id,
                    payload,
                    created_at_ns,
                    inflight
                ) VALUES (?1, ?2, ?3, ?4, ?5, 0)",
                (
                    i64::try_from(outbox_id.get()).map_err(|err| {
                        SemanticTransactionError::InvariantViolation(format!(
                            "outbox_id out of range: {err}"
                        ))
                    })?,
                    record_kind,
                    object_id.map(|object_id| object_id.0.to_vec()),
                    payload.as_ref(),
                    created_at_ns,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn reserve_outbox_messages(
        &self,
        limit: u64,
    ) -> Result<Vec<(OutboxId, crate::crdt::types::SharedMessage)>, SemanticTransactionError> {
        let limit = i64::try_from(limit).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "outbox read limit out of range: {err}"
            ))
        })?;
        let mut sql_rows = self
            .conn
            .query(
                "SELECT outbox_id, record_kind, object_id, payload, created_at_ns, inflight
                 FROM outbox
                 WHERE inflight = 0
                 ORDER BY outbox_id ASC
                 LIMIT ?1",
                (limit,),
            )
            .await?;

        let mut rows = Vec::new();
        while let Some(row) = sql_rows.next().await? {
            rows.push(outbox::Row::from_sql_row(&row)?);
        }

        assert_contiguous_outbox_rows(&rows)?;
        let Some(first) = rows.first().map(|row| row.outbox_id) else {
            return Ok(Vec::new());
        };
        let last = rows.last().expect("non-empty rows").outbox_id;

        let updated = self
            .conn
            .execute(
                "UPDATE outbox
                 SET inflight = 1
                 WHERE inflight = 0 AND outbox_id >= ?1 AND outbox_id <= ?2",
                (
                    i64::try_from(first.get()).map_err(|err| {
                        SemanticTransactionError::InvariantViolation(format!(
                            "outbox reserve first id out of range: {err}"
                        ))
                    })?,
                    i64::try_from(last.get()).map_err(|err| {
                        SemanticTransactionError::InvariantViolation(format!(
                            "outbox reserve last id out of range: {err}"
                        ))
                    })?,
                ),
            )
            .await?;
        if updated != rows.len() as u64 {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "reserved {updated} outbox rows, expected {}",
                rows.len()
            )));
        }

        let mut result = Vec::new();
        for row in rows {
            result.push(row.into_shared_message()?);
        }
        Ok(result)
    }

    pub async fn trim_outbox(
        &self,
        through: std::ops::RangeToInclusive<OutboxId>,
    ) -> Result<(), SemanticTransactionError> {
        let end = i64::try_from(through.end.get()).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "trim outbox end out of range: {err}"
            ))
        })?;
        self.conn
            .execute(
                "DELETE FROM outbox WHERE outbox_id <= ?1 AND inflight = 1",
                (end,),
            )
            .await?;
        Ok(())
    }

    pub async fn release_outbox_messages(&self) -> Result<u64, SemanticTransactionError> {
        let released = self
            .conn
            .execute("UPDATE outbox SET inflight = 0 WHERE inflight = 1", ())
            .await?;
        Ok(released)
    }

    #[cfg(feature = "sim")]
    pub async fn count_outbox_inflight(&self) -> Result<u64, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query("SELECT COUNT(*) FROM outbox WHERE inflight = 1", ())
            .await?;
        let row = rows.next().await?.ok_or_else(|| {
            SemanticTransactionError::InvariantViolation(
                "missing outbox inflight count row".to_string(),
            )
        })?;
        let count = u64::try_from(row.get::<i64>(0)?).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "outbox inflight count out of range: {err}"
            ))
        })?;
        Ok(count)
    }

    pub async fn select_import_staged_files(
        &self,
        epoch: ImportEpoch,
    ) -> Result<Vec<import_staged_files::Row>, SemanticTransactionError> {
        let import_epoch = i64::try_from(epoch.get()).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "import_staged_files.import_epoch out of range: {err}"
            ))
        })?;
        let mut rows = self
            .conn
            .query(
                "SELECT
                    import_epoch,
                    action_seq,
                    path,
                    object_id,
                    claim_id,
                    stage_kind,
                    file_key,
                    size_bytes,
                    mtime_ns,
                    hash,
                    prior_doc_blob,
                    text_update,
                    staged_at_ns
                 FROM import_staged_files
                 WHERE import_epoch = ?1
                 ORDER BY action_seq ASC",
                (import_epoch,),
            )
            .await?;

        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(import_staged_files::Row::from_sql_row(&row)?);
        }
        Ok(result)
    }

    pub async fn count_import_staged_files(
        &self,
        epoch: ImportEpoch,
    ) -> Result<u64, SemanticTransactionError> {
        let import_epoch = i64::try_from(epoch.get()).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "import_staged_files.import_epoch out of range: {err}"
            ))
        })?;
        let mut rows = self
            .conn
            .query(
                "SELECT COUNT(*) FROM import_staged_files WHERE import_epoch = ?1",
                (import_epoch,),
            )
            .await?;
        let row = rows.next().await?.ok_or_else(|| {
            SemanticTransactionError::InvariantViolation(
                "import_staged_files count returned no rows".to_string(),
            )
        })?;
        let count = row.get::<i64>(0)?;
        u64::try_from(count).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "import_staged_files count out of range {count}: {err}"
            ))
        })
    }

    pub async fn delete_import_staged_files(
        &self,
        epoch: ImportEpoch,
    ) -> Result<(), SemanticTransactionError> {
        let import_epoch = i64::try_from(epoch.get()).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "import_staged_files.import_epoch out of range: {err}"
            ))
        })?;
        self.conn
            .execute(
                "DELETE FROM import_staged_files WHERE import_epoch = ?1",
                (import_epoch,),
            )
            .await?;
        Ok(())
    }

    // -- ignored_files -----------------------------------------------------------

    pub async fn select_ignored_file(
        &self,
        path: &RelativePath,
    ) -> Result<Option<super::table::ignored_files::Row>, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query(
                "SELECT path, reason, file_key, size_bytes, mtime_ns, ignored_at_ns
                 FROM ignored_files
                 WHERE path = ?1",
                (path.to_db_path(),),
            )
            .await?;

        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        Ok(Some(super::table::ignored_files::Row::from_sql_row(&row)?))
    }

    pub async fn select_all_ignored_files(
        &self,
    ) -> Result<Vec<super::table::ignored_files::Row>, SemanticTransactionError> {
        let mut rows = self
            .conn
            .query(
                "SELECT path, reason, file_key, size_bytes, mtime_ns, ignored_at_ns
                 FROM ignored_files
                 ORDER BY path ASC",
                (),
            )
            .await?;

        let mut result = Vec::new();
        while let Some(row) = rows.next().await? {
            result.push(super::table::ignored_files::Row::from_sql_row(&row)?);
        }
        Ok(result)
    }

    pub async fn upsert_ignored_file(
        &self,
        path: &RelativePath,
        reason: super::table::ignored_files::Reason,
        stat: &crate::fs::types::FileStatFingerprint,
        ignored_at: OffsetDateTime,
    ) -> Result<(), SemanticTransactionError> {
        let mtime_ns = datetime_to_unix_ns("ignored_files.mtime", stat.mtime)?;
        let ignored_at_ns = datetime_to_unix_ns("ignored_files.ignored_at", ignored_at)?;
        self.conn
            .execute(
                "INSERT INTO ignored_files (path, reason, file_key, size_bytes, mtime_ns, ignored_at_ns)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(path) DO UPDATE SET
                     reason = excluded.reason,
                     file_key = excluded.file_key,
                     size_bytes = excluded.size_bytes,
                     mtime_ns = excluded.mtime_ns,
                     ignored_at_ns = excluded.ignored_at_ns",
                (
                    path.to_db_path(),
                    reason.as_str(),
                    stat.file_key.encode(),
                    i64::try_from(stat.size).map_err(|err| {
                        SemanticTransactionError::InvariantViolation(format!(
                            "ignored_files.size_bytes out of range: {err}"
                        ))
                    })?,
                    mtime_ns,
                    ignored_at_ns,
                ),
            )
            .await?;
        Ok(())
    }

    pub async fn delete_ignored_file(
        &self,
        path: &RelativePath,
    ) -> Result<(), SemanticTransactionError> {
        self.conn
            .execute(
                "DELETE FROM ignored_files WHERE path = ?1",
                (path.to_db_path(),),
            )
            .await?;
        Ok(())
    }

    // ---------------------------------------------------------------------------

    pub async fn count_table(&self, table: &'static str) -> Result<u64, SemanticTransactionError> {
        let sql = match table {
            "objects" => "SELECT COUNT(*) FROM objects",
            "stable_namespace" => "SELECT COUNT(*) FROM stable_namespace",
            "prior_namespace" => "SELECT COUNT(*) FROM prior_namespace",
            "stable_text_objects" => "SELECT COUNT(*) FROM stable_text_objects",
            "prior_text_objects" => "SELECT COUNT(*) FROM prior_text_objects",
            "stable_tree" => "SELECT COUNT(*) FROM stable_tree",
            "prior_tree" => "SELECT COUNT(*) FROM prior_tree",
            "import_staged_files" => "SELECT COUNT(*) FROM import_staged_files",
            "outbox" => "SELECT COUNT(*) FROM outbox",
            "ignored_files" => "SELECT COUNT(*) FROM ignored_files",
            _ => {
                return Err(SemanticTransactionError::InvariantViolation(format!(
                    "unsupported table count: {table}"
                )));
            }
        };

        let mut rows = self.conn.query(sql, ()).await?;
        let row = rows.next().await?.ok_or_else(|| {
            SemanticTransactionError::InvariantViolation(format!("{table} count returned no rows"))
        })?;
        let count = row.get::<i64>(0)?;
        u64::try_from(count).map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "{table} count out of range {count}: {err}"
            ))
        })
    }
}

struct FileFingerprintParts {
    file_key: String,
    size_bytes: u64,
    mtime_ns: i64,
    hash: Bytes,
}

fn staged_file_fingerprint_parts(
    fingerprint: &crate::fs::types::FileFingerprint,
) -> Result<FileFingerprintParts, SemanticTransactionError> {
    let crate::fs::types::FileFingerprint::StatAndContent { stat, content } = fingerprint else {
        return Err(SemanticTransactionError::InvariantViolation(
            "import staged file fingerprint must include content hash".to_string(),
        ));
    };
    Ok(FileFingerprintParts {
        file_key: stat.file_key.encode(),
        size_bytes: stat.size,
        mtime_ns: datetime_to_unix_ns("import_staged_files.mtime", stat.mtime)?,
        hash: content.hash.as_bytes().clone(),
    })
}

fn assert_contiguous_outbox_rows(rows: &[outbox::Row]) -> Result<(), SemanticTransactionError> {
    let Some(first) = rows.first() else {
        return Ok(());
    };

    let mut expected = first.outbox_id;
    for row in rows {
        if row.inflight {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "selected outbox row {:?} was already inflight",
                row.outbox_id
            )));
        }
        if row.outbox_id != expected {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "outbox reservation selected non-contiguous rows; expected {:?}, saw {:?}",
                expected, row.outbox_id
            )));
        }
        expected = row.outbox_id.checked_next().ok_or_else(|| {
            SemanticTransactionError::InvariantViolation(
                "outbox id overflow while validating reservation".to_string(),
            )
        })?;
    }

    Ok(())
}
