use crate::crdt::types::{
    NamespaceClaimId, ObjectId, ObjectKind, SharedMessage, SharedMessageKind,
};
use crate::crdt::{namespace, text_doc};
use crate::fs::types::{
    ExpectedBefore, FileFingerprint, GuardedDeleteResult, GuardedReadResult, GuardedWriteResult,
};
use crate::fs::types::{ScanResult, TreeEntryKind};
use crate::semantic::actor::{
    ApplyScanOutput, ApplySharedMessageBatchOutput, CommitImportActionOutcome,
    CommitImportEpochOutput, CompletedImportEpoch, CompletedProjectionEpoch, ImportActionContext,
    ImportEpochKind, SemanticEffects,
};
use crate::semantic::table::import_staged_files::{self, StageKind};
use crate::semantic::transaction::{SemanticTransaction, SemanticTransactionError};
use crate::semantic::types::{
    ImportAction, ImportActionId, ImportActionKind, ImportActionResult, ImportEpoch,
    ImportEpochStarted, ImportPlan, ImportReadOutcome, NextWork, ProjectionAction,
    ProjectionActionCommitContext, ProjectionActionId, ProjectionActionKind,
    ProjectionActionResult, ProjectionEpoch, ProjectionEpochEndReason, ProjectionEpochStarted,
    ProjectionGeneration, ProjectionPlan,
};
use crate::types::{OutboxId, SharedMessageBatch};
use bytes::Bytes;
use futures::future::BoxFuture;
use std::collections::BTreeMap;
use std::ops::{RangeTo, RangeToInclusive};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, instrument, trace, warn};
use turso::{Connection, Database};
use xxhash_rust::xxh3::xxh3_64;

const TURSO_RETRY_ATTEMPTS: usize = 25;
const TURSO_RETRY_BASE_DELAY: Duration = Duration::from_millis(10);
const TURSO_RETRY_MAX_DELAY: Duration = Duration::from_millis(250);
const RECENT_TOMBSTONE_REUSE_WINDOW_SECS: i64 = 5 * 60;

#[derive(Debug, Clone, PartialEq, Eq)]
struct DesiredStableTreeFile {
    claimed_path: crate::fs::types::RelativePath,
    claim_id: NamespaceClaimId,
    object_id: ObjectId,
}

#[derive(Clone)]
pub struct SemanticService {
    pool: Arc<bb8::Pool<TursoConnectionManager>>,
}

#[cfg(feature = "sim")]
#[derive(Debug, Clone)]
pub struct SemanticDebugSnapshot {
    pub prior_live_paths: BTreeMap<String, ObjectId>,
    pub stable_paths: BTreeMap<String, ObjectId>,
    pub outbox_rows: u64,
    pub outbox_inflight_rows: u64,
}

impl SemanticService {
    pub fn new(pool: bb8::Pool<TursoConnectionManager>) -> Self {
        Self {
            pool: Arc::new(pool),
        }
    }

    #[cfg(feature = "sim")]
    pub async fn debug_snapshot(&self) -> eyre::Result<SemanticDebugSnapshot> {
        self.exec_tx("debug_snapshot", move |tx| {
            Box::pin(async move {
                let prior_live_paths = tx
                    .select_prior_tree_live_files()
                    .await?
                    .into_iter()
                    .map(|row| (row.path.to_string(), row.object_id))
                    .collect();
                let stable_paths = tx
                    .select_stable_tree_files()
                    .await?
                    .into_iter()
                    .map(|row| (row.path.to_string(), row.object_id))
                    .collect();
                let outbox_rows = tx.count_table("outbox").await?;
                let outbox_inflight_rows = tx.count_outbox_inflight().await?;
                Ok(SemanticDebugSnapshot {
                    prior_live_paths,
                    stable_paths,
                    outbox_rows,
                    outbox_inflight_rows,
                })
            })
        })
        .await
    }

    #[instrument(skip(self, scan))]
    pub(crate) async fn apply_init_scan(
        &self,
        scan: ScanResult,
        epoch: ImportEpoch,
    ) -> eyre::Result<ImportEpochStarted> {
        let scan = Arc::new(scan);
        self.exec_tx("apply_init_scan", move |tx| {
            let scan = scan.clone();
            Box::pin(async move {
                let semantic_tables = [
                    "objects",
                    "stable_text_objects",
                    "prior_text_objects",
                    "stable_tree",
                    "prior_tree",
                    "import_staged_files",
                    "outbox",
                ];
                for table in semantic_tables {
                    let count = tx.count_table(table).await?;
                    if count != 0 {
                        return Err(SemanticTransactionError::InvariantViolation(format!(
                            "init requires empty semantic table {table}, found {count} rows"
                        )));
                    }
                }

                let expected_empty_namespace = namespace::empty_namespace_state(1);
                let stable_namespace = tx.select_stable_namespace().await?.ok_or_else(|| {
                    SemanticTransactionError::InvariantViolation(
                        "missing stable_namespace".to_string(),
                    )
                })?;
                let prior_namespace = tx.select_prior_namespace().await?.ok_or_else(|| {
                    SemanticTransactionError::InvariantViolation(
                        "missing prior_namespace".to_string(),
                    )
                })?;
                if stable_namespace.doc_blob != expected_empty_namespace
                    || prior_namespace.doc_blob != expected_empty_namespace
                {
                    return Err(SemanticTransactionError::InvariantViolation(
                        "init requires empty prior/stable namespaces".to_string(),
                    ));
                }

                let ignored = tx.select_all_ignored_files().await?;
                let ignored_map: BTreeMap<_, _> = ignored
                    .into_iter()
                    .map(|row| (row.path, row.stat))
                    .collect();

                let mut actions = Vec::new();
                let mut next_seq = 0;
                for entry in scan.tree.entries() {
                    match &entry.kind {
                        TreeEntryKind::Directory => {
                            // not handled
                        }
                        TreeEntryKind::File { fingerprint } => {
                            if let Some(ignored_stat) = ignored_map.get(&entry.path) {
                                if fingerprint.stat() == ignored_stat {
                                    continue;
                                }
                            }
                            let action_id = ImportActionId::new(epoch, next_seq);
                            next_seq += 1;
                            actions.push(ImportAction {
                                id: action_id,
                                kind: ImportActionKind::Read {
                                    path: entry.path.clone(),
                                    expected_fingerprint: fingerprint.clone(),
                                },
                            });
                        }
                    }
                }

                Ok(ImportEpochStarted {
                    epoch,
                    plan: ImportPlan { actions },
                })
            })
        })
        .await
    }

    #[instrument(skip(self, scan))]
    pub(crate) async fn apply_scan(
        &self,
        scan: ScanResult,
        epoch: ImportEpoch,
    ) -> eyre::Result<ApplyScanOutput> {
        let scan = Arc::new(scan);
        self.exec_tx("apply_scan", move |tx| {
            let scan = scan.clone();
            Box::pin(async move {
                let mut observed_files = BTreeMap::new();
                for entry in scan.tree.entries() {
                    if !scan.scope.contains_path(&entry.path) {
                        return Err(SemanticTransactionError::InvariantViolation(format!(
                            "scan result for scope {:?} contained out-of-scope path {}",
                            scan.scope, entry.path
                        )));
                    }
                    match &entry.kind {
                        TreeEntryKind::Directory => {}
                        TreeEntryKind::File { fingerprint } => {
                            observed_files.insert(entry.path.clone(), fingerprint.clone());
                        }
                    }
                }

                // Load ignored file entries within this scan's scope so we can
                // skip files whose stat fingerprint hasn't changed since the
                // last failed import attempt.
                let ignored = tx.select_all_ignored_files().await?;
                let ignored_in_scope: BTreeMap<_, _> = ignored
                    .into_iter()
                    .filter(|row| scan.scope.contains_path(&row.path))
                    .map(|row| (row.path, row.stat))
                    .collect();

                let mut actions = Vec::new();
                let mut missing_prior = Vec::new();
                let mut next_seq = 0;

                for prior in tx.select_prior_tree_live_files().await? {
                    if !scan.scope.contains_path(&prior.path) {
                        continue;
                    }
                    match observed_files.remove(&prior.path) {
                        None => missing_prior.push(prior),
                        Some(observed) if observed.stat() == prior.fingerprint.stat() => {}
                        Some(observed) => {
                            // If the file's current stat matches a known ignored
                            // entry, skip re-reading (e.g. text→binary overwrite).
                            if let Some(ignored_stat) = ignored_in_scope.get(&prior.path) {
                                if observed.stat() == ignored_stat {
                                    continue;
                                }
                            }
                            let action_id = ImportActionId::new(epoch, next_seq);
                            next_seq += 1;
                            actions.push(ImportAction {
                                id: action_id,
                                kind: ImportActionKind::Read {
                                    path: prior.path,
                                    expected_fingerprint: observed,
                                },
                            });
                        }
                    }
                }

                for (path, fingerprint) in observed_files {
                    // If this file was previously ignored and its stat
                    // fingerprint hasn't changed, skip it — no point re-reading.
                    if let Some(ignored_stat) = ignored_in_scope.get(&path) {
                        if fingerprint.stat() == ignored_stat {
                            continue;
                        }
                    }
                    let action_id = ImportActionId::new(epoch, next_seq);
                    next_seq += 1;
                    actions.push(ImportAction {
                        id: action_id,
                        kind: ImportActionKind::Read {
                            path,
                            expected_fingerprint: fingerprint,
                        },
                    });
                }

                // Clean up ignored_files entries for files that have been
                // deleted from disk (no longer in the scan result).
                for (ignored_path, _) in &ignored_in_scope {
                    // If the path wasn't observed in the scan AND isn't in
                    // prior_tree (already handled by missing_prior), remove it.
                    // We simply check whether the scan tree contains it.
                    if !scan.tree.entries().iter().any(|e| &e.path == ignored_path) {
                        tx.delete_ignored_file(ignored_path).await?;
                    }
                }

                let delete_outbox_messages =
                    apply_missing_prior_paths_as_local_deletes_tx(tx, missing_prior).await?;
                let next_work = if actions.is_empty() {
                    NextWork::None
                } else {
                    NextWork::Import(ImportEpochStarted {
                        epoch,
                        plan: ImportPlan { actions },
                    })
                };

                Ok(ApplyScanOutput {
                    next_work,
                    effects: SemanticEffects {
                        projection_changed: None,
                        outbox_ready: nonzero_u64(delete_outbox_messages),
                    },
                })
            })
        })
        .await
    }

    pub(crate) async fn commit_import_action(
        &self,
        context: ImportActionContext,
        result: ImportActionResult,
    ) -> eyre::Result<CommitImportActionOutcome> {
        let decision = prepare_import_action_stage(context, result)?;
        let action_id = decision.action_id();

        match decision {
            ImportActionStageDecision::Stage(plan) => {
                let plan = Arc::new(plan);
                self.exec_tx("commit_import_action", move |tx| {
                    let plan = plan.clone();
                    Box::pin(async move {
                        stage_import_action_tx(tx, &plan).await?;
                        // Clear any prior ignored entry for this path on
                        // successful stage (file content changed to valid UTF-8).
                        tx.delete_ignored_file(&plan.path).await?;
                        Ok(())
                    })
                })
                .await?;
            }
            ImportActionStageDecision::IgnorePersist {
                path, stat, reason, ..
            } => {
                let now = time::OffsetDateTime::now_utc();
                self.exec_tx("persist_ignored_file", move |tx| {
                    let path = path.clone();
                    let stat = stat.clone();
                    Box::pin(async move {
                        tx.upsert_ignored_file(&path, reason, &stat, now).await?;
                        Ok(())
                    })
                })
                .await?;
            }
            ImportActionStageDecision::Ignore { .. } => {}
        }

        Ok(CommitImportActionOutcome { action_id })
    }

    pub(crate) async fn commit_import_epoch(
        &self,
        completed: CompletedImportEpoch,
    ) -> eyre::Result<CommitImportEpochOutput> {
        let completed = Arc::new(completed);
        self.exec_tx("commit_import_epoch", move |tx| {
            let completed = completed.clone();
            Box::pin(async move {
                match completed.kind {
                    ImportEpochKind::Init => commit_init_import_epoch_tx(tx, &completed).await,
                    ImportEpochKind::Sync => commit_sync_import_epoch_tx(tx, &completed).await,
                }
            })
        })
        .await
    }

    pub(crate) async fn commit_projection_action(
        &self,
        context: ProjectionActionCommitContext,
        result: ProjectionActionResult,
    ) -> eyre::Result<ProjectionActionResult> {
        let applied_write = match &result {
            ProjectionActionResult::WriteFile {
                result:
                    GuardedWriteResult::Written { fingerprint }
                    | GuardedWriteResult::AlreadyApplied { fingerprint },
                ..
            } => Some((context.path.clone(), fingerprint.clone())),
            _ => None,
        };

        if let Some((path, fingerprint)) = applied_write {
            self.exec_tx("commit_projection_action", move |tx| {
                let path = path.clone();
                let fingerprint = fingerprint.clone();
                Box::pin(async move {
                    tx.mark_projection_write_intent_applied(&path, &fingerprint)
                        .await
                })
            })
            .await?;
        }

        Ok(result)
    }

    pub(crate) async fn commit_projection_epoch(
        &self,
        completed: CompletedProjectionEpoch,
        reason: ProjectionEpochEndReason,
    ) -> eyre::Result<()> {
        let completed = Arc::new(completed);
        self.exec_tx("commit_projection_epoch", move |tx| {
            let completed = completed.clone();
            Box::pin(async move { commit_projection_epoch_tx(tx, &completed, reason).await })
        })
        .await
    }

    pub(crate) async fn get_next_work(
        &self,
        projection_epoch: ProjectionEpoch,
    ) -> eyre::Result<NextWork> {
        self.exec_tx("get_next_work", move |tx| {
            Box::pin(async move { get_next_work_tx(tx, projection_epoch).await })
        })
        .await
    }

    pub(crate) async fn apply_shared_message_batch(
        &self,
        batch: SharedMessageBatch,
    ) -> eyre::Result<ApplySharedMessageBatchOutput> {
        let batch = Arc::new(batch);
        self.exec_tx("apply_shared_message_batch", move |tx| {
            let batch = batch.clone();
            Box::pin(async move { apply_shared_message_batch_tx(tx, &batch).await })
        })
        .await
    }

    pub(crate) async fn read_outbox(
        &self,
        num_messages: u64,
    ) -> eyre::Result<Vec<(OutboxId, SharedMessage)>> {
        self.exec_tx("read_outbox", move |tx| {
            Box::pin(async move { tx.reserve_outbox_messages(num_messages).await })
        })
        .await
    }

    pub(crate) async fn release_outbox(&self) -> eyre::Result<u64> {
        self.exec_tx("release_outbox", move |tx| {
            Box::pin(async move { tx.release_outbox_messages().await })
        })
        .await
    }

    pub(crate) async fn trim_outbox(
        &self,
        through: RangeToInclusive<OutboxId>,
    ) -> eyre::Result<()> {
        self.exec_tx("trim_outbox", move |tx| {
            Box::pin(async move {
                tx.trim_outbox(through).await?;
                Ok(())
            })
        })
        .await
    }

    pub(crate) async fn read_stable_cursor(
        &self,
    ) -> eyre::Result<RangeTo<crate::log::types::SequenceNumber>> {
        self.exec_tx("read_stable_cursor", move |tx| {
            Box::pin(async move { tx.select_stable_cursor().await })
        })
        .await
    }

    pub(crate) async fn read_stable_namespace(&self) -> eyre::Result<Bytes> {
        self.exec_tx("read_stable_namespace", move |tx| {
            Box::pin(async move {
                let stable_namespace = tx.select_stable_namespace().await?.ok_or_else(|| {
                    SemanticTransactionError::InvariantViolation(
                        "missing stable_namespace".to_string(),
                    )
                })?;
                Ok(stable_namespace.doc_blob)
            })
        })
        .await
    }

    async fn exec_tx<T, F>(&self, label: &'static str, mut body: F) -> eyre::Result<T>
    where
        F: for<'tx> FnMut(
            &'tx SemanticTransaction<'tx>,
        ) -> BoxFuture<'tx, Result<T, SemanticTransactionError>>,
    {
        let conn = self.pool.get().await?;
        let mut attempt = 0;

        loop {
            let tx = match SemanticTransaction::begin(&conn)
                .await
                .map_err(SemanticTransactionError::from)
            {
                Ok(tx) => tx,
                Err(SemanticTransactionError::TursoRetryable(reason))
                    if attempt < TURSO_RETRY_ATTEMPTS =>
                {
                    warn!(label, attempt, reason, "retrying semantic tx begin");
                    attempt += 1;
                    tokio::time::sleep(turso_retry_delay(attempt)).await;
                    continue;
                }
                Err(err) => return Err(err.into_report()),
            };

            let body_started = perf_start!();
            let body_result = body(&tx).await;
            let body_ok = body_result.is_ok();
            trace_perf!(
                body_started,
                label,
                attempt,
                ok = body_ok,
                "semantic tx body finished"
            );

            let result = match body_result {
                Ok(value) => {
                    let commit_started = perf_start!();
                    let commit_result = tx.commit().await;
                    let commit_ok = commit_result.is_ok();
                    trace_perf!(
                        commit_started,
                        label,
                        attempt,
                        ok = commit_ok,
                        "semantic tx commit finished"
                    );
                    commit_result
                        .map(|_| value)
                        .map_err(SemanticTransactionError::from)
                }
                Err(err) => Err(err),
            };

            match result {
                Ok(value) => {
                    trace!(label, attempt, "semantic tx committed");
                    return Ok(value);
                }
                Err(SemanticTransactionError::TursoRetryable(reason))
                    if attempt < TURSO_RETRY_ATTEMPTS =>
                {
                    warn!(label, attempt, reason, "retrying semantic tx");
                    attempt += 1;
                    let _ = tx.rollback().await;
                    tokio::time::sleep(turso_retry_delay(attempt)).await;
                }
                Err(err) => {
                    let _ = tx.rollback().await;
                    return Err(err.into_report());
                }
            }
        }
    }
}

fn turso_retry_delay(attempt: usize) -> Duration {
    let multiplier = 1_u32 << attempt.min(5);
    TURSO_RETRY_BASE_DELAY
        .saturating_mul(multiplier)
        .min(TURSO_RETRY_MAX_DELAY)
}

async fn apply_shared_message_batch_tx(
    tx: &SemanticTransaction<'_>,
    batch: &SharedMessageBatch,
) -> Result<ApplySharedMessageBatchOutput, SemanticTransactionError> {
    let total_started = perf_start!();
    let stable_cursor = tx.select_stable_cursor().await?;
    if stable_cursor.end != *batch.sequence_range.start() {
        return Err(SemanticTransactionError::InvariantViolation(format!(
            "unexpected batch; stable_cursor={:?}, batch.sequence_range={:?}",
            stable_cursor, batch.sequence_range
        )));
    }
    let daemon_writer_id = tx.select_daemon_writer_id().await?;

    let mut namespace_doc: Option<namespace::NamespaceDoc> = None;
    let mut text_docs = BTreeMap::<ObjectId, Bytes>::new();
    let mut applied_message_count = 0usize;
    let mut skipped_local_echo_count = 0usize;
    let mut max_applied_timestamp: Option<time::OffsetDateTime> = None;

    for envelope in &batch.messages {
        if envelope.origin.daemon_writer_id == daemon_writer_id {
            skipped_local_echo_count += 1;
            trace!(
                sequence_number = envelope.sequence_number,
                outbox_id = envelope.origin.outbox_id.get(),
                "apply_shared_message_batch skipped local echo"
            );
            continue;
        }

        applied_message_count += 1;
        max_applied_timestamp = Some(match max_applied_timestamp {
            Some(current) => current.max(envelope.timestamp),
            None => envelope.timestamp,
        });
        match &envelope.shared_message {
            SharedMessage::NamespaceUpdate { yjs_update } => {
                let started = perf_start!();
                if namespace_doc.is_none() {
                    let stable_namespace =
                        tx.select_stable_namespace().await?.ok_or_else(|| {
                            SemanticTransactionError::InvariantViolation(
                                "missing stable_namespace".to_string(),
                            )
                        })?;
                    namespace_doc = Some(namespace::NamespaceDoc::from_full_state(
                        1,
                        stable_namespace.doc_blob,
                    )?);
                }
                namespace_doc
                    .as_ref()
                    .expect("namespace doc initialized")
                    .apply_update(yjs_update.as_ref())?;
                trace_perf!(
                    started,
                    sequence_number = envelope.sequence_number,
                    update_bytes = yjs_update.len(),
                    "apply_shared_message_batch namespace message applied"
                );
            }
            SharedMessage::TextObjectUpdate {
                object_id,
                yjs_update,
            } => {
                let started = perf_start!();
                let base = match text_docs.get(object_id) {
                    Some(doc_blob) => doc_blob.clone(),
                    None => tx
                        .select_stable_text_object_doc(object_id)
                        .await?
                        .unwrap_or_else(|| text_doc::empty_text_state(1)),
                };
                let next = text_doc::apply_text_update(base.as_ref(), yjs_update.as_ref())?;
                let next_state_bytes = next.full_state_bytes.len();
                text_docs.insert(object_id.clone(), next.full_state_bytes);
                trace_perf!(
                    started,
                    sequence_number = envelope.sequence_number,
                    object_id = ?object_id,
                    base_state_bytes = base.len(),
                    update_bytes = yjs_update.len(),
                    next_state_bytes,
                    "apply_shared_message_batch text message applied"
                );
            }
            SharedMessage::BinaryObjectPut { .. } => {
                return Err(SemanticTransactionError::InvariantViolation(
                    "binary shared messages are not supported in v0".to_string(),
                ));
            }
        }
    }

    if let Some(namespace_doc) = namespace_doc {
        let updated_at = max_applied_timestamp
            .expect("namespace doc exists only after applying at least one message");
        let started = perf_start!();
        write_stable_namespace_projection_tx(tx, &namespace_doc, updated_at).await?;
        trace_perf!(
            started,
            "apply_shared_message_batch stable namespace projection written"
        );
    }

    if !text_docs.is_empty() {
        let updated_at = max_applied_timestamp
            .expect("text docs exist only after applying at least one message");
        for (object_id, doc_blob) in text_docs {
            let started = perf_start!();
            let doc_blob_bytes = doc_blob.len();
            tx.upsert_stable_text_object(&object_id, doc_blob, updated_at)
                .await?;
            trace_perf!(
                started,
                object_id = ?object_id,
                doc_blob_bytes,
                "apply_shared_message_batch stable text object upserted"
            );
        }
    }

    let cursor_started = perf_start!();
    tx.update_stable_cursor(batch.sequence_range.clone())
        .await?;
    trace_perf!(
        cursor_started,
        sequence_range = ?batch.sequence_range,
        "apply_shared_message_batch stable cursor updated"
    );

    let next_cursor = batch.sequence_range.end().checked_add(1).ok_or_else(|| {
        SemanticTransactionError::InvariantViolation("stable cursor overflow".to_string())
    })?;

    trace_perf!(
        total_started,
        message_count = batch.messages.len(),
        applied_message_count,
        skipped_local_echo_count,
        sequence_range = ?batch.sequence_range,
        "apply_shared_message_batch transaction body complete"
    );

    Ok(ApplySharedMessageBatchOutput {
        effects: SemanticEffects {
            projection_changed: (applied_message_count > 0)
                .then(|| ProjectionGeneration::new(next_cursor)),
            outbox_ready: None,
        },
    })
}

async fn write_stable_namespace_projection_tx(
    tx: &SemanticTransaction<'_>,
    doc: &namespace::NamespaceDoc,
    updated_at: time::OffsetDateTime,
) -> Result<(), SemanticTransactionError> {
    let total_started = perf_start!();
    let materialize_started = perf_start!();
    let projection = doc.materialize()?;
    let confirmed_object_count = projection.confirmed_objects.len();
    let placement_count = projection.placements.len();
    trace_perf!(
        materialize_started,
        confirmed_object_count,
        placement_count,
        "stable namespace materialized"
    );

    let namespace_started = perf_start!();
    tx.update_stable_namespace(doc.encode_full_state(), updated_at)
        .await?;
    trace_perf!(namespace_started, "stable namespace row updated");

    for object in projection.confirmed_objects {
        let started = perf_start!();
        match object.meta.kind {
            ObjectKind::Text => {
                tx.insert_or_ignore_object(
                    &object.object_id,
                    ObjectKind::Text,
                    &object.meta.creator_writer_id,
                    updated_at,
                )
                .await?;
                trace_perf!(
                    started,
                    object_id = ?object.object_id,
                    "stable namespace object ensured"
                );
            }
            other => {
                return Err(SemanticTransactionError::InvariantViolation(format!(
                    "unsupported namespace object kind in v0: {other:?}"
                )));
            }
        }
    }

    let desired_started = perf_start!();
    let mut desired_tree = BTreeMap::new();
    for placement in projection.placements {
        match placement.kind {
            ObjectKind::Text => {
                let previous = desired_tree.insert(
                    placement.path.clone(),
                    DesiredStableTreeFile {
                        claimed_path: placement.claimed_path,
                        claim_id: placement.claim_id,
                        object_id: placement.object_id,
                    },
                );
                if previous.is_some() {
                    return Err(SemanticTransactionError::InvariantViolation(format!(
                        "namespace materialized duplicate stable_tree path {}",
                        placement.path
                    )));
                }
            }
            other => {
                return Err(SemanticTransactionError::InvariantViolation(format!(
                    "unsupported namespace placement kind in v0: {other:?}"
                )));
            }
        }
    }
    trace_perf!(
        desired_started,
        desired_row_count = desired_tree.len(),
        "stable tree desired rows prepared"
    );

    let current_started = perf_start!();
    let mut current_tree = BTreeMap::new();
    for row in tx.select_stable_tree_files().await? {
        let previous = current_tree.insert(row.path.clone(), row);
        if previous.is_some() {
            return Err(SemanticTransactionError::InvariantViolation(
                "stable_tree contained duplicate path".to_string(),
            ));
        }
    }
    trace_perf!(
        current_started,
        current_row_count = current_tree.len(),
        "stable tree current rows loaded"
    );

    let diff_started = perf_start!();
    let mut delete_paths = Vec::new();
    let mut insert_rows = Vec::new();

    for (path, current) in &current_tree {
        match desired_tree.get(path) {
            None => delete_paths.push(path.clone()),
            Some(desired) if !stable_tree_row_matches_desired(current, desired) => {
                // Delete changed rows first so claim_id uniqueness cannot block
                // rendered path moves during conflict winner/loser flips.
                delete_paths.push(path.clone());
                insert_rows.push((path.clone(), desired.clone()));
            }
            Some(_) => {}
        }
    }

    for (path, desired) in desired_tree {
        if !current_tree.contains_key(&path) {
            insert_rows.push((path, desired));
        }
    }

    trace_perf!(
        diff_started,
        deleted_row_count = delete_paths.len(),
        inserted_row_count = insert_rows.len(),
        "stable tree delta computed"
    );

    let delete_started = perf_start!();
    for path in &delete_paths {
        tx.delete_stable_tree_file(path).await?;
    }
    trace_perf!(
        delete_started,
        deleted_row_count = delete_paths.len(),
        "stable tree delta deleted"
    );

    let insert_started = perf_start!();
    for (path, desired) in &insert_rows {
        tx.insert_stable_tree_file(
            path,
            &desired.claimed_path,
            &desired.claim_id,
            &desired.object_id,
            updated_at,
        )
        .await?;
    }
    trace_perf!(
        insert_started,
        inserted_row_count = insert_rows.len(),
        "stable tree delta inserted"
    );

    trace_perf!(
        total_started,
        confirmed_object_count,
        placement_count,
        "stable namespace projection write complete"
    );

    Ok(())
}

fn stable_tree_row_matches_desired(
    row: &crate::semantic::table::stable_tree::Row,
    desired: &DesiredStableTreeFile,
) -> bool {
    row.claimed_path == desired.claimed_path
        && row.claim_id.as_ref() == desired.claim_id.0.as_ref()
        && row.object_id == desired.object_id
}

async fn apply_missing_prior_paths_as_local_deletes_tx(
    tx: &SemanticTransaction<'_>,
    missing_prior: Vec<crate::semantic::table::prior_tree::Row>,
) -> Result<u64, SemanticTransactionError> {
    if missing_prior.is_empty() {
        return Ok(0);
    }

    let now = time::OffsetDateTime::now_utc();
    let writer_id = tx.select_daemon_writer_id().await?;
    let client_id = namespace::client_id_for_writer(writer_id.0.as_ref());
    let prior_namespace = tx.select_prior_namespace().await?.ok_or_else(|| {
        SemanticTransactionError::InvariantViolation("missing prior_namespace".to_string())
    })?;
    let stable_namespace = tx.select_stable_namespace().await?.ok_or_else(|| {
        SemanticTransactionError::InvariantViolation("missing stable_namespace".to_string())
    })?;

    let prior_doc = namespace::NamespaceDoc::from_full_state(client_id, prior_namespace.doc_blob)?;
    let prior_sv_before = prior_doc.state_vector();
    for row in &missing_prior {
        prior_doc.remove_claim(&NamespaceClaimId(row.claim_id.clone()));
    }
    let namespace_update = prior_doc.encode_update_since(&prior_sv_before);

    tx.update_prior_namespace(prior_doc.encode_full_state(), now)
        .await?;
    if !namespace_update.is_empty() {
        let stable_doc = namespace::NamespaceDoc::from_full_state(1, stable_namespace.doc_blob)?;
        stable_doc.apply_update(namespace_update.as_ref())?;
        write_stable_namespace_projection_tx(tx, &stable_doc, now).await?;
    }

    for row in &missing_prior {
        tx.tombstone_prior_tree_file(&row.path, now).await?;
    }

    if namespace_update.is_empty() {
        return Ok(0);
    }

    let outbox_id = tx.reserve_outbox_ids(1).await?.ok_or_else(|| {
        SemanticTransactionError::InvariantViolation(
            "expected outbox id for local delete namespace update".to_string(),
        )
    })?;
    tx.insert_outbox_message(
        outbox_id,
        SharedMessageKind::NamespaceUpdate,
        None,
        namespace_update,
        now,
    )
    .await?;
    Ok(1)
}

async fn get_next_work_tx(
    tx: &SemanticTransaction<'_>,
    projection_epoch: ProjectionEpoch,
) -> Result<NextWork, SemanticTransactionError> {
    let now = time::OffsetDateTime::now_utc();
    let stable_cursor = tx.select_stable_cursor().await?;
    let generation = ProjectionGeneration::new(stable_cursor.end);
    let stable_namespace = tx.select_stable_namespace().await?.ok_or_else(|| {
        SemanticTransactionError::InvariantViolation("missing stable_namespace".to_string())
    })?;
    let mut actions = Vec::new();
    let mut next_seq = 0;
    let mut prior_by_path = tx
        .select_prior_tree_live_files()
        .await?
        .into_iter()
        .map(|row| (row.path.clone(), row))
        .collect::<BTreeMap<_, _>>();

    for row in tx.select_stable_tree_files().await? {
        let doc_blob = tx
            .select_stable_text_object_doc(&row.object_id)
            .await?
            .unwrap_or_else(|| text_doc::empty_text_state(1));
        let expected_before = match prior_by_path.remove(&row.path) {
            None => Some(ExpectedBefore::Missing),
            Some(prior) => {
                let prior_doc_blob = tx
                    .select_prior_text_object_doc(&prior.object_id)
                    .await?
                    .unwrap_or_else(|| text_doc::empty_text_state(1));
                if prior.object_id == row.object_id && prior_doc_blob == doc_blob {
                    None
                } else {
                    Some(ExpectedBefore::PresentWithFingerprint(prior.fingerprint))
                }
            }
        };
        let Some(expected_before) = expected_before else {
            continue;
        };

        let text_state = text_doc::decode_text_state(1, doc_blob.as_ref())?;
        let bytes = Bytes::from(text_state.text.into_bytes());

        // Record the planned write durably before any fs effect can happen.
        // If the epoch is invalidated or the daemon dies before the epoch
        // commit advances prior, the import path uses this intent to
        // recognize the on-disk content as our own write instead of
        // re-importing it as a user edit (which would duplicate text).
        let target_hash = Bytes::copy_from_slice(&xxh3_64(bytes.as_ref()).to_be_bytes());
        tx.upsert_projection_write_intent(&row.path, &target_hash, &row.object_id, &doc_blob, now)
            .await?;

        let action_id = ProjectionActionId::new(projection_epoch, next_seq);
        next_seq += 1;
        actions.push(ProjectionAction {
            id: action_id,
            kind: ProjectionActionKind::WriteFile {
                path: row.path.clone(),
                bytes,
                expected_before,
            },
            commit_context: ProjectionActionCommitContext {
                path: row.path,
                claimed_path: row.claimed_path,
                claim_id: NamespaceClaimId(row.claim_id),
                object_id: row.object_id,
                target_text_doc_blob: Some(doc_blob),
            },
        });
    }

    for (_path, prior) in prior_by_path {
        let action_id = ProjectionActionId::new(projection_epoch, next_seq);
        next_seq += 1;
        actions.push(ProjectionAction {
            id: action_id,
            kind: ProjectionActionKind::DeleteFile {
                path: prior.path.clone(),
                expected_before: ExpectedBefore::PresentWithFingerprint(prior.fingerprint.clone()),
            },
            commit_context: ProjectionActionCommitContext {
                path: prior.path,
                claimed_path: prior.claimed_path,
                claim_id: NamespaceClaimId(prior.claim_id),
                object_id: prior.object_id,
                target_text_doc_blob: None,
            },
        });
    }

    if actions.is_empty() {
        Ok(NextWork::None)
    } else {
        Ok(NextWork::Project(ProjectionEpochStarted {
            epoch: projection_epoch,
            generation,
            target_namespace_doc_blob: stable_namespace.doc_blob,
            plan: ProjectionPlan { actions },
        }))
    }
}

async fn commit_projection_epoch_tx(
    tx: &SemanticTransaction<'_>,
    completed: &CompletedProjectionEpoch,
    reason: ProjectionEpochEndReason,
) -> Result<(), SemanticTransactionError> {
    trace!(
        epoch = ?completed.epoch,
        generation = ?completed.generation,
        ?reason,
        "committing projection epoch"
    );

    match reason {
        ProjectionEpochEndReason::PlanExhausted => {}
        ProjectionEpochEndReason::ActionInvalidatedProjection { .. } => {
            // An invalidated projection must not advance the prior baseline.
            // Successful writes orphaned here are recognized at the next
            // import via the projection write-intent journal.
            return Ok(());
        }
    }

    let accepted_at = time::OffsetDateTime::now_utc();
    tx.update_prior_namespace(completed.target_namespace_doc_blob.clone(), accepted_at)
        .await?;

    for action in &completed.actions {
        commit_completed_projection_action_tx(tx, action, accepted_at).await?;
    }

    Ok(())
}

async fn commit_completed_projection_action_tx(
    tx: &SemanticTransaction<'_>,
    action: &crate::semantic::actor::CompletedProjectionAction,
    accepted_at: time::OffsetDateTime,
) -> Result<(), SemanticTransactionError> {
    assert_eq!(
        action.result.action_id(),
        action.action_id,
        "completed projection action result id does not match action id"
    );

    match &action.result {
        ProjectionActionResult::WriteFile {
            result:
                GuardedWriteResult::Written { fingerprint }
                | GuardedWriteResult::AlreadyApplied { fingerprint },
            ..
        } => {
            let target_text_doc_blob = action
                .commit_context
                .target_text_doc_blob
                .clone()
                .ok_or_else(|| {
                    SemanticTransactionError::InvariantViolation(format!(
                        "write projection action {:?} missing target text doc blob",
                        action.action_id
                    ))
                })?;
            tx.upsert_prior_text_object(
                &action.commit_context.object_id,
                target_text_doc_blob,
                accepted_at,
            )
            .await?;
            tx.upsert_prior_tree_file(
                &action.commit_context.path,
                &action.commit_context.claimed_path,
                &action.commit_context.claim_id,
                &action.commit_context.object_id,
                fingerprint,
                accepted_at,
            )
            .await?;
            // Prior now reflects this write; outstanding intents for the path
            // (this epoch's and any orphaned generations) are obsolete.
            tx.delete_projection_write_intents_for_path(&action.commit_context.path)
                .await?;
            Ok(())
        }
        ProjectionActionResult::WriteFile { result, .. } => {
            Err(SemanticTransactionError::InvariantViolation(format!(
                "cannot commit invalidated projection write result for action {:?}: {}",
                action.action_id,
                Into::<&'static str>::into(result)
            )))
        }
        ProjectionActionResult::DeleteFile {
            result: GuardedDeleteResult::Deleted | GuardedDeleteResult::AlreadyDeleted,
            ..
        } => {
            tx.tombstone_prior_tree_file(&action.commit_context.path, accepted_at)
                .await?;
            // A committed delete invalidates any outstanding write intents for
            // the path; without this a much later user file that happens to
            // byte-match a dead intent could be misread as our own write.
            tx.delete_projection_write_intents_for_path(&action.commit_context.path)
                .await?;
            Ok(())
        }
        ProjectionActionResult::DeleteFile {
            result: GuardedDeleteResult::Conflict { .. },
            ..
        } => Err(SemanticTransactionError::InvariantViolation(format!(
            "cannot commit invalidated projection delete result for action {:?}",
            action.action_id
        ))),
        ProjectionActionResult::Failed { .. } => {
            Err(SemanticTransactionError::InvariantViolation(format!(
                "cannot commit failed projection action {:?}",
                action.action_id
            )))
        }
    }
}

async fn commit_init_import_epoch_tx(
    tx: &SemanticTransaction<'_>,
    completed: &CompletedImportEpoch,
) -> Result<CommitImportEpochOutput, SemanticTransactionError> {
    let staged = tx.select_import_staged_files(completed.epoch).await?;
    validate_init_staged_rows(completed, &staged)?;

    if staged.is_empty() {
        tx.delete_import_staged_files(completed.epoch).await?;
        return Ok(CommitImportEpochOutput {
            effects: SemanticEffects::none(),
        });
    }

    let now = time::OffsetDateTime::now_utc();
    let writer_id = tx.select_daemon_writer_id().await?;
    let stable_namespace = tx.select_stable_namespace().await?.ok_or_else(|| {
        SemanticTransactionError::InvariantViolation("missing stable_namespace".to_string())
    })?;
    let prior_namespace = tx.select_prior_namespace().await?.ok_or_else(|| {
        SemanticTransactionError::InvariantViolation("missing prior_namespace".to_string())
    })?;
    if stable_namespace.doc_blob != prior_namespace.doc_blob {
        return Err(SemanticTransactionError::InvariantViolation(
            "init import requires prior/stable namespace to match".to_string(),
        ));
    }

    let namespace_client_id = namespace::client_id_for_writer(writer_id.0.as_ref());
    let namespace_doc =
        namespace::NamespaceDoc::from_full_state(namespace_client_id, stable_namespace.doc_blob)?;
    let namespace_sv_before = namespace_doc.state_vector();

    for row in &staged {
        namespace_doc.add_new_object(&row.object_id, ObjectKind::Text, &writer_id);
        namespace_doc.add_new_claim(&row.claim_id, &row.object_id, &row.path);
    }

    let namespace_update = namespace_doc.encode_update_since(&namespace_sv_before);
    if namespace_update.is_empty() {
        return Err(SemanticTransactionError::InvariantViolation(
            "init import namespace update unexpectedly empty".to_string(),
        ));
    }
    let namespace_full_state = namespace_doc.encode_full_state();

    tx.update_stable_namespace(namespace_full_state.clone(), now)
        .await?;
    tx.update_prior_namespace(namespace_full_state, now).await?;

    for row in &staged {
        tx.insert_object(&row.object_id, ObjectKind::Text, &writer_id, now)
            .await?;
        tx.insert_stable_text_object(&row.object_id, row.prior_doc_blob.clone(), now)
            .await?;
        tx.insert_prior_text_object(&row.object_id, row.prior_doc_blob.clone(), now)
            .await?;
        tx.insert_stable_tree_file(&row.path, &row.path, &row.claim_id, &row.object_id, now)
            .await?;
        tx.insert_prior_tree_file(
            &row.path,
            &row.path,
            &row.claim_id,
            &row.object_id,
            &row.fingerprint,
            now,
        )
        .await?;
    }

    let outbox_message_count = u64::try_from(staged.len())
        .map_err(|err| {
            SemanticTransactionError::InvariantViolation(format!(
                "staged row count out of range: {err}"
            ))
        })?
        .checked_add(1)
        .ok_or_else(|| {
            SemanticTransactionError::InvariantViolation(
                "outbox message count overflow".to_string(),
            )
        })?;
    let first_outbox_id = tx
        .reserve_outbox_ids(outbox_message_count)
        .await?
        .ok_or_else(|| {
            SemanticTransactionError::InvariantViolation(
                "expected outbox id allocation".to_string(),
            )
        })?;
    let mut outbox_id = first_outbox_id;

    tx.insert_outbox_message(
        outbox_id,
        SharedMessageKind::NamespaceUpdate,
        None,
        namespace_update,
        now,
    )
    .await?;

    for row in &staged {
        outbox_id = next_outbox_id(outbox_id)?;
        tx.insert_outbox_message(
            outbox_id,
            SharedMessageKind::TextUpdate,
            Some(&row.object_id),
            row.text_update.clone(),
            now,
        )
        .await?;
    }

    tx.delete_import_staged_files(completed.epoch).await?;

    Ok(CommitImportEpochOutput {
        effects: SemanticEffects {
            projection_changed: None,
            outbox_ready: Some(outbox_message_count),
        },
    })
}

async fn commit_sync_import_epoch_tx(
    tx: &SemanticTransaction<'_>,
    completed: &CompletedImportEpoch,
) -> Result<CommitImportEpochOutput, SemanticTransactionError> {
    let total_started = perf_start!();
    let staged_started = perf_start!();
    let staged = tx.select_import_staged_files(completed.epoch).await?;
    validate_sync_staged_rows(completed, &staged)?;
    trace_perf!(
        staged_started,
        epoch = ?completed.epoch,
        staged_count = staged.len(),
        completed_actions = completed.actions.len(),
        "commit_sync_import_epoch staged rows loaded"
    );

    if staged.is_empty() {
        let cleanup_started = perf_start!();
        tx.delete_import_staged_files(completed.epoch).await?;
        trace_perf!(
            cleanup_started,
            epoch = ?completed.epoch,
            "commit_sync_import_epoch empty staged cleanup complete"
        );
        trace_perf!(
            total_started,
            epoch = ?completed.epoch,
            staged_count = 0,
            "commit_sync_import_epoch complete"
        );
        return Ok(CommitImportEpochOutput {
            effects: SemanticEffects::none(),
        });
    }

    let now = time::OffsetDateTime::now_utc();
    let namespace_changed = staged
        .iter()
        .any(|row| matches!(row.stage_kind, StageKind::New | StageKind::Resurrect));
    let namespace_started = perf_start!();
    let mut namespace_update = Bytes::new();
    if namespace_changed {
        let writer_id = tx.select_daemon_writer_id().await?;
        let prior_namespace = tx.select_prior_namespace().await?.ok_or_else(|| {
            SemanticTransactionError::InvariantViolation("missing prior_namespace".to_string())
        })?;
        let stable_namespace = tx.select_stable_namespace().await?.ok_or_else(|| {
            SemanticTransactionError::InvariantViolation("missing stable_namespace".to_string())
        })?;

        let namespace_client_id = namespace::client_id_for_writer(writer_id.0.as_ref());
        let prior_namespace_doc = namespace::NamespaceDoc::from_full_state(
            namespace_client_id,
            prior_namespace.doc_blob,
        )?;
        let prior_namespace_sv_before = prior_namespace_doc.state_vector();

        for row in &staged {
            match row.stage_kind {
                StageKind::New => {
                    prior_namespace_doc.add_new_object(
                        &row.object_id,
                        ObjectKind::Text,
                        &writer_id,
                    );
                    prior_namespace_doc.add_new_claim(&row.claim_id, &row.object_id, &row.path);
                }
                StageKind::Resurrect => {
                    prior_namespace_doc.add_new_claim(&row.claim_id, &row.object_id, &row.path);
                }
                StageKind::Update => {}
            }
        }

        namespace_update = prior_namespace_doc.encode_update_since(&prior_namespace_sv_before);
        if namespace_update.is_empty() {
            return Err(SemanticTransactionError::InvariantViolation(
                "sync import marked namespace changed but encoded empty namespace update"
                    .to_string(),
            ));
        }

        tx.update_prior_namespace(prior_namespace_doc.encode_full_state(), now)
            .await?;
        let stable_namespace_doc =
            namespace::NamespaceDoc::from_full_state(1, stable_namespace.doc_blob)?;
        stable_namespace_doc.apply_update(namespace_update.as_ref())?;
        write_stable_namespace_projection_tx(tx, &stable_namespace_doc, now).await?;
    }
    trace_perf!(
        namespace_started,
        namespace_changed,
        namespace_update_bytes = namespace_update.len(),
        staged_count = staged.len(),
        "commit_sync_import_epoch namespace phase complete"
    );

    for row in &staged {
        let row_started = perf_start!();
        let stable_doc_blob = if row.text_update.is_empty() {
            tx.select_stable_text_object_doc(&row.object_id)
                .await?
                .unwrap_or_else(|| text_doc::empty_text_state(1))
        } else {
            let stable_base = tx
                .select_stable_text_object_doc(&row.object_id)
                .await?
                .unwrap_or_else(|| text_doc::empty_text_state(1));
            text_doc::apply_text_update(stable_base.as_ref(), row.text_update.as_ref())?
                .full_state_bytes
        };
        tx.upsert_stable_text_object(&row.object_id, stable_doc_blob, now)
            .await?;
        tx.upsert_prior_text_object(&row.object_id, row.prior_doc_blob.clone(), now)
            .await?;
        tx.upsert_prior_tree_file(
            &row.path,
            &row.path,
            &row.claim_id,
            &row.object_id,
            &row.fingerprint,
            now,
        )
        .await?;
        trace_perf!(
            row_started,
            path = %row.path,
            object_id = ?row.object_id,
            stage_kind = ?row.stage_kind,
            text_update_bytes = row.text_update.len(),
            prior_doc_bytes = row.prior_doc_blob.len(),
            "commit_sync_import_epoch text row complete"
        );
    }

    let outbox_started = perf_start!();
    let outbox_message_count = u64::from(namespace_changed)
        .checked_add(
            u64::try_from(
                staged
                    .iter()
                    .filter(|row| !row.text_update.is_empty())
                    .count(),
            )
            .map_err(|err| {
                SemanticTransactionError::InvariantViolation(format!(
                    "staged text update count out of range: {err}"
                ))
            })?,
        )
        .ok_or_else(|| {
            SemanticTransactionError::InvariantViolation(
                "sync import outbox message count overflow".to_string(),
            )
        })?;

    if let Some(mut outbox_id) = tx.reserve_outbox_ids(outbox_message_count).await? {
        let mut wrote_any = false;
        if namespace_changed {
            tx.insert_outbox_message(
                outbox_id,
                SharedMessageKind::NamespaceUpdate,
                None,
                namespace_update,
                now,
            )
            .await?;
            wrote_any = true;
        }

        for row in staged.iter().filter(|row| !row.text_update.is_empty()) {
            if wrote_any {
                outbox_id = next_outbox_id(outbox_id)?;
            }
            tx.insert_outbox_message(
                outbox_id,
                SharedMessageKind::TextUpdate,
                Some(&row.object_id),
                row.text_update.clone(),
                now,
            )
            .await?;
            wrote_any = true;
        }
    }
    trace_perf!(
        outbox_started,
        outbox_message_count,
        "commit_sync_import_epoch outbox phase complete"
    );

    let cleanup_started = perf_start!();
    tx.delete_import_staged_files(completed.epoch).await?;
    trace_perf!(
        cleanup_started,
        epoch = ?completed.epoch,
        "commit_sync_import_epoch staged cleanup complete"
    );

    trace_perf!(
        total_started,
        epoch = ?completed.epoch,
        staged_count = staged.len(),
        outbox_message_count,
        "commit_sync_import_epoch complete"
    );

    Ok(CommitImportEpochOutput {
        effects: SemanticEffects {
            projection_changed: None,
            outbox_ready: nonzero_u64(outbox_message_count),
        },
    })
}

fn validate_init_staged_rows(
    completed: &CompletedImportEpoch,
    staged: &[import_staged_files::Row],
) -> Result<(), SemanticTransactionError> {
    // Staged rows are a subset of completed actions: actions whose stage
    // decision was Ignore (non-UTF-8 files, reads invalidated by concurrent
    // changes) complete without staging anything.
    let completed_actions = completed
        .actions
        .iter()
        .map(|action| (action.action_id, action))
        .collect::<BTreeMap<_, _>>();

    for action in &completed.actions {
        if action.action_id.epoch != completed.epoch {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "completed import action {:?} is not in epoch {:?}",
                action.action_id, completed.epoch
            )));
        }
    }

    for row in staged {
        let action_id = ImportActionId {
            epoch: row.import_epoch,
            seq: row.action_seq,
        };
        let action = completed_actions.get(&action_id).ok_or_else(|| {
            SemanticTransactionError::InvariantViolation(format!(
                "staged init import row epoch={:?} seq={} has no completed action",
                row.import_epoch, row.action_seq
            ))
        })?;
        if row.import_epoch != completed.epoch {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "staged init import row {:?} is not in epoch {:?}",
                action_id, completed.epoch
            )));
        }
        if row.stage_kind != StageKind::New {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "init import action {:?} staged non-new row: {:?}",
                action_id, row.stage_kind
            )));
        }

        match &action.action_kind {
            ImportActionKind::Read { path, .. } if path == &row.path => {}
            ImportActionKind::Read { path, .. } => {
                return Err(SemanticTransactionError::InvariantViolation(format!(
                    "completed import action {:?} path {} does not match staged path {}",
                    action.action_id, path, row.path
                )));
            }
            ImportActionKind::Stat { .. } => {
                return Err(SemanticTransactionError::InvariantViolation(format!(
                    "init import epoch cannot commit stat action {:?}",
                    action.action_id
                )));
            }
        }
    }

    Ok(())
}

fn validate_sync_staged_rows(
    completed: &CompletedImportEpoch,
    staged: &[import_staged_files::Row],
) -> Result<(), SemanticTransactionError> {
    let completed_actions = completed
        .actions
        .iter()
        .map(|action| (action.action_id, action))
        .collect::<BTreeMap<_, _>>();

    for action in &completed.actions {
        if action.action_id.epoch != completed.epoch {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "completed sync import action {:?} is not in epoch {:?}",
                action.action_id, completed.epoch
            )));
        }

        if matches!(action.action_kind, ImportActionKind::Stat { .. }) {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "sync import epoch cannot commit stat action {:?}",
                action.action_id
            )));
        }
    }

    for row in staged {
        let action_id = ImportActionId {
            epoch: row.import_epoch,
            seq: row.action_seq,
        };
        let action = completed_actions.get(&action_id).ok_or_else(|| {
            SemanticTransactionError::InvariantViolation(format!(
                "staged sync import row epoch={:?} seq={} has no completed action",
                row.import_epoch, row.action_seq
            ))
        })?;

        if row.import_epoch != completed.epoch {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "staged sync import row {:?} is not in epoch {:?}",
                action_id, completed.epoch
            )));
        }
        match &action.action_kind {
            ImportActionKind::Read { path, .. } if path == &row.path => {}
            ImportActionKind::Read { path, .. } => {
                return Err(SemanticTransactionError::InvariantViolation(format!(
                    "completed sync import action {:?} path {} does not match staged path {}",
                    action.action_id, path, row.path
                )));
            }
            ImportActionKind::Stat { .. } => {
                return Err(SemanticTransactionError::InvariantViolation(format!(
                    "sync import epoch cannot commit stat action {:?}",
                    action.action_id
                )));
            }
        }
    }

    Ok(())
}

fn next_outbox_id(id: OutboxId) -> Result<OutboxId, SemanticTransactionError> {
    id.get()
        .checked_add(1)
        .map(OutboxId::new)
        .ok_or_else(|| SemanticTransactionError::InvariantViolation("outbox id overflow".into()))
}

fn nonzero_u64(value: u64) -> Option<u64> {
    if value == 0 { None } else { Some(value) }
}

struct ImportActionStagePlan {
    action_id: ImportActionId,
    epoch_kind: ImportEpochKind,
    path: crate::fs::types::RelativePath,
    fingerprint: FileFingerprint,
    content: String,
}

enum ImportActionStageDecision {
    Stage(ImportActionStagePlan),
    Ignore {
        action_id: ImportActionId,
    },
    IgnorePersist {
        action_id: ImportActionId,
        path: crate::fs::types::RelativePath,
        stat: crate::fs::types::FileStatFingerprint,
        reason: crate::semantic::table::ignored_files::Reason,
    },
}

impl ImportActionStageDecision {
    fn action_id(&self) -> ImportActionId {
        match self {
            ImportActionStageDecision::Stage(plan) => plan.action_id,
            ImportActionStageDecision::Ignore { action_id }
            | ImportActionStageDecision::IgnorePersist { action_id, .. } => *action_id,
        }
    }
}

fn prepare_import_action_stage(
    context: ImportActionContext,
    result: ImportActionResult,
) -> eyre::Result<ImportActionStageDecision> {
    match (context.epoch_kind, context.action_kind, result) {
        (
            epoch_kind @ (ImportEpochKind::Init | ImportEpochKind::Sync),
            ImportActionKind::Read {
                path,
                expected_fingerprint,
            },
            ImportActionResult::Read { action_id, outcome },
        ) => match outcome {
            ImportReadOutcome::Completed(GuardedReadResult::Read { bytes, fingerprint }) => {
                if !fingerprint_satisfies_expected(&fingerprint, &expected_fingerprint) {
                    eyre::bail!(
                        "import read {:?} returned fingerprint that does not satisfy expected fingerprint",
                        action_id
                    );
                }
                let FileFingerprint::StatAndContent { .. } = &fingerprint else {
                    eyre::bail!("import read {action_id:?} returned stat-only fingerprint");
                };

                // v0 syncs text only: binary (non-UTF-8) files are skipped
                // uniformly. Init must tolerate them too — a single binary
                // file in the directory must not abort workspace creation.
                let content = match String::from_utf8(bytes.to_vec()) {
                    Ok(content) => content,
                    Err(err) => {
                        debug!(
                            ?action_id,
                            %path,
                            ?epoch_kind,
                            error = %err,
                            "ignoring non-UTF-8 file during import"
                        );
                        return Ok(ImportActionStageDecision::IgnorePersist {
                            action_id,
                            path,
                            stat: fingerprint.stat().clone(),
                            reason: crate::semantic::table::ignored_files::Reason::NonUtf8,
                        });
                    }
                };

                Ok(ImportActionStageDecision::Stage(ImportActionStagePlan {
                    action_id,
                    epoch_kind,
                    path,
                    fingerprint,
                    content,
                }))
            }
            // Files changing underneath the import are skipped for both epoch
            // kinds: sync defers to the scan the conflict already triggered,
            // and a file skipped during init is imported by the first sync
            // daemon's startup full scan.
            ImportReadOutcome::Completed(GuardedReadResult::ChangedBetweenStats {
                before,
                after,
            }) => {
                debug!(
                    ?action_id,
                    %path,
                    ?epoch_kind,
                    ?before,
                    ?after,
                    "import read changed between stats; deferring to a fresh scan"
                );
                Ok(ImportActionStageDecision::Ignore { action_id })
            }
            ImportReadOutcome::Completed(GuardedReadResult::ConflictBeforeRead { observed }) => {
                debug!(
                    ?action_id,
                    %path,
                    ?epoch_kind,
                    ?observed,
                    "import read conflicted before read; deferring to a fresh scan"
                );
                Ok(ImportActionStageDecision::Ignore { action_id })
            }
            ImportReadOutcome::Failed(err) => Err(err),
        },
        (
            ImportEpochKind::Init | ImportEpochKind::Sync,
            ImportActionKind::Stat { .. },
            ImportActionResult::Stat { action_id, .. },
        ) => {
            eyre::bail!("import stat actions are not supported yet: {action_id:?}");
        }
        (_, _, result) => {
            eyre::bail!(
                "import action result {:?} did not match expected import action context",
                result.action_id()
            );
        }
    }
}

fn fingerprint_satisfies_expected(observed: &FileFingerprint, expected: &FileFingerprint) -> bool {
    match expected {
        FileFingerprint::StatOnly(expected_stat) => observed.stat() == expected_stat,
        FileFingerprint::StatAndContent {
            stat: expected_stat,
            content: expected_content,
        } => match observed {
            FileFingerprint::StatAndContent { stat, content } => {
                stat == expected_stat && content == expected_content
            }
            FileFingerprint::StatOnly(_) => false,
        },
    }
}

async fn stage_import_action_tx(
    tx: &SemanticTransaction<'_>,
    plan: &ImportActionStagePlan,
) -> Result<(), SemanticTransactionError> {
    let total_started = perf_start!();

    if plan.epoch_kind == ImportEpochKind::Sync
        && realign_prior_from_write_intent_tx(tx, plan).await?
    {
        // The on-disk content is a projection write of ours that an
        // invalidated epoch or crash orphaned before prior advanced. Prior has
        // been realigned to the intended target; importing it as a user edit
        // would mint duplicate CRDT ops.
        return Ok(());
    }

    let writer_id = tx.select_daemon_writer_id().await?;
    let client_id = text_doc::client_id_for_writer(writer_id.0.as_ref());
    let staged = match plan.epoch_kind {
        ImportEpochKind::Init => stage_new_text_file(
            client_id,
            &plan.content,
            ObjectId::generate(),
            NamespaceClaimId::generate(),
        ),
        ImportEpochKind::Sync => match tx.select_prior_tree_live_file(&plan.path).await? {
            Some(prior) => {
                stage_existing_text_file(tx, client_id, plan, prior, StageKind::Update).await?
            }
            None => {
                let cutoff = time::OffsetDateTime::now_utc()
                    - time::Duration::seconds(RECENT_TOMBSTONE_REUSE_WINDOW_SECS);
                let reusable_tombstone = match tx
                    .select_recent_tombstoned_prior_tree_file(&plan.path, cutoff)
                    .await?
                {
                    Some(prior) => Some(prior),
                    None => {
                        tx.select_recent_tombstoned_prior_tree_file_by_file_key(
                            &plan.fingerprint.stat().file_key,
                            cutoff,
                        )
                        .await?
                    }
                };

                match reusable_tombstone {
                    Some(prior) => {
                        // Editor safe-save often appears as delete followed by
                        // recreate, and ordinary renames appear as an old path
                        // tombstone plus a new path with the same local file
                        // key. Reuse the recent tombstoned object, but create a
                        // fresh namespace claim because the old claim was
                        // already removed.
                        stage_existing_text_file(tx, client_id, plan, prior, StageKind::Resurrect)
                            .await?
                    }
                    None => stage_new_text_file(
                        client_id,
                        &plan.content,
                        ObjectId::generate(),
                        NamespaceClaimId::generate(),
                    ),
                }
            }
        },
    };

    let staged_stage_kind = staged.stage_kind;
    let staged_object_id = staged.object_id.clone();
    let staged_text_update_bytes = staged.text_update.len();
    let staged_prior_doc_bytes = staged.prior_doc_blob.len();
    let row = import_staged_files::Row {
        import_epoch: plan.action_id.epoch,
        action_seq: plan.action_id.seq,
        path: plan.path.clone(),
        object_id: staged.object_id,
        claim_id: staged.claim_id,
        stage_kind: staged.stage_kind,
        fingerprint: plan.fingerprint.clone(),
        prior_doc_blob: staged.prior_doc_blob,
        text_update: staged.text_update,
        staged_at: time::OffsetDateTime::now_utc(),
    };

    tx.insert_import_staged_file(&row).await?;
    trace_perf!(
        total_started,
        action_id = ?plan.action_id,
        path = %plan.path,
        epoch_kind = ?plan.epoch_kind,
        stage_kind = ?staged_stage_kind,
        object_id = ?staged_object_id,
        content_bytes = plan.content.len(),
        text_update_bytes = staged_text_update_bytes,
        prior_doc_bytes = staged_prior_doc_bytes,
        "stage_import_action complete"
    );

    Ok(())
}

/// Recovery half of the projection write-intent journal: if the file content
/// being imported matches an outstanding intent AND the path still maps to the
/// intent's object in the stable tree, the content is the daemon's own
/// projection write whose epoch never committed. Realign prior to the intended
/// target (so the next projection plans from the true disk baseline) and emit
/// no import. Returns true when the import for this path was consumed.
async fn realign_prior_from_write_intent_tx(
    tx: &SemanticTransaction<'_>,
    plan: &ImportActionStagePlan,
) -> Result<bool, SemanticTransactionError> {
    let Some((intent_object_id, target_doc_blob)) = tx
        .select_applied_projection_write_intent(&plan.path, &plan.fingerprint)
        .await?
    else {
        return Ok(false);
    };

    let stable_row = tx.select_stable_tree_file(&plan.path).await?;
    let Some(stable_row) = stable_row.filter(|row| row.object_id == intent_object_id) else {
        // Stable has moved the path off the intent's object since the write
        // was planned; the matching bytes no longer describe our write's
        // place in the world. Treat the disk content as user intent.
        debug!(
            path = %plan.path,
            intent_object_id = ?intent_object_id,
            "projection write intent no longer matches stable tree; importing as user edit"
        );
        return Ok(false);
    };

    let Some(stable_doc_blob) = tx.select_stable_text_object_doc(&intent_object_id).await? else {
        debug!(
            path = %plan.path,
            intent_object_id = ?intent_object_id,
            "projection write intent matched disk but stable text object is missing; importing as user edit"
        );
        return Ok(false);
    };
    if stable_doc_blob != target_doc_blob {
        // The same bytes/fingerprint can be observed after a later user edit
        // (notably clearing a file to empty). Only suppress when the applied
        // intent still describes the current stable CRDT doc.
        debug!(
            path = %plan.path,
            intent_object_id = ?intent_object_id,
            "projection write intent is stale relative to stable text object; importing as user edit"
        );
        return Ok(false);
    }

    let now = time::OffsetDateTime::now_utc();
    tx.upsert_prior_text_object(&intent_object_id, stable_doc_blob, now)
        .await?;
    tx.upsert_prior_tree_file(
        &plan.path,
        &stable_row.claimed_path,
        &NamespaceClaimId(stable_row.claim_id),
        &intent_object_id,
        &plan.fingerprint,
        now,
    )
    .await?;
    tx.delete_projection_write_intents_for_path(&plan.path)
        .await?;

    debug!(
        path = %plan.path,
        object_id = ?intent_object_id,
        "realigned prior to orphaned projection write; suppressed self-echo import"
    );
    Ok(true)
}

struct StagedTextImport {
    stage_kind: StageKind,
    object_id: ObjectId,
    claim_id: NamespaceClaimId,
    prior_doc_blob: Bytes,
    text_update: Bytes,
}

async fn stage_existing_text_file(
    tx: &SemanticTransaction<'_>,
    client_id: u64,
    plan: &ImportActionStagePlan,
    prior: crate::semantic::table::prior_tree::Row,
    stage_kind: StageKind,
) -> Result<StagedTextImport, SemanticTransactionError> {
    assert!(
        matches!(stage_kind, StageKind::Update | StageKind::Resurrect),
        "existing text import must be update or resurrect"
    );

    let total_started = perf_start!();
    let object_id = prior.object_id;
    let claim_id = match stage_kind {
        StageKind::Update => NamespaceClaimId(prior.claim_id),
        StageKind::Resurrect => NamespaceClaimId::generate(),
        StageKind::New => unreachable!("asserted above"),
    };

    let prior_doc_blob = tx
        .select_prior_text_object_doc(&object_id)
        .await?
        .unwrap_or_else(|| text_doc::empty_text_state(client_id));
    let decode_started = perf_start!();
    let prior_text_state = text_doc::decode_text_state(client_id, prior_doc_blob.as_ref())?;
    let prior_text = prior_text_state.text;
    trace_perf!(
        decode_started,
        path = %plan.path,
        object_id = ?object_id,
        prior_doc_bytes = prior_doc_blob.len(),
        prior_text_bytes = prior_text.len(),
        "stage_existing_text_file prior text decoded"
    );

    let prior_capture_started = perf_start!();
    let prior_capture = text_doc::capture_text_change(
        client_id,
        prior_doc_blob.as_ref(),
        &prior_text,
        &plan.content,
    )?;
    trace_perf!(
        prior_capture_started,
        path = %plan.path,
        object_id = ?object_id,
        old_bytes = prior_text.len(),
        new_bytes = plan.content.len(),
        changed = prior_capture.is_some(),
        "stage_existing_text_file prior text change captured"
    );

    let (next_prior_doc_blob, text_update) = match prior_capture {
        Some(capture) => (capture.full_state_bytes, capture.update_bytes),
        None => (prior_doc_blob, Bytes::new()),
    };

    let staged = StagedTextImport {
        stage_kind,
        object_id,
        claim_id,
        prior_doc_blob: next_prior_doc_blob,
        text_update,
    };
    trace_perf!(
        total_started,
        path = %plan.path,
        object_id = ?staged.object_id,
        stage_kind = ?stage_kind,
        text_update_bytes = staged.text_update.len(),
        prior_doc_bytes = staged.prior_doc_blob.len(),
        "stage_existing_text_file complete"
    );
    Ok(staged)
}

fn stage_new_text_file(
    client_id: u64,
    content: &str,
    object_id: ObjectId,
    claim_id: NamespaceClaimId,
) -> StagedTextImport {
    let prior_doc_blob = text_doc::text_state_from_content(client_id, content);
    // A full-state update is a valid update against an empty text object.
    let text_update = prior_doc_blob.clone();
    StagedTextImport {
        stage_kind: StageKind::New,
        object_id,
        claim_id,
        prior_doc_blob,
        text_update,
    }
}

pub struct TursoConnectionManager {
    db: Database,
}

impl TursoConnectionManager {
    pub fn new(db: Database) -> Self {
        Self { db }
    }
}

impl bb8::ManageConnection for TursoConnectionManager {
    type Connection = Connection;
    type Error = turso::Error;

    async fn connect(&self) -> Result<Connection, turso::Error> {
        self.db.connect()
    }

    async fn is_valid(&self, conn: &mut Connection) -> Result<(), turso::Error> {
        conn.query("SELECT 1", ()).await?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Connection) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::types::{
        FileContentFingerprint, FileHash, FileKey, FileStatFingerprint, RelativePath,
    };
    use crate::semantic::actor::CompletedImportAction;

    fn stat() -> FileStatFingerprint {
        FileStatFingerprint::new(FileKey::new(1, 1), 5, time::OffsetDateTime::UNIX_EPOCH)
    }

    fn read_result(action_id: ImportActionId, bytes: &'static [u8]) -> ImportActionResult {
        ImportActionResult::Read {
            action_id,
            outcome: ImportReadOutcome::Completed(GuardedReadResult::Read {
                bytes: Bytes::from_static(bytes),
                fingerprint: FileFingerprint::StatAndContent {
                    stat: stat(),
                    content: FileContentFingerprint::new(FileHash::new(Bytes::from_static(
                        b"hash",
                    ))),
                },
            }),
        }
    }

    fn read_context(epoch_kind: ImportEpochKind, path: &str) -> ImportActionContext {
        ImportActionContext {
            epoch_kind,
            action_kind: ImportActionKind::Read {
                path: RelativePath::parse(path).expect("valid path"),
                expected_fingerprint: FileFingerprint::StatOnly(stat()),
            },
        }
    }

    #[test]
    fn non_utf8_files_are_ignored_for_both_epoch_kinds() -> eyre::Result<()> {
        for epoch_kind in [ImportEpochKind::Init, ImportEpochKind::Sync] {
            let action_id = ImportActionId::new(ImportEpoch::new(0), 0);
            let decision = prepare_import_action_stage(
                read_context(epoch_kind, "blob.bin"),
                read_result(action_id, b"\x00\x01\xff\xfe\x80"),
            )?;
            assert!(
                matches!(decision, ImportActionStageDecision::IgnorePersist { .. }),
                "{epoch_kind:?} import must ignore non-UTF-8 content"
            );
        }
        Ok(())
    }

    #[test]
    fn init_staged_rows_may_be_a_subset_of_completed_actions() {
        let epoch = ImportEpoch::new(0);
        let completed = CompletedImportEpoch {
            epoch,
            kind: ImportEpochKind::Init,
            actions: vec![
                CompletedImportAction {
                    action_id: ImportActionId::new(epoch, 0),
                    action_kind: ImportActionKind::Read {
                        path: RelativePath::parse("a.txt").expect("valid path"),
                        expected_fingerprint: FileFingerprint::StatOnly(stat()),
                    },
                },
                // seq 1 (a binary file) completed with an Ignore decision, so
                // it has no staged row.
                CompletedImportAction {
                    action_id: ImportActionId::new(epoch, 1),
                    action_kind: ImportActionKind::Read {
                        path: RelativePath::parse("blob.bin").expect("valid path"),
                        expected_fingerprint: FileFingerprint::StatOnly(stat()),
                    },
                },
            ],
        };
        let staged_row = |seq: u64, path: &str| import_staged_files::Row {
            import_epoch: epoch,
            action_seq: seq,
            path: RelativePath::parse(path).expect("valid path"),
            object_id: ObjectId(Bytes::from_static(b"o")),
            claim_id: NamespaceClaimId(Bytes::from_static(b"c")),
            stage_kind: StageKind::New,
            fingerprint: FileFingerprint::StatAndContent {
                stat: stat(),
                content: FileContentFingerprint::new(FileHash::new(Bytes::from_static(b"h"))),
            },
            prior_doc_blob: Bytes::new(),
            text_update: Bytes::new(),
            staged_at: time::OffsetDateTime::UNIX_EPOCH,
        };

        assert!(validate_init_staged_rows(&completed, &[staged_row(0, "a.txt")]).is_ok());
        // Unknown seq must still be rejected.
        assert!(validate_init_staged_rows(&completed, &[staged_row(7, "a.txt")]).is_err());
        // Path mismatch must still be rejected.
        assert!(validate_init_staged_rows(&completed, &[staged_row(1, "a.txt")]).is_err());
    }
}
