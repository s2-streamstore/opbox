use crate::crdt::types::{
    NamespaceClaimId, ObjectId, ObjectKind, SharedMessage, SharedMessageKind,
};
use crate::crdt::{namespace, text_doc};
use crate::fs::types::{
    ExpectedBefore, FileFingerprint, GuardedDeleteResult, GuardedReadResult, GuardedWriteResult,
};
use crate::fs::types::{ScanResult, ScanScope, TreeEntryKind};
use crate::log::types::SequenceNumber;
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
use std::ops::{RangeInclusive, RangeToInclusive};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, instrument, trace, warn};
use turso::{Connection, Database};

const TURSO_RETRY_ATTEMPTS: usize = 3;
const TURSO_RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct SemanticService {
    pool: Arc<bb8::Pool<TursoConnectionManager>>,
}

impl SemanticService {
    pub fn new(pool: bb8::Pool<TursoConnectionManager>) -> Self {
        Self {
            pool: Arc::new(pool),
        }
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

                let mut actions = Vec::new();
                let mut next_seq = 0;
                for entry in scan.tree.entries() {
                    match &entry.kind {
                        TreeEntryKind::Directory => {
                            // not handled
                        }
                        TreeEntryKind::File { fingerprint } => {
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

    #[instrument(skip(self))]
    pub(crate) async fn apply_scan(
        &self,
        scan: ScanResult,
        epoch: ImportEpoch,
    ) -> eyre::Result<ApplyScanOutput> {
        let scan = Arc::new(scan);
        self.exec_tx("apply_scan", move |tx| {
            let scan = scan.clone();
            Box::pin(async move {
                let ScanScope::Full = &scan.scope else {
                    return Err(SemanticTransactionError::InvariantViolation(
                        "normal apply_scan only supports full scans in v0".to_string(),
                    ));
                };

                let mut observed_files = BTreeMap::new();
                for entry in scan.tree.entries() {
                    match &entry.kind {
                        TreeEntryKind::Directory => {}
                        TreeEntryKind::File { fingerprint } => {
                            observed_files.insert(entry.path.clone(), fingerprint.clone());
                        }
                    }
                }

                let mut actions = Vec::new();
                let mut missing_prior = Vec::new();
                let mut next_seq = 0;

                for prior in tx.select_prior_tree_live_files().await? {
                    match observed_files.remove(&prior.path) {
                        None => missing_prior.push(prior),
                        Some(observed) if observed.stat() == prior.fingerprint.stat() => {}
                        Some(observed) => {
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

        if let ImportActionStageDecision::Stage(plan) = decision {
            let plan = Arc::new(plan);
            self.exec_tx("commit_import_action", move |tx| {
                let plan = plan.clone();
                Box::pin(async move {
                    stage_import_action_tx(tx, &plan).await?;
                    Ok(())
                })
            })
            .await?;
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
        result: ProjectionActionResult,
    ) -> eyre::Result<ProjectionActionResult> {
        self.exec_tx("commit_projection_action", move |tx| {
            Box::pin(async move {
                let _tx = tx;
                Ok(())
            })
        })
        .await?;

        Ok(result)
    }

    pub(crate) async fn commit_projection_epoch(
        &self,
        completed: CompletedProjectionEpoch,
        reason: ProjectionEpochEndReason,
    ) -> eyre::Result<NextWork> {
        let completed = Arc::new(completed);
        self.exec_tx("commit_projection_epoch", move |tx| {
            let completed = completed.clone();
            Box::pin(async move {
                commit_projection_epoch_tx(tx, &completed, reason).await?;
                Ok(NextWork::None)
            })
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
        let consolidated = Arc::new(consolidate_shared_message_batch(batch)?);
        self.exec_tx("apply_shared_message_batch", move |tx| {
            let consolidated = consolidated.clone();
            Box::pin(async move { apply_consolidated_shared_batch_tx(tx, &consolidated).await })
        })
        .await
    }

    pub(crate) async fn read_outbox(
        &self,
        num_messages: u64,
    ) -> eyre::Result<Vec<(OutboxId, SharedMessage)>> {
        self.exec_tx("read_outbox", move |tx| {
            Box::pin(async move { tx.select_outbox_messages(num_messages).await })
        })
        .await
    }

    pub(crate) async fn trim_outbox(
        &self,
        through: RangeToInclusive<OutboxId>,
    ) -> eyre::Result<()> {
        self.exec_tx("trim_outbox", move |tx| {
            Box::pin(async move {
                tx.trim_outbox(through.clone()).await?;
                Ok(())
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
                    tokio::time::sleep(TURSO_RETRY_DELAY).await;
                    continue;
                }
                Err(err) => return Err(err.into_report()),
            };

            let result = match body(&tx).await {
                Ok(value) => tx
                    .commit()
                    .await
                    .map(|_| value)
                    .map_err(SemanticTransactionError::from),
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
                    tokio::time::sleep(TURSO_RETRY_DELAY).await;
                }
                Err(err) => {
                    let _ = tx.rollback().await;
                    return Err(err.into_report());
                }
            }
        }
    }
}

#[derive(Clone)]
struct ConsolidatedSharedBatch {
    sequence_range: RangeInclusive<SequenceNumber>,
    max_timestamp: time::OffsetDateTime,
    namespace_update: Option<bytes::Bytes>,
    text_updates: BTreeMap<ObjectId, bytes::Bytes>,
}

fn consolidate_shared_message_batch(
    batch: SharedMessageBatch,
) -> eyre::Result<ConsolidatedSharedBatch> {
    let mut max_timestamp: Option<time::OffsetDateTime> = None;
    let mut namespace_updates = Vec::new();
    let mut text_updates = BTreeMap::<ObjectId, Vec<bytes::Bytes>>::new();

    for envelope in batch.messages {
        max_timestamp = Some(match max_timestamp {
            Some(current) => current.max(envelope.timestamp),
            None => envelope.timestamp,
        });

        match envelope.shared_message {
            SharedMessage::NamespaceUpdate { yjs_update } => {
                namespace_updates.push(yjs_update);
            }
            SharedMessage::TextObjectUpdate {
                object_id,
                yjs_update,
            } => {
                text_updates.entry(object_id).or_default().push(yjs_update);
            }
            SharedMessage::BinaryObjectPut { .. } => {
                eyre::bail!("binary shared messages are not supported in v0");
            }
        }
    }

    let namespace_update = if namespace_updates.is_empty() {
        None
    } else {
        let doc = namespace::NamespaceDoc::new(30_000_000);
        for update in namespace_updates {
            doc.apply_update(update.as_ref())?;
        }
        Some(doc.encode_full_state())
    };

    let mut merged_text_updates = BTreeMap::new();
    for (object_id, updates) in text_updates {
        let doc = text_doc::TextObjectDoc::new(40_000_000);
        for update in updates {
            doc.apply_update(update.as_ref())?;
        }
        merged_text_updates.insert(object_id, doc.encode_full_state());
    }

    Ok(ConsolidatedSharedBatch {
        sequence_range: batch.sequence_range,
        max_timestamp: max_timestamp
            .ok_or_else(|| eyre::eyre!("cannot consolidate empty shared message batch"))?,
        namespace_update,
        text_updates: merged_text_updates,
    })
}

async fn apply_consolidated_shared_batch_tx(
    tx: &SemanticTransaction<'_>,
    batch: &ConsolidatedSharedBatch,
) -> Result<ApplySharedMessageBatchOutput, SemanticTransactionError> {
    let stable_cursor = tx.select_stable_cursor().await?;
    if stable_cursor.end != *batch.sequence_range.start() {
        return Err(SemanticTransactionError::InvariantViolation(format!(
            "unexpected batch; stable_cursor={:?}, batch.sequence_range={:?}",
            stable_cursor, batch.sequence_range
        )));
    }

    if let Some(namespace_update) = &batch.namespace_update {
        apply_stable_namespace_update_tx(tx, namespace_update, batch.max_timestamp).await?;
    }

    for (object_id, update) in &batch.text_updates {
        let base = tx
            .select_stable_text_object_doc(object_id)
            .await?
            .unwrap_or_else(|| text_doc::empty_text_state(1));
        let next = text_doc::apply_text_update(base.as_ref(), update.as_ref())?;
        tx.upsert_stable_text_object(object_id, next.full_state_bytes, batch.max_timestamp)
            .await?;
    }

    tx.update_stable_cursor(batch.sequence_range.clone())
        .await?;

    let next_cursor = batch.sequence_range.end().checked_add(1).ok_or_else(|| {
        SemanticTransactionError::InvariantViolation("stable cursor overflow".to_string())
    })?;

    Ok(ApplySharedMessageBatchOutput {
        effects: SemanticEffects {
            projection_changed: Some(ProjectionGeneration::new(next_cursor)),
            outbox_ready: None,
        },
    })
}

async fn apply_stable_namespace_update_tx(
    tx: &SemanticTransaction<'_>,
    update: &bytes::Bytes,
    updated_at: time::OffsetDateTime,
) -> Result<(), SemanticTransactionError> {
    let stable_namespace = tx.select_stable_namespace().await?.ok_or_else(|| {
        SemanticTransactionError::InvariantViolation("missing stable_namespace".to_string())
    })?;
    let doc = namespace::NamespaceDoc::from_full_state(1, stable_namespace.doc_blob)?;
    doc.apply_update(update.as_ref())?;
    write_stable_namespace_projection_tx(tx, &doc, updated_at).await
}

async fn write_stable_namespace_projection_tx(
    tx: &SemanticTransaction<'_>,
    doc: &namespace::NamespaceDoc,
    updated_at: time::OffsetDateTime,
) -> Result<(), SemanticTransactionError> {
    let projection = doc.materialize()?;

    tx.update_stable_namespace(doc.encode_full_state(), updated_at)
        .await?;
    for object in projection.confirmed_objects {
        match object.meta.kind {
            ObjectKind::Text => {
                tx.insert_or_ignore_object(
                    &object.object_id,
                    ObjectKind::Text,
                    &object.meta.creator_writer_id,
                    updated_at,
                )
                .await?;
            }
            other => {
                return Err(SemanticTransactionError::InvariantViolation(format!(
                    "unsupported namespace object kind in v0: {other:?}"
                )));
            }
        }
    }

    tx.delete_stable_tree().await?;
    for placement in projection.placements {
        match placement.kind {
            ObjectKind::Text => {
                tx.insert_stable_tree_file(
                    &placement.path,
                    &placement.claimed_path,
                    &placement.claim_id,
                    &placement.object_id,
                    updated_at,
                )
                .await?;
            }
            other => {
                return Err(SemanticTransactionError::InvariantViolation(format!(
                    "unsupported namespace placement kind in v0: {other:?}"
                )));
            }
        }
    }

    Ok(())
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
    let stable_cursor = tx.select_stable_cursor().await?;
    let generation = ProjectionGeneration::new(stable_cursor.end);
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
        let action_id = ProjectionActionId::new(projection_epoch, next_seq);
        next_seq += 1;
        actions.push(ProjectionAction {
            id: action_id,
            kind: ProjectionActionKind::WriteFile {
                path: row.path.clone(),
                bytes: Bytes::from(text_state.text.into_bytes()),
                expected_before,
            },
            commit_context: ProjectionActionCommitContext {
                path: row.path,
                claimed_path: row.claimed_path,
                claim_id: NamespaceClaimId(row.claim_id),
                object_id: row.object_id,
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
            },
        });
    }

    if actions.is_empty() {
        Ok(NextWork::None)
    } else {
        Ok(NextWork::Project(ProjectionEpochStarted {
            epoch: projection_epoch,
            generation,
            plan: ProjectionPlan { actions },
        }))
    }
}

async fn commit_projection_epoch_tx(
    tx: &SemanticTransaction<'_>,
    completed: &CompletedProjectionEpoch,
    reason: ProjectionEpochEndReason,
) -> Result<(), SemanticTransactionError> {
    match reason {
        ProjectionEpochEndReason::PlanExhausted => {}
        ProjectionEpochEndReason::ProjectionChanged { .. }
        | ProjectionEpochEndReason::ActionInvalidatedProjection { .. } => {
            // A stale/invalidated projection must not advance the prior baseline.
            return Ok(());
        }
    }

    let stable_cursor = tx.select_stable_cursor().await?;
    let current_generation = ProjectionGeneration::new(stable_cursor.end);
    if current_generation != completed.generation {
        return Err(SemanticTransactionError::InvariantViolation(format!(
            "cannot commit projection epoch {:?}; generation changed from {:?} to {:?}",
            completed.epoch, completed.generation, current_generation
        )));
    }

    let accepted_at = time::OffsetDateTime::now_utc();
    let stable_namespace = tx.select_stable_namespace().await?.ok_or_else(|| {
        SemanticTransactionError::InvariantViolation("missing stable_namespace".to_string())
    })?;
    tx.update_prior_namespace(stable_namespace.doc_blob, accepted_at)
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
            let stable_doc_blob = tx
                .select_stable_text_object_doc(&action.commit_context.object_id)
                .await?
                .unwrap_or_else(|| text_doc::empty_text_state(1));
            tx.upsert_prior_text_object(
                &action.commit_context.object_id,
                stable_doc_blob,
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
            Ok(())
        }
        ProjectionActionResult::WriteFile { result, .. } => {
            Err(SemanticTransactionError::InvariantViolation(format!(
                "cannot commit invalidated projection write result for action {:?}: {}",
                action.action_id,
                guarded_write_result_kind(result)
            )))
        }
        ProjectionActionResult::DeleteFile {
            result: GuardedDeleteResult::Deleted | GuardedDeleteResult::AlreadyDeleted,
            ..
        } => {
            tx.tombstone_prior_tree_file(&action.commit_context.path, accepted_at)
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

fn guarded_write_result_kind(result: &GuardedWriteResult) -> &'static str {
    match result {
        GuardedWriteResult::Written { .. } => "written",
        GuardedWriteResult::AlreadyApplied { .. } => "already_applied",
        GuardedWriteResult::ConflictBeforeSwap { .. } => "conflict_before_swap",
        GuardedWriteResult::ConflictAfterSwap { .. } => "conflict_after_swap",
        GuardedWriteResult::Conflict { .. } => "conflict",
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
            next_work: NextWork::None,
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
        next_work: NextWork::None,
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
    let staged = tx.select_import_staged_files(completed.epoch).await?;
    validate_sync_staged_rows(completed, &staged)?;

    if staged.is_empty() {
        tx.delete_import_staged_files(completed.epoch).await?;
        return Ok(CommitImportEpochOutput {
            next_work: NextWork::None,
            effects: SemanticEffects::none(),
        });
    }

    let now = time::OffsetDateTime::now_utc();
    let writer_id = tx.select_daemon_writer_id().await?;
    let prior_namespace = tx.select_prior_namespace().await?.ok_or_else(|| {
        SemanticTransactionError::InvariantViolation("missing prior_namespace".to_string())
    })?;
    let stable_namespace = tx.select_stable_namespace().await?.ok_or_else(|| {
        SemanticTransactionError::InvariantViolation("missing stable_namespace".to_string())
    })?;

    let namespace_client_id = namespace::client_id_for_writer(writer_id.0.as_ref());
    let prior_namespace_doc =
        namespace::NamespaceDoc::from_full_state(namespace_client_id, prior_namespace.doc_blob)?;
    let prior_namespace_sv_before = prior_namespace_doc.state_vector();

    for row in &staged {
        match row.stage_kind {
            StageKind::New => {
                prior_namespace_doc.add_new_object(&row.object_id, ObjectKind::Text, &writer_id);
                prior_namespace_doc.add_new_claim(&row.claim_id, &row.object_id, &row.path);
            }
            StageKind::Update => {}
        }
    }

    let namespace_update = prior_namespace_doc.encode_update_since(&prior_namespace_sv_before);
    tx.update_prior_namespace(prior_namespace_doc.encode_full_state(), now)
        .await?;
    if !namespace_update.is_empty() {
        let stable_namespace_doc =
            namespace::NamespaceDoc::from_full_state(1, stable_namespace.doc_blob)?;
        stable_namespace_doc.apply_update(namespace_update.as_ref())?;
        write_stable_namespace_projection_tx(tx, &stable_namespace_doc, now).await?;
    }

    for row in &staged {
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
    }

    let outbox_message_count = u64::from(!namespace_update.is_empty())
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
        if !namespace_update.is_empty() {
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

    tx.delete_import_staged_files(completed.epoch).await?;

    Ok(CommitImportEpochOutput {
        next_work: NextWork::None,
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
    if staged.len() != completed.actions.len() {
        return Err(SemanticTransactionError::InvariantViolation(format!(
            "import epoch {:?} completed {} actions but staged {} rows",
            completed.epoch,
            completed.actions.len(),
            staged.len()
        )));
    }

    for (action, row) in completed.actions.iter().zip(staged) {
        if action.action_id.epoch != completed.epoch {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "completed import action {:?} is not in epoch {:?}",
                action.action_id, completed.epoch
            )));
        }
        if action.action_id.seq != row.action_seq || action.action_id.epoch != row.import_epoch {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "completed import action {:?} does not match staged row epoch={:?} seq={}",
                action.action_id, row.import_epoch, row.action_seq
            )));
        }
        if row.stage_kind != StageKind::New {
            return Err(SemanticTransactionError::InvariantViolation(format!(
                "init import action {:?} staged non-new row: {:?}",
                action.action_id, row.stage_kind
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
    Ignore { action_id: ImportActionId },
}

impl ImportActionStageDecision {
    fn action_id(&self) -> ImportActionId {
        match self {
            ImportActionStageDecision::Stage(plan) => plan.action_id,
            ImportActionStageDecision::Ignore { action_id } => *action_id,
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

                let content = match String::from_utf8(bytes.to_vec()) {
                    Ok(content) => content,
                    Err(err) if epoch_kind == ImportEpochKind::Sync => {
                        debug!(
                            ?action_id,
                            %path,
                            error = %err,
                            "ignoring non-UTF-8 file during sync import"
                        );
                        return Ok(ImportActionStageDecision::Ignore { action_id });
                    }
                    Err(err) => return Err(err.into()),
                };

                Ok(ImportActionStageDecision::Stage(ImportActionStagePlan {
                    action_id,
                    epoch_kind,
                    path,
                    fingerprint,
                    content,
                }))
            }
            ImportReadOutcome::Completed(GuardedReadResult::ChangedBetweenStats {
                before,
                after,
            }) => {
                eyre::bail!(
                    "import read {:?} changed between stats; before={before:?}, after={after:?}",
                    action_id
                );
            }
            ImportReadOutcome::Completed(GuardedReadResult::ConflictBeforeRead { observed }) => {
                eyre::bail!(
                    "import read {:?} conflicted before read; observed={observed:?}",
                    action_id
                );
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
                let object_id = prior.object_id;
                let claim_id = NamespaceClaimId(prior.claim_id);
                let prior_doc_blob = tx
                    .select_prior_text_object_doc(&object_id)
                    .await?
                    .unwrap_or_else(|| text_doc::empty_text_state(client_id));
                let prior_text =
                    text_doc::decode_text_state(client_id, prior_doc_blob.as_ref())?.text;
                let capture = text_doc::capture_text_change(
                    prior_doc_blob.as_ref(),
                    &prior_text,
                    &plan.content,
                )?;
                match capture {
                    Some(capture) => StagedTextImport {
                        stage_kind: StageKind::Update,
                        object_id,
                        claim_id,
                        prior_doc_blob: capture.full_state_bytes,
                        text_update: capture.update_bytes,
                    },
                    None => StagedTextImport {
                        stage_kind: StageKind::Update,
                        object_id,
                        claim_id,
                        prior_doc_blob,
                        text_update: Bytes::new(),
                    },
                }
            }
            None => stage_new_text_file(
                client_id,
                &plan.content,
                ObjectId::generate(),
                NamespaceClaimId::generate(),
            ),
        },
    };

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

    tx.insert_import_staged_file(&row).await
}

struct StagedTextImport {
    stage_kind: StageKind,
    object_id: ObjectId,
    claim_id: NamespaceClaimId,
    prior_doc_blob: Bytes,
    text_update: Bytes,
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
