use crate::semantic::service::SemanticService;
use crate::semantic::types::{
    ImportActionId, ImportActionKind, ImportActionResult, ImportEpoch, ImportEpochStarted,
    NextWork, ProjectionActionCommitContext, ProjectionActionId, ProjectionActionResult,
    ProjectionEpoch, ProjectionEpochEndReason, ProjectionEpochStarted, ProjectionGeneration,
    SemanticEvent, SemanticRequest,
};
use eyre::eyre;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::BTreeMap;
use std::time::Instant;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::debug;

#[derive(Debug)]
struct EngineHandle {
    tx: UnboundedSender<SemanticEvent>,
}

impl EngineHandle {
    fn send_event(&self, event: SemanticEvent) -> eyre::Result<()> {
        self.tx.send(event)?;
        Ok(())
    }
}

pub struct SemanticActor {
    request_rx: UnboundedReceiver<SemanticRequest>,
    engine_handle: EngineHandle,
    service: SemanticService,
    runtime_state: SemanticRuntimeState,
    next_op_id: SemanticOpId,
    deferred_import_epoch_commit: Option<DeferredImportEpochCommit>,
    deferred_projection_epoch_commit: Option<DeferredProjectionEpochCommit>,
    pending_ops: BTreeMap<SemanticOpId, PendingSemanticOp>,
    running_ops: FuturesUnordered<BoxFuture<'static, SemanticTaskResult>>,
}

impl SemanticActor {
    pub fn new(
        request_rx: UnboundedReceiver<SemanticRequest>,
        event_tx: UnboundedSender<SemanticEvent>,
        service: SemanticService,
    ) -> SemanticActor {
        Self {
            request_rx,
            engine_handle: EngineHandle { tx: event_tx },
            service,
            runtime_state: SemanticRuntimeState::default(),
            next_op_id: SemanticOpId(0),
            deferred_import_epoch_commit: None,
            deferred_projection_epoch_commit: None,
            pending_ops: BTreeMap::new(),
            running_ops: FuturesUnordered::new(),
        }
    }

    fn next_op_id(&mut self) -> SemanticOpId {
        let id = self.next_op_id;
        self.next_op_id.0 += 1;
        id
    }

    pub async fn run(mut self, token: CancellationToken) -> eyre::Result<()> {
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    debug!("cancelled");

                    return Ok(());
                }

                // Poll running operations.
                Some(result) = self.running_ops.next(), if !self.running_ops.is_empty() => {
                    self.finish_task_result(result).await?;
                }

                // New requests from engine.
                Some(request) = self.request_rx.recv() => {
                    self.start_request(request)?;
                }

                // TODO timer block

                else => {
                    return Err(eyre!("unrecoverable error"));
                }
            }
        }
    }

    fn start_request(&mut self, request: SemanticRequest) -> eyre::Result<()> {
        match request {
            SemanticRequest::ApplyInitScan { scan, reply } => {
                let epoch = self.runtime_state.reserve_import_epoch();
                let op_id = self.next_op_id();
                self.pending_ops.insert(
                    op_id,
                    PendingSemanticOp::new(PendingSemanticOpKind::ApplyInitScan { reply }),
                );
                let service = self.service.clone();
                self.running_ops.push(Box::pin(async move {
                    SemanticTaskResult::ApplyInitScan {
                        op_id,
                        result: service.apply_init_scan(scan, epoch).await,
                    }
                }));
            }
            SemanticRequest::ApplyScan { scan, reply } => {
                let epoch = self.runtime_state.reserve_import_epoch();
                let op_id = self.next_op_id();
                self.pending_ops.insert(
                    op_id,
                    PendingSemanticOp::new(PendingSemanticOpKind::ApplyScan { reply }),
                );
                let service = self.service.clone();
                self.running_ops.push(Box::pin(async move {
                    SemanticTaskResult::ApplyScan {
                        op_id,
                        result: service.apply_scan(scan, epoch).await,
                    }
                }));
            }
            SemanticRequest::CommitImportAction { result } => {
                let context = self.runtime_state.validate_import_action(&result);
                let op_id = self.next_op_id();
                self.pending_ops.insert(
                    op_id,
                    PendingSemanticOp::new(PendingSemanticOpKind::CommitImportAction),
                );
                let service = self.service.clone();
                self.running_ops.push(Box::pin(async move {
                    SemanticTaskResult::CommitImportAction {
                        op_id,
                        result: service.commit_import_action(context, result).await,
                    }
                }));
            }
            SemanticRequest::CommitImportEpoch { epoch, reply } => {
                self.runtime_state.validate_import_epoch_commit(epoch);
                assert!(
                    self.deferred_import_epoch_commit.is_none(),
                    "duplicate deferred import epoch commit request"
                );

                if self.runtime_state.import_epoch_complete(epoch) {
                    self.start_commit_import_epoch(epoch, reply);
                } else {
                    self.deferred_import_epoch_commit =
                        Some(DeferredImportEpochCommit { epoch, reply });
                }
            }
            SemanticRequest::CommitProjectionAction { result } => {
                self.runtime_state.validate_projection_action(&result);
                let op_id = self.next_op_id();
                self.pending_ops.insert(
                    op_id,
                    PendingSemanticOp::new(PendingSemanticOpKind::CommitProjectionAction),
                );
                let service = self.service.clone();
                self.running_ops.push(Box::pin(async move {
                    SemanticTaskResult::CommitProjectionAction {
                        op_id,
                        result: service.commit_projection_action(result).await,
                    }
                }));
            }
            SemanticRequest::CommitProjectionEpoch {
                epoch,
                reason,
                reply,
            } => {
                self.runtime_state.validate_projection_epoch_commit(epoch);
                assert!(
                    self.deferred_projection_epoch_commit.is_none(),
                    "duplicate deferred projection epoch commit request"
                );

                if self.projection_epoch_ready_to_commit(epoch, reason) {
                    self.start_commit_projection_epoch(epoch, reason, reply);
                } else {
                    self.deferred_projection_epoch_commit = Some(DeferredProjectionEpochCommit {
                        epoch,
                        reason,
                        reply,
                    });
                }
            }
            SemanticRequest::GetNextWork { reply } => {
                let projection_epoch = self.runtime_state.reserve_projection_epoch();
                let op_id = self.next_op_id();
                self.pending_ops.insert(
                    op_id,
                    PendingSemanticOp::new(PendingSemanticOpKind::GetNextWork { reply }),
                );
                let service = self.service.clone();
                self.running_ops.push(Box::pin(async move {
                    SemanticTaskResult::GetNextWork {
                        op_id,
                        result: service.get_next_work(projection_epoch).await,
                    }
                }));
            }
            SemanticRequest::ReadOutbox {
                num_messages,
                reply,
            } => {
                let op_id = self.next_op_id();
                self.pending_ops.insert(
                    op_id,
                    PendingSemanticOp::new(PendingSemanticOpKind::ReadOutbox { reply }),
                );
                let service = self.service.clone();
                self.running_ops.push(Box::pin(async move {
                    SemanticTaskResult::ReadOutbox {
                        op_id,
                        result: service.read_outbox(num_messages).await,
                    }
                }));
            }
            SemanticRequest::TrimOutbox { through } => {
                let op_id = self.next_op_id();
                self.pending_ops.insert(
                    op_id,
                    PendingSemanticOp::new(PendingSemanticOpKind::TrimOutbox),
                );
                let service = self.service.clone();
                self.running_ops.push(Box::pin(async move {
                    SemanticTaskResult::TrimOutbox {
                        op_id,
                        result: service.trim_outbox(through).await,
                    }
                }));
            }
            SemanticRequest::ApplySharedMessageBatch { batch, reply } => {
                let op_id = self.next_op_id();
                self.pending_ops.insert(
                    op_id,
                    PendingSemanticOp::new(PendingSemanticOpKind::ApplySharedMessageBatch {
                        reply,
                    }),
                );
                let service = self.service.clone();
                self.running_ops.push(Box::pin(async move {
                    SemanticTaskResult::ApplySharedMessageBatch {
                        op_id,
                        result: service.apply_shared_message_batch(batch).await,
                    }
                }));
            }
        }

        Ok(())
    }

    async fn finish_task_result(&mut self, result: SemanticTaskResult) -> eyre::Result<()> {
        let op_id = result.op_id();
        let pending = self.take_pending_op(op_id)?;

        match (result, pending.kind) {
            (
                SemanticTaskResult::ApplyInitScan { result, .. },
                PendingSemanticOpKind::ApplyInitScan { reply },
            ) => {
                let result = result.map(|started| {
                    self.runtime_state
                        .install_import_epoch(&started, ImportEpochKind::Init);
                    NextWork::Import(started)
                });
                let _ = reply.send(result);
            }
            (
                SemanticTaskResult::ApplyScan { result, .. },
                PendingSemanticOpKind::ApplyScan { reply },
            ) => {
                let result = result.and_then(|output| {
                    output.effects.emit(&self.engine_handle)?;
                    if let NextWork::Import(started) = &output.next_work {
                        self.runtime_state
                            .install_import_epoch(started, ImportEpochKind::Sync);
                    }
                    Ok(output.next_work)
                });
                let _ = reply.send(result);
            }
            (
                SemanticTaskResult::CommitImportAction { result, .. },
                PendingSemanticOpKind::CommitImportAction,
            ) => {
                let action_id = result?.action_id;
                self.runtime_state.finish_import_action(action_id);
                self.start_ready_deferred_import_epoch_commit();
            }
            (
                SemanticTaskResult::ApplySharedMessageBatch { result, .. },
                PendingSemanticOpKind::ApplySharedMessageBatch { reply },
            ) => {
                let result = result.and_then(|output| {
                    output.effects.emit(&self.engine_handle)?;
                    Ok(())
                });
                let _ = reply.send(result);
            }
            (
                SemanticTaskResult::CommitImportEpoch { result, .. },
                PendingSemanticOpKind::CommitImportEpoch { reply },
            ) => {
                let result = result.and_then(|output| output.effects.emit(&self.engine_handle));
                let _ = reply.send(result);
            }
            (
                SemanticTaskResult::CommitProjectionAction { result, .. },
                PendingSemanticOpKind::CommitProjectionAction,
            ) => {
                let result = result?;
                self.runtime_state.finish_projection_action(result);
                self.start_ready_deferred_projection_epoch_commit();
            }
            (
                SemanticTaskResult::CommitProjectionEpoch { result, .. },
                PendingSemanticOpKind::CommitProjectionEpoch { reply },
            ) => {
                let _ = reply.send(result);
            }
            (
                SemanticTaskResult::GetNextWork { result, .. },
                PendingSemanticOpKind::GetNextWork { reply },
            ) => {
                let result = result.inspect(|next_work| {
                    if let NextWork::Project(started) = next_work {
                        self.runtime_state.install_projection_epoch(started);
                    }
                });
                let _ = reply.send(result);
            }
            (
                SemanticTaskResult::ReadOutbox { result, .. },
                PendingSemanticOpKind::ReadOutbox { reply },
            ) => {
                let _ = reply.send(result);
            }
            (SemanticTaskResult::TrimOutbox { result, .. }, PendingSemanticOpKind::TrimOutbox) => {
                result?;
            }
            (result, pending) => {
                return Err(eyre!(
                    "semantic op {op_id:?} completed with mismatched pending kind: result={}, pending={}",
                    result.kind(),
                    pending.kind(),
                ));
            }
        }

        Ok(())
    }

    fn take_pending_op(&mut self, op_id: SemanticOpId) -> eyre::Result<PendingSemanticOp> {
        self.pending_ops
            .remove(&op_id)
            .ok_or_else(|| eyre!("semantic op {op_id:?} completed but was not pending"))
    }

    fn start_ready_deferred_import_epoch_commit(&mut self) {
        let Some(deferred) = &self.deferred_import_epoch_commit else {
            return;
        };
        if !self.runtime_state.import_epoch_complete(deferred.epoch) {
            return;
        }

        let deferred = self
            .deferred_import_epoch_commit
            .take()
            .expect("deferred import epoch commit was present");
        self.start_commit_import_epoch(deferred.epoch, deferred.reply);
    }

    fn start_ready_deferred_projection_epoch_commit(&mut self) {
        let Some(deferred) = &self.deferred_projection_epoch_commit else {
            return;
        };
        if !self.projection_epoch_ready_to_commit(deferred.epoch, deferred.reason) {
            return;
        }

        let deferred = self
            .deferred_projection_epoch_commit
            .take()
            .expect("deferred projection epoch commit was present");
        self.start_commit_projection_epoch(deferred.epoch, deferred.reason, deferred.reply);
    }

    fn start_commit_import_epoch(
        &mut self,
        epoch: ImportEpoch,
        reply: oneshot::Sender<eyre::Result<()>>,
    ) {
        let completed = self.runtime_state.finish_import_epoch(epoch);
        let op_id = self.next_op_id();
        self.pending_ops.insert(
            op_id,
            PendingSemanticOp::new(PendingSemanticOpKind::CommitImportEpoch { reply }),
        );
        let service = self.service.clone();
        self.running_ops.push(Box::pin(async move {
            SemanticTaskResult::CommitImportEpoch {
                op_id,
                result: service.commit_import_epoch(completed).await,
            }
        }));
    }

    fn start_commit_projection_epoch(
        &mut self,
        epoch: ProjectionEpoch,
        reason: ProjectionEpochEndReason,
        reply: oneshot::Sender<eyre::Result<()>>,
    ) {
        let completed = self.runtime_state.finish_projection_epoch(epoch, reason);
        let op_id = self.next_op_id();
        self.pending_ops.insert(
            op_id,
            PendingSemanticOp::new(PendingSemanticOpKind::CommitProjectionEpoch { reply }),
        );
        let service = self.service.clone();
        self.running_ops.push(Box::pin(async move {
            SemanticTaskResult::CommitProjectionEpoch {
                op_id,
                result: service.commit_projection_epoch(completed, reason).await,
            }
        }));
    }

    fn projection_epoch_ready_to_commit(
        &self,
        epoch: ProjectionEpoch,
        reason: ProjectionEpochEndReason,
    ) -> bool {
        self.runtime_state.validate_projection_epoch_commit(epoch);
        if self.projection_action_commits_in_flight() {
            return false;
        }

        match reason {
            ProjectionEpochEndReason::PlanExhausted => {
                self.runtime_state.projection_epoch_complete(epoch)
            }
            ProjectionEpochEndReason::ActionInvalidatedProjection { .. } => true,
        }
    }

    fn projection_action_commits_in_flight(&self) -> bool {
        self.pending_ops
            .values()
            .any(|op| matches!(&op.kind, PendingSemanticOpKind::CommitProjectionAction))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SemanticOpId(u64);

struct PendingSemanticOp {
    kind: PendingSemanticOpKind,
    #[allow(dead_code)] // reserved for stall detection
    started_at: Instant,
}

struct DeferredImportEpochCommit {
    epoch: ImportEpoch,
    reply: oneshot::Sender<eyre::Result<()>>,
}

struct DeferredProjectionEpochCommit {
    epoch: ProjectionEpoch,
    reason: ProjectionEpochEndReason,
    reply: oneshot::Sender<eyre::Result<()>>,
}

impl PendingSemanticOp {
    fn new(kind: PendingSemanticOpKind) -> Self {
        Self {
            kind,
            started_at: Instant::now(),
        }
    }
}

#[derive(Default)]
struct SemanticRuntimeState {
    next_import_epoch: u64,
    next_projection_epoch: u64,
    active_import: Option<ActiveImportEpoch>,
    active_projection: Option<ActiveProjectionEpoch>,
}

impl SemanticRuntimeState {
    fn reserve_import_epoch(&mut self) -> ImportEpoch {
        if let Some(active) = &self.active_import {
            panic!(
                "cannot reserve import epoch while import epoch {:?} is active",
                active.epoch
            );
        }

        let epoch = ImportEpoch::new(self.next_import_epoch);
        self.next_import_epoch += 1;
        epoch
    }

    fn install_import_epoch(&mut self, started: &ImportEpochStarted, kind: ImportEpochKind) {
        if let Some(active) = &self.active_import {
            panic!(
                "cannot install import epoch {:?} while import epoch {:?} is active",
                started.epoch, active.epoch
            );
        }

        let mut expected = BTreeMap::new();
        for action in &started.plan.actions {
            assert_eq!(
                action.id.epoch, started.epoch,
                "import action {:?} does not belong to import epoch {:?}",
                action.id, started.epoch
            );
            assert!(
                expected.insert(action.id, action.kind.clone()).is_none(),
                "duplicate import action id in plan: {:?}",
                action.id
            );
        }

        self.active_import = Some(ActiveImportEpoch {
            epoch: started.epoch,
            kind,
            expected,
            completed: BTreeMap::new(),
        });
    }

    fn reserve_projection_epoch(&mut self) -> ProjectionEpoch {
        if let Some(active) = &self.active_projection {
            panic!(
                "cannot reserve projection epoch while projection epoch {:?} is active",
                active.epoch
            );
        }

        let epoch = ProjectionEpoch::new(self.next_projection_epoch);
        self.next_projection_epoch += 1;
        epoch
    }

    fn install_projection_epoch(&mut self, started: &ProjectionEpochStarted) {
        if let Some(active) = &self.active_projection {
            panic!(
                "cannot install projection epoch {:?} while projection epoch {:?} is active",
                started.epoch, active.epoch
            );
        }

        let mut expected = BTreeMap::new();
        for action in &started.plan.actions {
            assert_eq!(
                action.id.epoch, started.epoch,
                "projection action {:?} does not belong to projection epoch {:?}",
                action.id, started.epoch
            );
            assert!(
                expected
                    .insert(action.id, action.commit_context.clone())
                    .is_none(),
                "duplicate projection action id in plan: {:?}",
                action.id
            );
        }

        self.active_projection = Some(ActiveProjectionEpoch {
            epoch: started.epoch,
            generation: started.generation,
            target_namespace_doc_blob: started.target_namespace_doc_blob.clone(),
            expected,
            completed: BTreeMap::new(),
        });
    }

    fn validate_import_action(&self, result: &ImportActionResult) -> ImportActionContext {
        let action_id = result.action_id();
        let active = self
            .active_import
            .as_ref()
            .expect("import action requested with no active import epoch");
        assert_eq!(
            action_id.epoch, active.epoch,
            "import action requested for epoch {:?}, but active epoch is {:?}",
            action_id.epoch, active.epoch
        );
        assert!(
            active.expected.contains_key(&action_id),
            "unexpected import action requested: {action_id:?}"
        );
        assert!(
            !active.completed.contains_key(&action_id),
            "duplicate import action requested: {action_id:?}"
        );

        ImportActionContext {
            epoch_kind: active.kind,
            action_kind: active
                .expected
                .get(&action_id)
                .expect("validated import action exists in expected map")
                .clone(),
        }
    }

    fn finish_import_action(&mut self, action_id: ImportActionId) {
        let active = self
            .active_import
            .as_mut()
            .expect("import action committed with no active import epoch");
        assert_eq!(
            action_id.epoch, active.epoch,
            "import action committed for epoch {:?}, but active epoch is {:?}",
            action_id.epoch, active.epoch
        );
        assert!(
            active.expected.contains_key(&action_id),
            "unexpected import action committed: {action_id:?}"
        );
        assert!(
            active.completed.insert(action_id, ()).is_none(),
            "duplicate import action commit: {action_id:?}"
        );
    }

    fn validate_import_epoch_commit(&self, epoch: ImportEpoch) {
        let active = self
            .active_import
            .as_ref()
            .expect("cannot commit import epoch; no import is active");
        assert_eq!(
            active.epoch, epoch,
            "cannot commit import epoch {:?}; active epoch is {:?}",
            epoch, active.epoch
        );
    }

    fn import_epoch_complete(&self, epoch: ImportEpoch) -> bool {
        self.validate_import_epoch_commit(epoch);
        let active = self
            .active_import
            .as_ref()
            .expect("validated import epoch has active import");
        active.completed.len() == active.expected.len()
    }

    fn finish_import_epoch(&mut self, epoch: ImportEpoch) -> CompletedImportEpoch {
        let active = self
            .active_import
            .take()
            .expect("cannot commit import epoch; no import is active");
        assert_eq!(
            active.epoch, epoch,
            "cannot commit import epoch {:?}; active epoch is {:?}",
            epoch, active.epoch
        );
        if active.completed.len() != active.expected.len() {
            let missing: Vec<_> = active
                .expected
                .keys()
                .filter(|action_id| !active.completed.contains_key(action_id))
                .copied()
                .collect();
            panic!("cannot commit import epoch {epoch:?}; missing actions: {missing:?}");
        }

        let ActiveImportEpoch {
            epoch,
            kind,
            expected,
            mut completed,
        } = active;

        let mut actions = Vec::new();
        for (action_id, action_kind) in expected {
            completed
                .remove(&action_id)
                .expect("completed import action missing after completeness check");
            actions.push(CompletedImportAction {
                action_id,
                action_kind,
            });
        }

        CompletedImportEpoch {
            epoch,
            kind,
            actions,
        }
    }

    fn validate_projection_action(&self, result: &ProjectionActionResult) {
        let action_id = result.action_id();
        let active = self
            .active_projection
            .as_ref()
            .expect("projection action requested with no active projection epoch");
        assert_eq!(
            action_id.epoch, active.epoch,
            "projection action requested for epoch {:?}, but active epoch is {:?}",
            action_id.epoch, active.epoch
        );
        assert!(
            active.expected.contains_key(&action_id),
            "unexpected projection action requested: {action_id:?}"
        );
        assert!(
            !active.completed.contains_key(&action_id),
            "duplicate projection action requested: {action_id:?}"
        );
    }

    fn finish_projection_action(&mut self, result: ProjectionActionResult) {
        let action_id = result.action_id();
        let active = self
            .active_projection
            .as_mut()
            .expect("projection action committed with no active projection epoch");
        assert_eq!(
            action_id.epoch, active.epoch,
            "projection action committed for epoch {:?}, but active epoch is {:?}",
            action_id.epoch, active.epoch
        );
        assert!(
            active.expected.contains_key(&action_id),
            "unexpected projection action committed: {action_id:?}"
        );
        assert!(
            active.completed.insert(action_id, result).is_none(),
            "duplicate projection action commit: {action_id:?}"
        );
    }

    fn validate_projection_epoch_commit(&self, epoch: ProjectionEpoch) {
        let active = self
            .active_projection
            .as_ref()
            .expect("cannot commit projection epoch; no projection is active");
        assert_eq!(
            active.epoch, epoch,
            "cannot commit projection epoch {:?}; active epoch is {:?}",
            epoch, active.epoch
        );
    }

    fn projection_epoch_complete(&self, epoch: ProjectionEpoch) -> bool {
        self.validate_projection_epoch_commit(epoch);
        let active = self
            .active_projection
            .as_ref()
            .expect("validated projection epoch has active projection");
        active.completed.len() == active.expected.len()
    }

    fn finish_projection_epoch(
        &mut self,
        epoch: ProjectionEpoch,
        reason: ProjectionEpochEndReason,
    ) -> CompletedProjectionEpoch {
        let active = self
            .active_projection
            .take()
            .expect("cannot commit projection epoch; no projection is active");
        assert_eq!(
            active.epoch, epoch,
            "cannot commit projection epoch {:?}; active epoch is {:?}",
            epoch, active.epoch
        );
        let require_complete = matches!(reason, ProjectionEpochEndReason::PlanExhausted);
        if require_complete && active.completed.len() != active.expected.len() {
            let missing: Vec<_> = active
                .expected
                .keys()
                .filter(|action_id| !active.completed.contains_key(action_id))
                .copied()
                .collect();
            panic!("cannot commit projection epoch {epoch:?}; missing actions: {missing:?}");
        }

        let ActiveProjectionEpoch {
            epoch,
            generation,
            target_namespace_doc_blob,
            expected,
            mut completed,
        } = active;

        let mut actions = Vec::new();
        for (action_id, commit_context) in expected {
            if let Some(result) = completed.remove(&action_id) {
                actions.push(CompletedProjectionAction {
                    action_id,
                    commit_context,
                    result,
                });
            } else {
                assert!(
                    !require_complete,
                    "completed projection action missing after completeness check: {action_id:?}"
                );
            }
        }

        CompletedProjectionEpoch {
            epoch,
            generation,
            target_namespace_doc_blob,
            actions,
        }
    }
}

struct ActiveImportEpoch {
    epoch: ImportEpoch,
    kind: ImportEpochKind,
    expected: BTreeMap<ImportActionId, ImportActionKind>,
    completed: BTreeMap<ImportActionId, ()>,
}

struct ActiveProjectionEpoch {
    epoch: ProjectionEpoch,
    generation: ProjectionGeneration,
    target_namespace_doc_blob: bytes::Bytes,
    expected: BTreeMap<ProjectionActionId, ProjectionActionCommitContext>,
    completed: BTreeMap<ProjectionActionId, ProjectionActionResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImportEpochKind {
    Init,
    Sync,
}

pub(crate) struct ImportActionContext {
    pub epoch_kind: ImportEpochKind,
    pub action_kind: ImportActionKind,
}

pub(crate) struct CommitImportActionOutcome {
    pub action_id: ImportActionId,
}

pub(crate) struct CompletedImportEpoch {
    pub epoch: ImportEpoch,
    pub kind: ImportEpochKind,
    pub actions: Vec<CompletedImportAction>,
}

pub(crate) struct CompletedImportAction {
    pub action_id: ImportActionId,
    pub action_kind: ImportActionKind,
}

pub(crate) struct CompletedProjectionEpoch {
    pub epoch: ProjectionEpoch,
    pub generation: ProjectionGeneration,
    pub target_namespace_doc_blob: bytes::Bytes,
    pub actions: Vec<CompletedProjectionAction>,
}

pub(crate) struct CompletedProjectionAction {
    pub action_id: ProjectionActionId,
    pub commit_context: ProjectionActionCommitContext,
    pub result: ProjectionActionResult,
}

#[derive(strum::IntoStaticStr)]
enum PendingSemanticOpKind {
    #[strum(serialize = "apply_init_scan")]
    ApplyInitScan {
        reply: oneshot::Sender<eyre::Result<NextWork>>,
    },
    #[strum(serialize = "apply_scan")]
    ApplyScan {
        reply: oneshot::Sender<eyre::Result<NextWork>>,
    },
    #[strum(serialize = "commit_import_epoch")]
    CommitImportEpoch {
        reply: oneshot::Sender<eyre::Result<()>>,
    },
    #[strum(serialize = "commit_import_action")]
    CommitImportAction,
    #[strum(serialize = "commit_projection_action")]
    CommitProjectionAction,
    #[strum(serialize = "commit_projection_epoch")]
    CommitProjectionEpoch {
        reply: oneshot::Sender<eyre::Result<()>>,
    },
    #[strum(serialize = "get_next_work")]
    GetNextWork {
        reply: oneshot::Sender<eyre::Result<NextWork>>,
    },
    #[strum(serialize = "read_outbox")]
    ReadOutbox {
        reply: oneshot::Sender<
            eyre::Result<Vec<(crate::types::OutboxId, crate::crdt::types::SharedMessage)>>,
        >,
    },
    #[strum(serialize = "trim_outbox")]
    TrimOutbox,
    #[strum(serialize = "apply_shared_message_batch")]
    ApplySharedMessageBatch {
        reply: oneshot::Sender<eyre::Result<()>>,
    },
}

#[derive(strum::IntoStaticStr)]
enum SemanticTaskResult {
    #[strum(serialize = "apply_init_scan")]
    ApplyInitScan {
        op_id: SemanticOpId,
        result: eyre::Result<ImportEpochStarted>,
    },
    #[strum(serialize = "apply_scan")]
    ApplyScan {
        op_id: SemanticOpId,
        result: eyre::Result<ApplyScanOutput>,
    },
    #[strum(serialize = "apply_shared_message_batch")]
    ApplySharedMessageBatch {
        op_id: SemanticOpId,
        result: eyre::Result<ApplySharedMessageBatchOutput>,
    },
    #[strum(serialize = "commit_import_epoch")]
    CommitImportEpoch {
        op_id: SemanticOpId,
        result: eyre::Result<CommitImportEpochOutput>,
    },
    #[strum(serialize = "commit_import_action")]
    CommitImportAction {
        op_id: SemanticOpId,
        result: eyre::Result<CommitImportActionOutcome>,
    },
    #[strum(serialize = "commit_projection_action")]
    CommitProjectionAction {
        op_id: SemanticOpId,
        result: eyre::Result<ProjectionActionResult>,
    },
    #[strum(serialize = "commit_projection_epoch")]
    CommitProjectionEpoch {
        op_id: SemanticOpId,
        result: eyre::Result<()>,
    },
    #[strum(serialize = "get_next_work")]
    GetNextWork {
        op_id: SemanticOpId,
        result: eyre::Result<NextWork>,
    },
    #[strum(serialize = "read_outbox")]
    ReadOutbox {
        op_id: SemanticOpId,
        result: eyre::Result<Vec<(crate::types::OutboxId, crate::crdt::types::SharedMessage)>>,
    },
    #[strum(serialize = "trim_outbox")]
    TrimOutbox {
        op_id: SemanticOpId,
        result: eyre::Result<()>,
    },
}

impl SemanticTaskResult {
    fn op_id(&self) -> SemanticOpId {
        match self {
            Self::ApplyInitScan { op_id, .. }
            | Self::ApplyScan { op_id, .. }
            | Self::CommitImportAction { op_id, .. }
            | Self::CommitImportEpoch { op_id, .. }
            | Self::CommitProjectionAction { op_id, .. }
            | Self::CommitProjectionEpoch { op_id, .. }
            | Self::GetNextWork { op_id, .. }
            | Self::ReadOutbox { op_id, .. }
            | Self::ApplySharedMessageBatch { op_id, .. }
            | Self::TrimOutbox { op_id, .. } => *op_id,
        }
    }

    fn kind(&self) -> &'static str {
        self.into()
    }
}

impl PendingSemanticOpKind {
    fn kind(&self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Default)]
pub(crate) struct SemanticEffects {
    pub projection_changed: Option<ProjectionGeneration>,
    pub outbox_ready: Option<u64>,
}

impl SemanticEffects {
    pub(crate) fn none() -> Self {
        Self::default()
    }

    fn emit(self, engine: &EngineHandle) -> eyre::Result<()> {
        if let Some(generation) = self.projection_changed {
            engine.send_event(SemanticEvent::ProjectionChanged { generation })?;
        }
        if let Some(num_messages) = self.outbox_ready {
            engine.send_event(SemanticEvent::OutboxReady { num_messages })?;
        }
        Ok(())
    }
}

pub(crate) struct ApplySharedMessageBatchOutput {
    pub effects: SemanticEffects,
}

pub(crate) struct ApplyScanOutput {
    pub next_work: NextWork,
    pub effects: SemanticEffects,
}

pub(crate) struct CommitImportEpochOutput {
    pub effects: SemanticEffects,
}
