use crate::fs::types::{
    DeleteIfExistsResult, FsRequest, GuardedDeleteResult, GuardedReadResult, GuardedWriteResult,
    RelativePath, ScanResult, ScanScope,
};
use crate::semantic::types::{
    ImportAction, ImportActionKind, ImportActionResult, ImportReadOutcome, ImportStatOutcome,
    ProjectionAction, ProjectionActionKind, ProjectionActionResult,
};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

const MAX_CLEANUP_IN_FLIGHT: usize = 16;

pub enum FsClientResponse {
    Scan {
        result: eyre::Result<ScanResult>,
    },
    ApplyImportAction {
        result: ImportActionResult,
    },
    ApplyProjectionAction {
        result: eyre::Result<ProjectionActionResult>,
    },
    DeleteIfExists {
        path: RelativePath,
        result: eyre::Result<DeleteIfExistsResult>,
    },
}

pub struct FsClient {
    tx: UnboundedSender<FsRequest>,
    pending: FuturesUnordered<BoxFuture<'static, FsClientResponse>>,
    /// Import/projection/scan work stays single-flight in v0; cleanup is separate
    /// because it is best-effort daemon-owned temp deletion, not semantic progress.
    phase_inflight: usize,
    cleanup_inflight: usize,
}

impl FsClient {
    pub fn new(tx: UnboundedSender<FsRequest>) -> Self {
        Self {
            tx,
            pending: FuturesUnordered::new(),
            phase_inflight: 0,
            cleanup_inflight: 0,
        }
    }

    pub fn inflight_count(&self) -> usize {
        self.pending.len()
    }

    pub fn can_accept_projection(&self) -> bool {
        // Keep projection single-flight for now.
        //
        // Later this can allow safe
        // parallelism for non-overlapping projection actions.
        self.phase_inflight == 0
    }

    pub fn can_accept_import(&self) -> bool {
        self.phase_inflight == 0
    }

    pub fn can_accept_scan(&self) -> bool {
        self.phase_inflight == 0
    }

    pub fn can_accept_cleanup(&self) -> bool {
        self.cleanup_inflight < MAX_CLEANUP_IN_FLIGHT
    }

    pub async fn next(&mut self) -> Option<FsClientResponse> {
        let response = self.pending.next().await?;

        match &response {
            FsClientResponse::Scan { .. }
            | FsClientResponse::ApplyImportAction { .. }
            | FsClientResponse::ApplyProjectionAction { .. } => {
                self.phase_inflight = self
                    .phase_inflight
                    .checked_sub(1)
                    .expect("phase fs in-flight underflow");
            }
            FsClientResponse::DeleteIfExists { .. } => {
                self.cleanup_inflight = self
                    .cleanup_inflight
                    .checked_sub(1)
                    .expect("cleanup fs in-flight underflow");
            }
        }

        Some(response)
    }

    pub fn scan(&mut self, scope: ScanScope) -> eyre::Result<()> {
        let (reply, rx) = oneshot::channel::<eyre::Result<ScanResult>>();
        self.tx.send(FsRequest::Scan { scope, reply })?;
        self.pending.push(Box::pin(async move {
            let result: eyre::Result<ScanResult> = match rx.await {
                Ok(result) => result,
                Err(err) => Err(err.into()),
            };
            FsClientResponse::Scan { result }
        }));
        self.phase_inflight += 1;
        Ok(())
    }

    pub fn apply_import_action(&mut self, action: ImportAction) -> eyre::Result<()> {
        let ImportAction { id, kind } = action;

        match kind {
            ImportActionKind::Read {
                path,
                expected_fingerprint,
            } => {
                let (reply, rx) = oneshot::channel::<eyre::Result<GuardedReadResult>>();
                self.tx.send(FsRequest::GuardedRead {
                    path,
                    expected: expected_fingerprint,
                    reply,
                })?;
                self.pending.push(Box::pin(async move {
                    let outcome = match rx.await {
                        Ok(Ok(result)) => ImportReadOutcome::Completed(result),
                        Ok(Err(err)) => ImportReadOutcome::Failed(err),
                        Err(err) => ImportReadOutcome::Failed(err.into()),
                    };
                    FsClientResponse::ApplyImportAction {
                        result: ImportActionResult::Read {
                            action_id: id,
                            outcome,
                        },
                    }
                }));
                self.phase_inflight += 1;
            }
            ImportActionKind::Stat { path } => {
                let (reply, rx) = oneshot::channel();
                self.tx.send(FsRequest::Stat { path, reply })?;
                self.pending.push(Box::pin(async move {
                    let outcome = match rx.await {
                        Ok(Ok(result)) => ImportStatOutcome::Completed(result),
                        Ok(Err(err)) => ImportStatOutcome::Failed(err),
                        Err(err) => ImportStatOutcome::Failed(err.into()),
                    };
                    FsClientResponse::ApplyImportAction {
                        result: ImportActionResult::Stat {
                            action_id: id,
                            outcome,
                        },
                    }
                }));
                self.phase_inflight += 1;
            }
        }

        Ok(())
    }

    pub fn apply_projection_action(&mut self, action: ProjectionAction) -> eyre::Result<()> {
        let ProjectionAction {
            id,
            kind,
            commit_context: _,
        } = action;

        match kind {
            ProjectionActionKind::WriteFile {
                path,
                bytes,
                expected_before,
            } => {
                let (reply, rx) = oneshot::channel::<eyre::Result<GuardedWriteResult>>();
                self.tx.send(FsRequest::GuardedWrite {
                    path,
                    bytes,
                    expected_before,
                    reply,
                })?;
                self.pending.push(Box::pin(async move {
                    let result = match rx.await {
                        Ok(Ok(result)) => Ok(ProjectionActionResult::WriteFile {
                            action_id: id,
                            result,
                        }),
                        Ok(Err(error)) => Ok(ProjectionActionResult::Failed {
                            action_id: id,
                            error,
                        }),
                        Err(error) => Ok(ProjectionActionResult::Failed {
                            action_id: id,
                            error: error.into(),
                        }),
                    };
                    FsClientResponse::ApplyProjectionAction { result }
                }));
                self.phase_inflight += 1;
            }
            ProjectionActionKind::DeleteFile {
                path,
                expected_before,
            } => {
                let (reply, rx) = oneshot::channel::<eyre::Result<GuardedDeleteResult>>();
                self.tx.send(FsRequest::GuardedDelete {
                    path,
                    expected_before,
                    reply,
                })?;
                self.pending.push(Box::pin(async move {
                    let result = match rx.await {
                        Ok(Ok(result)) => Ok(ProjectionActionResult::DeleteFile {
                            action_id: id,
                            result,
                        }),
                        Ok(Err(error)) => Ok(ProjectionActionResult::Failed {
                            action_id: id,
                            error,
                        }),
                        Err(error) => Ok(ProjectionActionResult::Failed {
                            action_id: id,
                            error: error.into(),
                        }),
                    };
                    FsClientResponse::ApplyProjectionAction { result }
                }));
                self.phase_inflight += 1;
            }
        }

        Ok(())
    }

    pub fn delete_if_exists(&mut self, path: RelativePath) -> eyre::Result<()> {
        assert!(
            self.can_accept_cleanup(),
            "cleanup delete requested while cleanup pipeline is full"
        );

        let response_path = path.clone();
        let (reply, rx) = oneshot::channel::<eyre::Result<DeleteIfExistsResult>>();
        self.tx.send(FsRequest::DeleteIfExists { path, reply })?;
        self.pending.push(Box::pin(async move {
            let result = match rx.await {
                Ok(result) => result,
                Err(error) => Err(error.into()),
            };
            FsClientResponse::DeleteIfExists {
                path: response_path,
                result,
            }
        }));
        self.cleanup_inflight += 1;
        Ok(())
    }
}
