use crate::fs::fio::FileIO;
use crate::fs::types::{
    DeleteIfExistsResult, FsRequest, GuardedDeleteResult, GuardedReadResult, GuardedWriteResult,
    ScanResult, TreeEntry,
};
use eyre::eyre;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::BTreeMap;
use std::time::Instant;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::debug;

pub struct FsActor<IO: FileIO> {
    fio: IO,
    request_rx: UnboundedReceiver<FsRequest>,
    next_op_id: FsOpId,
    pending_ops: BTreeMap<FsOpId, PendingFsOp>,
    running_ops: FuturesUnordered<BoxFuture<'static, FsTaskResult>>,
}

impl<IO: FileIO + Clone + Send + 'static> FsActor<IO> {
    pub fn new(fio: IO, request_rx: UnboundedReceiver<FsRequest>) -> FsActor<IO> {
        Self {
            fio,
            request_rx,
            next_op_id: FsOpId(0),
            pending_ops: BTreeMap::new(),
            running_ops: FuturesUnordered::new(),
        }
    }

    fn next_op_id(&mut self) -> FsOpId {
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

                Some(result) = self.running_ops.next(), if !self.running_ops.is_empty() => {
                    self.finish_task_result(result)?;
                }

                Some(request) = self.request_rx.recv() => {
                    self.start_request(request)?;
                }

                else => {

                    return Err(eyre!("unrecoverable error"));
                }
            }
        }
    }

    fn start_request(&mut self, request: FsRequest) -> eyre::Result<()> {
        let op_id = self.next_op_id();

        match request {
            FsRequest::Scan { scope, reply } => {
                self.pending_ops
                    .insert(op_id, PendingFsOp::new(PendingFsOpKind::Scan { reply }));
                let fio = self.fio.clone();
                self.running_ops.push(Box::pin(async move {
                    FsTaskResult::Scan {
                        op_id,
                        result: fio.scan(scope).await,
                    }
                }));
            }
            FsRequest::Stat { path, reply } => {
                self.pending_ops
                    .insert(op_id, PendingFsOp::new(PendingFsOpKind::Stat { reply }));
                let fio = self.fio.clone();
                self.running_ops.push(Box::pin(async move {
                    FsTaskResult::Stat {
                        op_id,
                        result: fio.stat(path).await,
                    }
                }));
            }
            FsRequest::GuardedRead {
                path,
                expected,
                reply,
            } => {
                self.pending_ops.insert(
                    op_id,
                    PendingFsOp::new(PendingFsOpKind::GuardedRead { reply }),
                );
                let fio = self.fio.clone();
                self.running_ops.push(Box::pin(async move {
                    FsTaskResult::GuardedRead {
                        op_id,
                        result: fio.guarded_read(path, expected).await,
                    }
                }));
            }
            FsRequest::GuardedWrite {
                path,
                bytes,
                expected_before,
                reply,
            } => {
                self.pending_ops.insert(
                    op_id,
                    PendingFsOp::new(PendingFsOpKind::GuardedWrite { reply }),
                );
                let fio = self.fio.clone();
                self.running_ops.push(Box::pin(async move {
                    FsTaskResult::GuardedWrite {
                        op_id,
                        result: fio.guarded_write(path, bytes, expected_before).await,
                    }
                }));
            }
            FsRequest::GuardedDelete {
                path,
                expected_before,
                reply,
            } => {
                self.pending_ops.insert(
                    op_id,
                    PendingFsOp::new(PendingFsOpKind::GuardedDelete { reply }),
                );
                let fio = self.fio.clone();
                self.running_ops.push(Box::pin(async move {
                    FsTaskResult::GuardedDelete {
                        op_id,
                        result: fio.guarded_delete(path, expected_before).await,
                    }
                }));
            }
            FsRequest::DeleteIfExists { path, reply } => {
                self.pending_ops.insert(
                    op_id,
                    PendingFsOp::new(PendingFsOpKind::DeleteIfExists { reply }),
                );
                let fio = self.fio.clone();
                self.running_ops.push(Box::pin(async move {
                    FsTaskResult::DeleteIfExists {
                        op_id,
                        result: fio.delete_if_exists(path).await,
                    }
                }));
            }
        }

        Ok(())
    }

    fn finish_task_result(&mut self, result: FsTaskResult) -> eyre::Result<()> {
        let op_id = result.op_id();
        let pending = self.take_pending_op(op_id)?;

        match (result, pending.kind) {
            (FsTaskResult::Scan { result, .. }, PendingFsOpKind::Scan { reply }) => {
                let _ = reply.send(result);
            }
            (FsTaskResult::Stat { result, .. }, PendingFsOpKind::Stat { reply }) => {
                let _ = reply.send(result);
            }
            (FsTaskResult::GuardedRead { result, .. }, PendingFsOpKind::GuardedRead { reply }) => {
                let _ = reply.send(result);
            }
            (
                FsTaskResult::GuardedWrite { result, .. },
                PendingFsOpKind::GuardedWrite { reply },
            ) => {
                let _ = reply.send(result);
            }
            (
                FsTaskResult::GuardedDelete { result, .. },
                PendingFsOpKind::GuardedDelete { reply },
            ) => {
                let _ = reply.send(result);
            }
            (
                FsTaskResult::DeleteIfExists { result, .. },
                PendingFsOpKind::DeleteIfExists { reply },
            ) => {
                let _ = reply.send(result);
            }
            (result, pending) => {
                return Err(eyre!(
                    "fs op {op_id:?} completed with mismatched pending kind: result={}, pending={}",
                    result.kind(),
                    pending.kind(),
                ));
            }
        }

        Ok(())
    }

    fn take_pending_op(&mut self, op_id: FsOpId) -> eyre::Result<PendingFsOp> {
        self.pending_ops
            .remove(&op_id)
            .ok_or_else(|| eyre!("fs op {op_id:?} completed but was not pending"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FsOpId(u64);

struct PendingFsOp {
    kind: PendingFsOpKind,
    #[allow(dead_code)] // reserved for stall detection
    started_at: Instant,
}

impl PendingFsOp {
    fn new(kind: PendingFsOpKind) -> Self {
        Self {
            kind,
            started_at: Instant::now(),
        }
    }
}

#[derive(strum::IntoStaticStr)]
enum PendingFsOpKind {
    #[strum(serialize = "scan")]
    Scan {
        reply: oneshot::Sender<eyre::Result<ScanResult>>,
    },
    #[strum(serialize = "stat")]
    Stat {
        reply: oneshot::Sender<eyre::Result<Option<TreeEntry>>>,
    },
    #[strum(serialize = "guarded_read")]
    GuardedRead {
        reply: oneshot::Sender<eyre::Result<GuardedReadResult>>,
    },
    #[strum(serialize = "guarded_write")]
    GuardedWrite {
        reply: oneshot::Sender<eyre::Result<GuardedWriteResult>>,
    },
    #[strum(serialize = "guarded_delete")]
    GuardedDelete {
        reply: oneshot::Sender<eyre::Result<GuardedDeleteResult>>,
    },
    #[strum(serialize = "delete_if_exists")]
    DeleteIfExists {
        reply: oneshot::Sender<eyre::Result<DeleteIfExistsResult>>,
    },
}

impl PendingFsOpKind {
    fn kind(&self) -> &'static str {
        self.into()
    }
}

#[derive(strum::IntoStaticStr)]
enum FsTaskResult {
    #[strum(serialize = "scan")]
    Scan {
        op_id: FsOpId,
        result: eyre::Result<ScanResult>,
    },
    #[strum(serialize = "stat")]
    Stat {
        op_id: FsOpId,
        result: eyre::Result<Option<TreeEntry>>,
    },
    #[strum(serialize = "guarded_read")]
    GuardedRead {
        op_id: FsOpId,
        result: eyre::Result<GuardedReadResult>,
    },
    #[strum(serialize = "guarded_write")]
    GuardedWrite {
        op_id: FsOpId,
        result: eyre::Result<GuardedWriteResult>,
    },
    #[strum(serialize = "guarded_delete")]
    GuardedDelete {
        op_id: FsOpId,
        result: eyre::Result<GuardedDeleteResult>,
    },
    #[strum(serialize = "delete_if_exists")]
    DeleteIfExists {
        op_id: FsOpId,
        result: eyre::Result<DeleteIfExistsResult>,
    },
}

impl FsTaskResult {
    fn op_id(&self) -> FsOpId {
        match self {
            Self::Scan { op_id, .. }
            | Self::Stat { op_id, .. }
            | Self::GuardedRead { op_id, .. }
            | Self::GuardedWrite { op_id, .. }
            | Self::GuardedDelete { op_id, .. }
            | Self::DeleteIfExists { op_id, .. } => *op_id,
        }
    }

    fn kind(&self) -> &'static str {
        self.into()
    }
}
