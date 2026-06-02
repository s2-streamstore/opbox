use crate::crdt::types::SharedMessage;
use crate::fs::types::ScanResult;
use crate::semantic::types::{
    ImportActionResult, ImportEpoch, NextWork, ProjectionActionResult, ProjectionEpoch,
    ProjectionEpochEndReason, SemanticRequest,
};
use crate::types::{OutboxId, SharedMessageBatch};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use std::ops::RangeToInclusive;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tracing::instrument;

pub enum SemanticClientResponse {
    ApplyInitScan(eyre::Result<NextWork>),
    ApplyScan(eyre::Result<NextWork>),
    CommitImportEpoch(eyre::Result<NextWork>),
    CommitProjectionEpoch(eyre::Result<NextWork>),
    GetNextWork(eyre::Result<NextWork>),
    ApplySharedMessageBatch(eyre::Result<()>),
    ReadOutbox(eyre::Result<Vec<(OutboxId, SharedMessage)>>),
}

pub struct SemanticClient {
    tx: UnboundedSender<SemanticRequest>,
    pending: FuturesUnordered<BoxFuture<'static, SemanticClientResponse>>,
    in_flight_apply_shared_message_batch: usize,
}

impl SemanticClient {
    pub fn new(tx: UnboundedSender<SemanticRequest>) -> Self {
        Self {
            tx,
            pending: FuturesUnordered::new(),
            in_flight_apply_shared_message_batch: 0,
        }
    }

    fn enqueue<T>(
        &mut self,
        build: impl FnOnce(oneshot::Sender<eyre::Result<T>>) -> SemanticRequest,
        map: impl FnOnce(eyre::Result<T>) -> SemanticClientResponse + Send + 'static,
    ) -> eyre::Result<()>
    where
        T: Send + 'static,
    {
        let (reply, rx) = oneshot::channel();
        self.tx.send(build(reply))?;

        self.pending.push(Box::pin(async move {
            let result = match rx.await {
                Ok(result) => result,
                Err(err) => Err(err.into()),
            };
            map(result)
        }));

        Ok(())
    }

    pub async fn next(&mut self) -> Option<SemanticClientResponse> {
        match self.pending.next().await {
            Some(resp @ SemanticClientResponse::ApplySharedMessageBatch(_)) => {
                self.in_flight_apply_shared_message_batch = self
                    .in_flight_apply_shared_message_batch
                    .checked_sub(1)
                    .expect("no overflow");
                Some(resp)
            }
            Some(resp) => Some(resp),
            None => None,
        }
    }

    #[instrument(skip(self))]
    pub fn apply_init_scan(&mut self, scan: ScanResult) -> eyre::Result<()> {
        self.enqueue(
            |reply| SemanticRequest::ApplyInitScan { scan, reply },
            SemanticClientResponse::ApplyInitScan,
        )
    }

    #[instrument(skip(self))]
    pub fn apply_scan(&mut self, scan: ScanResult) -> eyre::Result<()> {
        self.enqueue(
            |reply| SemanticRequest::ApplyScan { scan, reply },
            SemanticClientResponse::ApplyScan,
        )
    }

    pub fn commit_import_action(&mut self, result: ImportActionResult) -> eyre::Result<()> {
        self.tx
            .send(SemanticRequest::CommitImportAction { result })?;
        Ok(())
    }

    pub fn commit_import_epoch(&mut self, epoch: ImportEpoch) -> eyre::Result<()> {
        self.enqueue(
            |reply| SemanticRequest::CommitImportEpoch { epoch, reply },
            SemanticClientResponse::CommitImportEpoch,
        )
    }

    pub fn commit_projection_action(&mut self, result: ProjectionActionResult) -> eyre::Result<()> {
        self.tx
            .send(SemanticRequest::CommitProjectionAction { result })?;
        Ok(())
    }

    pub fn commit_projection_epoch(
        &mut self,
        epoch: ProjectionEpoch,
        reason: ProjectionEpochEndReason,
    ) -> eyre::Result<()> {
        self.enqueue(
            |reply| SemanticRequest::CommitProjectionEpoch {
                epoch,
                reason,
                reply,
            },
            SemanticClientResponse::CommitProjectionEpoch,
        )
    }

    pub fn get_next_work(&mut self) -> eyre::Result<()> {
        self.enqueue(
            |reply| SemanticRequest::GetNextWork { reply },
            SemanticClientResponse::GetNextWork,
        )
    }

    pub fn trim_outbox(&mut self, through: RangeToInclusive<OutboxId>) -> eyre::Result<()> {
        self.tx.send(SemanticRequest::TrimOutbox { through })?;
        Ok(())
    }

    pub fn read_outbox(&mut self, num_messages: u64) -> eyre::Result<()> {
        self.enqueue(
            |reply| SemanticRequest::ReadOutbox {
                num_messages,
                reply,
            },
            SemanticClientResponse::ReadOutbox,
        )
    }

    pub fn can_accept_apply_shared_message_batch(&self) -> bool {
        self.in_flight_apply_shared_message_batch == 0
    }

    pub fn apply_shared_message_batch(&mut self, batch: SharedMessageBatch) -> eyre::Result<()> {
        assert!(self.can_accept_apply_shared_message_batch());
        self.enqueue(
            |reply| SemanticRequest::ApplySharedMessageBatch { batch, reply },
            SemanticClientResponse::ApplySharedMessageBatch,
        )?;

        self.in_flight_apply_shared_message_batch += 1;
        Ok(())
    }

    // TODO(deferred) add scheduling/backpressure helpers for outbox reads.
}
