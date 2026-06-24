use crate::crdt::types::SharedMessage;
use crate::fs::types::ScanResult;
use crate::log::types::SequenceNumber;
use crate::semantic::types::{
    ImportActionResult, ImportEpoch, NextWork, ProjectionActionResult, ProjectionEpoch,
    ProjectionEpochEndReason, SemanticRequest,
};
use crate::types::{OutboxId, SharedMessageBatch};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use std::ops::{RangeInclusive, RangeTo, RangeToInclusive};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tracing::instrument;

pub enum SemanticClientResponse {
    ApplyInitScan(eyre::Result<NextWork>),
    ApplyScan(eyre::Result<NextWork>),
    CommitImportEpoch(eyre::Result<()>),
    CommitProjectionEpoch(eyre::Result<()>),
    GetNextWork {
        /// Echo of the caller-supplied boundary sequence, so the engine can
        /// bind the response to the boundary that requested it.
        boundary: u64,
        result: eyre::Result<NextWork>,
    },
    ApplySharedMessageBatch {
        sequence_range: RangeInclusive<SequenceNumber>,
        result: eyre::Result<()>,
    },
    ReadOutbox(eyre::Result<Vec<(OutboxId, SharedMessage)>>),
    ReleaseOutbox(eyre::Result<u64>),
    ReadStableCursor(eyre::Result<RangeTo<SequenceNumber>>),
}

pub struct SemanticClient {
    tx: UnboundedSender<SemanticRequest>,
    pending: FuturesUnordered<BoxFuture<'static, SemanticClientResponse>>,
    in_flight_apply_shared_message_batch: usize,
    in_flight_read_outbox: bool,
}

impl SemanticClient {
    pub fn new(tx: UnboundedSender<SemanticRequest>) -> Self {
        Self {
            tx,
            pending: FuturesUnordered::new(),
            in_flight_apply_shared_message_batch: 0,
            in_flight_read_outbox: false,
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
            Some(resp @ SemanticClientResponse::ApplySharedMessageBatch { .. }) => {
                self.in_flight_apply_shared_message_batch = self
                    .in_flight_apply_shared_message_batch
                    .checked_sub(1)
                    .expect("no overflow");
                Some(resp)
            }
            Some(resp @ SemanticClientResponse::ReadOutbox(_)) => {
                assert!(
                    self.in_flight_read_outbox,
                    "read-outbox response without an in-flight request"
                );
                self.in_flight_read_outbox = false;
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

    pub fn get_next_work(&mut self, boundary: u64) -> eyre::Result<()> {
        self.enqueue(
            |reply| SemanticRequest::GetNextWork { reply },
            move |result| SemanticClientResponse::GetNextWork { boundary, result },
        )
    }

    pub fn trim_outbox(&mut self, through: RangeToInclusive<OutboxId>) -> eyre::Result<()> {
        self.tx.send(SemanticRequest::TrimOutbox { through })?;
        Ok(())
    }

    pub fn read_outbox(&mut self, num_messages: u64) -> eyre::Result<()> {
        assert!(
            !self.in_flight_read_outbox,
            "cannot start read_outbox while another read_outbox is in flight"
        );
        self.enqueue(
            |reply| SemanticRequest::ReadOutbox {
                num_messages,
                reply,
            },
            SemanticClientResponse::ReadOutbox,
        )?;
        self.in_flight_read_outbox = true;
        Ok(())
    }

    pub fn release_outbox(&mut self) -> eyre::Result<()> {
        self.enqueue(
            |reply| SemanticRequest::ReleaseOutbox { reply },
            SemanticClientResponse::ReleaseOutbox,
        )
    }

    pub fn read_stable_cursor(&mut self) -> eyre::Result<()> {
        self.enqueue(
            |reply| SemanticRequest::ReadStableCursor { reply },
            SemanticClientResponse::ReadStableCursor,
        )
    }

    pub fn can_accept_apply_shared_message_batch(&self) -> bool {
        self.in_flight_apply_shared_message_batch == 0
    }

    pub fn apply_shared_message_batch(&mut self, batch: SharedMessageBatch) -> eyre::Result<()> {
        assert!(self.can_accept_apply_shared_message_batch());
        let sequence_range = batch.sequence_range.clone();
        self.enqueue(
            |reply| SemanticRequest::ApplySharedMessageBatch { batch, reply },
            move |result| SemanticClientResponse::ApplySharedMessageBatch {
                sequence_range,
                result,
            },
        )?;

        self.in_flight_apply_shared_message_batch += 1;
        Ok(())
    }

    // TODO(deferred) add scheduling/backpressure helpers for outbox reads.
}
