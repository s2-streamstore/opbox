use crate::fs::client::{FsClient, FsClientResponse};
use crate::fs::types::ScanScope;
use crate::log::types::{LogReaderEvent, LogReaderRequest, SequenceNumber, SharedMessageEnvelope};
use crate::semantic::client::{SemanticClient, SemanticClientResponse};
use crate::semantic::types::{NextWork, ProjectionActionResult, ProjectionEpochEndReason};
use crate::types::SharedMessageBatch;
use eyre::eyre;
use std::ops::RangeTo;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{debug, trace};

const MAX_SHARED_MESSAGE_BUFFER_MS: Duration = Duration::from_millis(500);

// Can overshoot.
const MAX_SHARED_MESSAGE_BUFFER_SIZE_BYTES: usize = 1024 * 1024 * 10;

#[derive(Default)]
struct SharedMessageBuffer {
    size_bytes: usize,
    earliest_timestamp: Option<Instant>,
    envelopes: Vec<SharedMessageEnvelope>,
}

impl SharedMessageBuffer {
    fn insert(&mut self, envelope: SharedMessageEnvelope) {
        if self.earliest_timestamp.is_none() {
            self.earliest_timestamp = Some(Instant::now());
        }
        self.size_bytes += envelope.shared_message.approximate_size_bytes();
        self.envelopes.push(envelope);
    }

    fn has_capacity(&self) -> bool {
        self.size_bytes < MAX_SHARED_MESSAGE_BUFFER_SIZE_BYTES
    }

    fn is_empty(&self) -> bool {
        self.envelopes.is_empty()
    }

    fn last_sequence_end(&self) -> Option<SequenceNumber> {
        self.envelopes
            .last()
            .map(|envelope| envelope.sequence_number + 1)
    }

    fn next_fire_at(&self) -> Option<Instant> {
        if self.has_capacity() {
            self.earliest_timestamp
                .map(|earliest| earliest + MAX_SHARED_MESSAGE_BUFFER_MS)
        } else {
            Some(Instant::now())
        }
    }
}

pub struct CloneClients {
    pub fs: FsClient,
    pub semantic: SemanticClient,
    pub log_reader: mpsc::UnboundedSender<LogReaderRequest>,
}

pub struct CloneEvents {
    pub log_reader: mpsc::Receiver<LogReaderEvent>,
}

pub struct CloneConfig {
    pub clients: CloneClients,
    pub events: CloneEvents,
}

#[derive(Debug)]
pub struct CloneResult {
    pub applied_log_cursor: RangeTo<SequenceNumber>,
    pub projected_actions: usize,
}

pub async fn run(config: CloneConfig) -> eyre::Result<CloneResult> {
    let CloneConfig { clients, events } = config;
    let CloneClients {
        mut fs,
        mut semantic,
        log_reader,
    } = clients;
    let CloneEvents {
        log_reader: mut log_reader_rx,
    } = events;

    fs.scan(ScanScope::Full)?;
    let scan = await_scan(&mut fs).await?;
    if !scan.tree.entries().is_empty() {
        eyre::bail!(
            "clone sync root is not empty; observed {} entries",
            scan.tree.entries().len()
        );
    }

    let applied_log_cursor =
        pull_shared_log_to_start_tail(&mut semantic, &log_reader, &mut log_reader_rx).await?;

    semantic.get_next_work()?;
    let next_work = await_get_next_work(&mut semantic).await?;
    let projected_actions = match next_work {
        NextWork::None => 0,
        NextWork::Import(_) => {
            eyre::bail!("clone returned import work after pulling shared log");
        }
        NextWork::Project(started) => {
            let mut projected_actions = 0;

            for action in started.plan.actions {
                fs.apply_projection_action(action)?;
                let result = await_projection_action(&mut fs).await?;
                fail_if_projection_action_invalidated(&result)?;
                semantic.commit_projection_action(result)?;
                projected_actions += 1;
            }

            semantic
                .commit_projection_epoch(started.epoch, ProjectionEpochEndReason::PlanExhausted)?;
            match await_commit_projection_epoch(&mut semantic).await? {
                NextWork::None => {}
                NextWork::Import(_) => {
                    eyre::bail!("clone projection epoch returned import work");
                }
                NextWork::Project(_) => {
                    eyre::bail!("clone projection epoch returned a second projection epoch");
                }
            }

            projected_actions
        }
    };

    Ok(CloneResult {
        applied_log_cursor,
        projected_actions,
    })
}

async fn pull_shared_log_to_start_tail(
    semantic: &mut SemanticClient,
    log_reader: &mpsc::UnboundedSender<LogReaderRequest>,
    log_reader_rx: &mut mpsc::Receiver<LogReaderEvent>,
) -> eyre::Result<RangeTo<SequenceNumber>> {
    log_reader.send(LogReaderRequest::Status)?;

    let mut target_tail: Option<RangeTo<SequenceNumber>> = None;
    let mut applied_end: SequenceNumber = 0;
    let mut buffer = SharedMessageBuffer::default();

    loop {
        if let Some(tail) = &target_tail
            && applied_end >= tail.end
        {
            return Ok(..applied_end);
        }

        if let Some(tail) = &target_tail
            && buffer
                .last_sequence_end()
                .is_some_and(|buffered_end| buffered_end >= tail.end)
        {
            applied_end = flush_shared_message_buffer(semantic, &mut buffer).await?;
            continue;
        }

        if !buffer.has_capacity() {
            applied_end = flush_shared_message_buffer(semantic, &mut buffer).await?;
            continue;
        }

        if let Some(deadline) = buffer.next_fire_at() {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    applied_end = flush_shared_message_buffer(semantic, &mut buffer).await?;
                }
                event = log_reader_rx.recv() => {
                    handle_log_reader_event(event, &mut target_tail, &mut buffer)?;
                }
            }
        } else {
            handle_log_reader_event(log_reader_rx.recv().await, &mut target_tail, &mut buffer)?;
        }
    }
}

fn handle_log_reader_event(
    event: Option<LogReaderEvent>,
    target_tail: &mut Option<RangeTo<SequenceNumber>>,
    buffer: &mut SharedMessageBuffer,
) -> eyre::Result<()> {
    match event.ok_or_else(|| eyre!("log reader stopped while clone waited for shared log"))? {
        LogReaderEvent::Status { tail } => {
            debug!(?tail, "clone captured start tail");
            *target_tail = Some(tail);
        }
        LogReaderEvent::Read(envelope) => {
            trace!(
                sequence_number = envelope.sequence_number,
                "clone buffered log message"
            );
            buffer.insert(envelope);
        }
    }

    Ok(())
}

async fn flush_shared_message_buffer(
    semantic: &mut SemanticClient,
    buffer: &mut SharedMessageBuffer,
) -> eyre::Result<SequenceNumber> {
    assert!(
        !buffer.is_empty(),
        "clone shared-message buffer flush requested while empty"
    );

    let buffer = std::mem::take(buffer);
    let next_end = buffer
        .last_sequence_end()
        .expect("non-empty buffer has last sequence");
    let message_count = buffer.envelopes.len();
    let batch = SharedMessageBatch::try_from(buffer.envelopes)?;
    semantic.apply_shared_message_batch(batch)?;
    await_apply_shared_message_batch(semantic).await?;

    debug!(
        message_count,
        applied_end = next_end,
        "clone applied log batch"
    );
    Ok(next_end)
}

async fn await_scan(fs: &mut FsClient) -> eyre::Result<crate::fs::types::ScanResult> {
    match fs
        .next()
        .await
        .ok_or_else(|| eyre!("fs actor stopped while clone waited for scan"))?
    {
        FsClientResponse::Scan { result } => result,
        _ => Err(eyre!("unexpected fs response while clone waited for scan")),
    }
}

async fn await_projection_action(fs: &mut FsClient) -> eyre::Result<ProjectionActionResult> {
    match fs
        .next()
        .await
        .ok_or_else(|| eyre!("fs actor stopped while clone waited for projection action"))?
    {
        FsClientResponse::ApplyProjectionAction { result } => result,
        _ => Err(eyre!(
            "unexpected fs response while clone waited for projection action"
        )),
    }
}

async fn await_apply_shared_message_batch(semantic: &mut SemanticClient) -> eyre::Result<()> {
    match semantic
        .next()
        .await
        .ok_or_else(|| eyre!("semantic actor stopped while clone waited for shared log apply"))?
    {
        SemanticClientResponse::ApplySharedMessageBatch(result) => result,
        _ => Err(eyre!(
            "unexpected semantic response while clone waited for shared log apply"
        )),
    }
}

async fn await_get_next_work(semantic: &mut SemanticClient) -> eyre::Result<NextWork> {
    match semantic
        .next()
        .await
        .ok_or_else(|| eyre!("semantic actor stopped while clone waited for next work"))?
    {
        SemanticClientResponse::GetNextWork(result) => result,
        _ => Err(eyre!(
            "unexpected semantic response while clone waited for next work"
        )),
    }
}

async fn await_commit_projection_epoch(semantic: &mut SemanticClient) -> eyre::Result<NextWork> {
    match semantic
        .next()
        .await
        .ok_or_else(|| eyre!("semantic actor stopped while clone waited for projection commit"))?
    {
        SemanticClientResponse::CommitProjectionEpoch(result) => result,
        _ => Err(eyre!(
            "unexpected semantic response while clone waited for projection commit"
        )),
    }
}

fn fail_if_projection_action_invalidated(result: &ProjectionActionResult) -> eyre::Result<()> {
    if result.invalidates_projection() {
        eyre::bail!(
            "clone projection action invalidated projection; result={}",
            projection_action_result_kind(result)
        );
    }

    Ok(())
}

fn projection_action_result_kind(result: &ProjectionActionResult) -> &'static str {
    match result {
        ProjectionActionResult::WriteFile { .. } => "write_file",
        ProjectionActionResult::DeleteFile { .. } => "delete_file",
        ProjectionActionResult::Failed { .. } => "failed",
    }
}
