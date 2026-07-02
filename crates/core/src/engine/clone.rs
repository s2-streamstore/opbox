use super::shared_message_buffer::SharedMessageBuffer;
use crate::fs::client::{FsClient, FsClientResponse};
use crate::fs::types::{ExpectedBefore, ScanScope};
use crate::log::types::{LogReadStop, LogReaderEvent, LogReaderRequest, SequenceNumber};
use crate::semantic::client::{SemanticClient, SemanticClientResponse};
use crate::semantic::types::{
    NextWork, ProjectionAction, ProjectionActionKind, ProjectionActionResult,
    ProjectionEpochEndReason,
};
use crate::types::SharedMessageBatch;
use eyre::eyre;
use std::ops::RangeTo;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, trace};

const MAX_SHARED_MESSAGE_BUFFER_MS: Duration = Duration::from_millis(500);

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
    pub log_read_stop: Option<LogReadStop>,
    /// Clone into a possibly-populated sync root: skip the emptiness check and
    /// project remote state over whatever is on disk. Files whose content
    /// already matches are left untouched; divergent files are overwritten.
    /// Local-only files are never deleted (a clone plan contains no deletes;
    /// they are imported and published on the next sync start).
    pub clobber: bool,
}

#[derive(Debug)]
pub struct CloneResult {
    pub applied_log_cursor: RangeTo<SequenceNumber>,
    pub projected_actions: usize,
}

pub async fn run(config: CloneConfig) -> eyre::Result<CloneResult> {
    let CloneConfig {
        clients,
        events,
        log_read_stop,
        clobber,
    } = config;
    let CloneClients {
        mut fs,
        mut semantic,
        log_reader,
    } = clients;
    let CloneEvents {
        log_reader: mut log_reader_rx,
    } = events;

    if !clobber {
        fs.scan(ScanScope::Full)?;
        let scan = await_scan(&mut fs).await?;
        if !scan.tree.entries().is_empty() {
            eyre::bail!(
                "clone sync root is not empty; observed {} entries",
                scan.tree.entries().len()
            );
        }
    }

    let applied_log_cursor = if log_read_stop.is_some() {
        pull_shared_log_to_reader_end(&mut semantic, &mut log_reader_rx).await?
    } else {
        pull_shared_log_to_start_tail(&mut semantic, &log_reader, &mut log_reader_rx).await?
    };

    semantic.get_next_work(0)?;
    let next_work = await_get_next_work(&mut semantic).await?;
    let projected_actions = match next_work {
        NextWork::None => 0,
        NextWork::Import(_) => {
            eyre::bail!("clone returned import work after pulling shared log");
        }
        NextWork::Project(started) => {
            let mut projected_actions = 0;

            for action in started.plan.actions {
                let action = if clobber {
                    unguard_write_action(action)?
                } else {
                    action
                };
                fs.apply_projection_action(action)?;
                let result = await_projection_action(&mut fs).await?;
                fail_if_projection_action_invalidated(&result)?;
                semantic.commit_projection_action_checked(result)?;
                await_commit_projection_action(&mut semantic).await?;
                projected_actions += 1;
            }

            semantic
                .commit_projection_epoch(started.epoch, ProjectionEpochEndReason::PlanExhausted)?;
            await_commit_projection_epoch(&mut semantic).await?;

            projected_actions
        }
    };

    Ok(CloneResult {
        applied_log_cursor,
        projected_actions,
    })
}

/// A clobbering clone projects into a sync root that may hold arbitrary local
/// content, while the plan's guards were computed against the empty prior
/// tree. Drop the write precondition so remote state replaces whatever is on
/// disk. Deletes are left guarded: a clone plan cannot contain them (nothing
/// is tracked yet), so one showing up means the plan is not a clone plan.
fn unguard_write_action(action: ProjectionAction) -> eyre::Result<ProjectionAction> {
    let ProjectionAction {
        id,
        kind,
        commit_context,
    } = action;
    match kind {
        ProjectionActionKind::WriteFile { path, bytes, .. } => Ok(ProjectionAction {
            id,
            kind: ProjectionActionKind::WriteFile {
                path,
                bytes,
                expected_before: ExpectedBefore::Anything,
            },
            commit_context,
        }),
        ProjectionActionKind::DeleteFile { path, .. } => {
            eyre::bail!("clone projection plan unexpectedly contains a delete for {path}");
        }
    }
}

async fn pull_shared_log_to_reader_end(
    semantic: &mut SemanticClient,
    log_reader_rx: &mut mpsc::Receiver<LogReaderEvent>,
) -> eyre::Result<RangeTo<SequenceNumber>> {
    let mut applied_end: SequenceNumber = 0;
    let mut buffer = SharedMessageBuffer::new(MAX_SHARED_MESSAGE_BUFFER_MS);

    loop {
        if !buffer.has_capacity() {
            applied_end = flush_shared_message_buffer(semantic, &mut buffer).await?;
            continue;
        }

        let event = if let Some(deadline) = buffer.next_fire_at() {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    applied_end = flush_shared_message_buffer(semantic, &mut buffer).await?;
                    continue;
                }
                event = log_reader_rx.recv() => event,
            }
        } else {
            log_reader_rx.recv().await
        };

        match event.ok_or_else(|| eyre!("log reader stopped while clone waited for read bound"))? {
            LogReaderEvent::Connected => {}
            LogReaderEvent::Disconnected { reason } => {
                eyre::bail!("log reader disconnected during clone: {reason}");
            }
            LogReaderEvent::Status { tail } => {
                eyre::bail!(
                    "clone received unexpected log reader status for bounded read; tail={tail:?}"
                );
            }
            LogReaderEvent::Read(envelope) => {
                trace!(
                    sequence_number = envelope.sequence_number,
                    "clone buffered log message"
                );
                buffer.insert(envelope);
            }
            LogReaderEvent::Ended { cursor } => {
                if !buffer.is_empty() {
                    applied_end = flush_shared_message_buffer(semantic, &mut buffer).await?;
                }
                if applied_end != cursor.end {
                    eyre::bail!(
                        "clone bounded read ended at cursor {cursor:?}, but applied cursor is ..{applied_end}"
                    );
                }
                debug!(?cursor, "clone reached bounded read end");
                return Ok(cursor);
            }
        }
    }
}

async fn pull_shared_log_to_start_tail(
    semantic: &mut SemanticClient,
    log_reader: &mpsc::UnboundedSender<LogReaderRequest>,
    log_reader_rx: &mut mpsc::Receiver<LogReaderEvent>,
) -> eyre::Result<RangeTo<SequenceNumber>> {
    log_reader.send(LogReaderRequest::Status)?;

    let mut target_tail: Option<RangeTo<SequenceNumber>> = None;
    let mut applied_end: SequenceNumber = 0;
    let mut buffer = SharedMessageBuffer::new(MAX_SHARED_MESSAGE_BUFFER_MS);

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
        LogReaderEvent::Connected => {}
        LogReaderEvent::Disconnected { reason } => {
            eyre::bail!("log reader disconnected during clone: {reason}");
        }
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
        LogReaderEvent::Ended { cursor } => {
            eyre::bail!("clone received unexpected bounded read end: {cursor:?}");
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

    let next_end = buffer
        .last_sequence_end()
        .expect("non-empty buffer has last sequence");
    let envelopes = buffer.drain();
    let message_count = envelopes.len();
    let batch = SharedMessageBatch::try_from(envelopes)?;
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
        SemanticClientResponse::ApplySharedMessageBatch { result, .. } => result,
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
        SemanticClientResponse::GetNextWork { result, .. } => result,
        _ => Err(eyre!(
            "unexpected semantic response while clone waited for next work"
        )),
    }
}

async fn await_commit_projection_action(semantic: &mut SemanticClient) -> eyre::Result<()> {
    match semantic.next().await.ok_or_else(|| {
        eyre!("semantic actor stopped while clone waited for commit_projection_action")
    })? {
        SemanticClientResponse::CommitProjectionAction(result) => result,
        _ => Err(eyre!(
            "unexpected semantic response while clone waited for commit_projection_action"
        )),
    }
}

async fn await_commit_projection_epoch(semantic: &mut SemanticClient) -> eyre::Result<()> {
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
        let result_kind: &'static str = result.into();
        eyre::bail!(
            "clone projection action invalidated projection; result={}",
            result_kind
        );
    }

    Ok(())
}
