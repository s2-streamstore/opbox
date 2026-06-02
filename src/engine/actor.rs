use crate::fs::client::{FsClient, FsClientResponse};
use crate::fs::types::{GuardedWriteResult, RelativePath, ScanScope};
use crate::log::types::{
    LogReaderEvent, LogReaderRequest, LogWriterRequest, LogWriterResponse, SharedMessageEnvelope,
};
use crate::semantic::client::{SemanticClient, SemanticClientResponse};
use crate::semantic::types::{
    ImportAction, ImportEpoch, NextWork, ProjectionAction, ProjectionActionId,
    ProjectionActionResult, ProjectionEpoch, ProjectionEpochEndReason, ProjectionGeneration,
    SemanticEvent,
};
use crate::types::SharedMessageBatch;
use enum_ordinalize::Ordinalize;
use eyre::eyre;
use std::collections::{BTreeSet, VecDeque};
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_muxt::{CoalesceMode, MuxTimer};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

const MAX_SHARED_MESSAGE_BUFFER_MS: Duration = Duration::from_millis(500);
const FULL_SCAN_INTERVAL: Duration = Duration::from_millis(500);

// Can overshoot.
const MAX_SHARED_MESSAGE_BUFFER_SIZE_BYTES: usize = 1024 * 1024 * 10;

#[derive(Ordinalize, Debug, Clone, Copy, PartialEq, Eq)]
enum TimerEvent {
    SharedMessageBufferDrain,
    FullScan,
}

#[derive(Default)]
struct SharedMessageBuffer {
    size_bytes: usize,
    earliest_timestamp: Option<Instant>,
    envelopes: Vec<SharedMessageEnvelope>,
}

impl SharedMessageBuffer {
    fn insert(&mut self, envelope: SharedMessageEnvelope) {
        if self.earliest_timestamp.is_none() {
            self.earliest_timestamp = Some(Instant::now())
        }
        self.size_bytes += envelope.shared_message.approximate_size_bytes();
        self.envelopes.push(envelope);
    }

    fn has_capacity(&self) -> bool {
        self.size_bytes < MAX_SHARED_MESSAGE_BUFFER_SIZE_BYTES
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

enum EnginePhase {
    Idle,
    Scanning,
    Importing {
        epoch: ImportEpoch,
        queue: VecDeque<ImportAction>,
        in_flight: usize,
        committing: bool,
    },
    Projecting {
        epoch: ProjectionEpoch,
        generation: ProjectionGeneration,
        plan: VecDeque<ProjectionAction>,
        in_flight: BTreeSet<ProjectionActionId>,
        end_reason: Option<ProjectionEpochEndReason>,
        committing: bool,
    },
}

impl EnginePhase {
    fn name(&self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Scanning => "scanning",
            Self::Importing { .. } => "importing",
            Self::Projecting { .. } => "projecting",
        }
    }
}

pub struct Engine {
    fs_client: FsClient,
    semantic_client: SemanticClient,

    log_reader_rx: mpsc::Receiver<LogReaderEvent>,
    log_reader_tx: mpsc::UnboundedSender<LogReaderRequest>,
    log_writer_rx: mpsc::UnboundedReceiver<LogWriterResponse>,
    log_writer_tx: mpsc::UnboundedSender<LogWriterRequest>,

    semantic_event_rx: mpsc::UnboundedReceiver<SemanticEvent>,

    phase: EnginePhase,
    /// Daemon-owned temp files left by failed guarded writes. These are GC-only:
    /// Semantic still receives the original projection conflict result.
    cleanup_queue: VecDeque<RelativePath>,

    /// Buffer shared messages received from log reader before sending
    /// for semantic actor to apply.
    shared_message_buffer: SharedMessageBuffer,
}

pub struct EngineClients {
    pub fs: FsClient,
    pub semantic: SemanticClient,
    pub log_reader: mpsc::UnboundedSender<LogReaderRequest>,
    pub log_writer: mpsc::UnboundedSender<LogWriterRequest>,
}

pub struct EngineEvents {
    pub log_reader: mpsc::Receiver<LogReaderEvent>,
    pub log_writer: mpsc::UnboundedReceiver<LogWriterResponse>,
    pub semantic: mpsc::UnboundedReceiver<SemanticEvent>,
}

pub struct EngineConfig {
    pub clients: EngineClients,
    pub events: EngineEvents,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        let EngineConfig { clients, events } = config;

        Self {
            fs_client: clients.fs,
            semantic_client: clients.semantic,
            log_reader_rx: events.log_reader,
            log_reader_tx: clients.log_reader,
            log_writer_rx: events.log_writer,
            log_writer_tx: clients.log_writer,
            semantic_event_rx: events.semantic,
            phase: EnginePhase::Idle,
            cleanup_queue: VecDeque::new(),
            shared_message_buffer: SharedMessageBuffer::default(),
        }
    }

    fn coalescing_arm_at(
        timer: &mut Pin<&mut MuxTimer<{ TimerEvent::VARIANT_COUNT }>>,
        event: TimerEvent,
        fire_at: Instant,
    ) {
        (*timer)
            .as_mut()
            .fire_at(event as usize, fire_at, CoalesceMode::Earliest);
    }

    fn cancel_timer(
        timer: &mut Pin<&mut MuxTimer<{ TimerEvent::VARIANT_COUNT }>>,
        event: TimerEvent,
    ) {
        (*timer).as_mut().cancel(event as usize);
    }

    fn set_phase(&mut self, phase: EnginePhase) {
        let from = self.phase.name();
        let to = phase.name();
        trace!(from, to, "engine phase changed");
        self.phase = phase;
    }

    fn start_scan(&mut self) -> eyre::Result<()> {
        self.fs_client.scan(ScanScope::Full)?;
        self.set_phase(EnginePhase::Scanning);
        Ok(())
    }

    fn maybe_start_periodic_scan(&mut self) -> eyre::Result<()> {
        if matches!(self.phase, EnginePhase::Idle) && self.fs_client.can_accept_scan() {
            self.start_scan()?;
        }
        Ok(())
    }

    fn handle_next_work(&mut self, next_work: NextWork) {
        let phase = match next_work {
            NextWork::None => EnginePhase::Idle,
            NextWork::Import(started) => EnginePhase::Importing {
                epoch: started.epoch,
                queue: started.plan.actions.into(),
                in_flight: 0,
                committing: false,
            },
            NextWork::Project(started) => EnginePhase::Projecting {
                epoch: started.epoch,
                generation: started.generation,
                plan: started.plan.actions.into(),
                in_flight: BTreeSet::new(),
                end_reason: None,
                committing: false,
            },
        };
        self.set_phase(phase);
    }

    fn handle_import_boundary_next_work(&mut self, next_work: NextWork) -> eyre::Result<()> {
        let should_check_projection = matches!(next_work, NextWork::None);
        self.handle_next_work(next_work);
        if should_check_projection {
            self.semantic_client.get_next_work()?;
        }
        Ok(())
    }

    fn drive_phase(&mut self) -> eyre::Result<()> {
        match &mut self.phase {
            EnginePhase::Idle | EnginePhase::Scanning => {}
            EnginePhase::Importing {
                epoch,
                queue,
                in_flight,
                committing,
            } => {
                while self.fs_client.can_accept_import() && !queue.is_empty() {
                    let action = queue.pop_front().expect("non-empty import queue");
                    self.fs_client.apply_import_action(action)?;
                    *in_flight += 1;
                }

                if queue.is_empty() && *in_flight == 0 && !*committing {
                    self.semantic_client.commit_import_epoch(*epoch)?;
                    *committing = true;
                }
            }
            EnginePhase::Projecting {
                epoch,
                generation: _,
                plan,
                in_flight,
                end_reason,
                committing,
            } => {
                while end_reason.is_none()
                    && self.fs_client.can_accept_projection()
                    && !plan.is_empty()
                {
                    let action = plan.pop_front().expect("non-empty projection plan");
                    let action_id = action.id;
                    self.fs_client.apply_projection_action(action)?;
                    in_flight.insert(action_id);
                }

                if (end_reason.is_some() || plan.is_empty()) && in_flight.is_empty() && !*committing
                {
                    let reason = end_reason
                        .take()
                        .unwrap_or(ProjectionEpochEndReason::PlanExhausted);
                    self.semantic_client
                        .commit_projection_epoch(*epoch, reason)?;
                    *committing = true;
                }
            }
        }

        Ok(())
    }

    fn drive_cleanup(&mut self) -> eyre::Result<()> {
        while self.fs_client.can_accept_cleanup() && !self.cleanup_queue.is_empty() {
            let path = self
                .cleanup_queue
                .pop_front()
                .expect("non-empty cleanup queue");
            self.fs_client.delete_if_exists(path)?;
        }

        Ok(())
    }

    fn handle_projection_changed(
        &mut self,
        changed_generation: ProjectionGeneration,
    ) -> eyre::Result<()> {
        match &mut self.phase {
            EnginePhase::Idle => {
                self.semantic_client.get_next_work()?;
            }
            EnginePhase::Projecting {
                generation,
                end_reason,
                ..
            } => {
                if changed_generation > *generation && end_reason.is_none() {
                    *end_reason = Some(ProjectionEpochEndReason::ProjectionChanged {
                        generation: changed_generation,
                    });
                }
            }
            EnginePhase::Scanning | EnginePhase::Importing { .. } => {
                // The phase boundary will ask Semantic for authoritative next work.
            }
        }

        Ok(())
    }

    fn handle_fs_response(&mut self, response: FsClientResponse) -> eyre::Result<()> {
        match response {
            FsClientResponse::Scan { result } => {
                match self.phase {
                    EnginePhase::Scanning => {}
                    _ => eyre::bail!("scan completed while engine was not scanning"),
                }
                self.semantic_client.apply_scan(result?)?;
            }
            FsClientResponse::ApplyImportAction { result } => {
                let EnginePhase::Importing {
                    epoch, in_flight, ..
                } = &mut self.phase
                else {
                    eyre::bail!("import action completed while engine was not importing");
                };

                *in_flight = in_flight
                    .checked_sub(1)
                    .ok_or_else(|| eyre!("import in_flight underflow"))?;
                let action_epoch = match &result {
                    crate::semantic::types::ImportActionResult::Read { action_id, .. }
                    | crate::semantic::types::ImportActionResult::Stat { action_id, .. } => {
                        action_id.epoch
                    }
                };
                if action_epoch != *epoch {
                    eyre::bail!(
                        "import action completed for epoch {:?}, but current epoch is {:?}",
                        action_epoch,
                        epoch
                    );
                }
                self.semantic_client.commit_import_action(result)?;
            }
            FsClientResponse::ApplyProjectionAction { result } => {
                let result = result?;
                let id = result.action_id();
                let cleanup_path = match &result {
                    ProjectionActionResult::WriteFile {
                        result: GuardedWriteResult::ConflictBeforeSwap { swap_path, .. },
                        ..
                    } => Some(swap_path.clone()),
                    _ => None,
                };

                {
                    let EnginePhase::Projecting {
                        epoch,
                        in_flight,
                        end_reason,
                        ..
                    } = &mut self.phase
                    else {
                        eyre::bail!("projection action completed while engine was not projecting");
                    };

                    if id.epoch != *epoch {
                        eyre::bail!(
                            "projection action completed for epoch {:?}, but current epoch is {:?}",
                            id.epoch,
                            epoch
                        );
                    }
                    if !in_flight.remove(&id) {
                        eyre::bail!("projection action {id:?} completed but was not in-flight");
                    }
                    if result.invalidates_projection() {
                        if end_reason.is_none() {
                            *end_reason =
                                Some(ProjectionEpochEndReason::ActionInvalidatedProjection {
                                    action_id: id,
                                });
                        }
                    }
                }

                if let Some(path) = cleanup_path {
                    self.cleanup_queue.push_back(path);
                }
                self.semantic_client.commit_projection_action(result)?;
            }
            FsClientResponse::DeleteIfExists { path, result } => {
                if let Err(error) = result {
                    tracing::warn!(%path, ?error, "cleanup delete-if-exists failed");
                }
            }
        }

        Ok(())
    }

    pub async fn run(mut self, token: CancellationToken) -> eyre::Result<()> {
        let timer = MuxTimer::<{ TimerEvent::VARIANT_COUNT }>::default();
        tokio::pin!(timer);

        self.start_scan()?;
        Self::coalescing_arm_at(
            &mut timer,
            TimerEvent::FullScan,
            Instant::now() + FULL_SCAN_INTERVAL,
        );

        loop {
            self.drive_cleanup()?;
            self.drive_phase()?;

            tokio::select! {
                _ = token.cancelled() => {
                    debug!("cancelled");

                    return Ok(());
                }

                (event_ord, _deadline) = &mut timer, if timer.is_armed() => {
                    let event = TimerEvent::from_ordinal(event_ord as i8).expect("valid event ordinal");
                    trace!(?event);
                    match event {
                        TimerEvent::SharedMessageBufferDrain => {
                            if self.semantic_client.can_accept_apply_shared_message_batch() {
                                let buffer = std::mem::take(&mut self.shared_message_buffer);
                                assert_ne!(buffer.envelopes.len(), 0);
                                let batch = SharedMessageBatch::try_from(buffer.envelopes)?;
                                Self::cancel_timer(&mut timer, TimerEvent::SharedMessageBufferDrain);
                                self.semantic_client.apply_shared_message_batch(batch)?;
                            }
                        }
                        TimerEvent::FullScan => {
                            self.maybe_start_periodic_scan()?;
                            Self::coalescing_arm_at(
                                &mut timer,
                                TimerEvent::FullScan,
                                Instant::now() + FULL_SCAN_INTERVAL,
                            );
                        }
                    }
                }

                Some(reader_event) = self.log_reader_rx.recv(), if self.shared_message_buffer.has_capacity() => {
                    match reader_event {
                        LogReaderEvent::Status{ .. } => {}
                        LogReaderEvent::Read(envelope) => {
                            self.shared_message_buffer.insert(envelope);
                            if self.semantic_client.can_accept_apply_shared_message_batch() {
                                Self::coalescing_arm_at(
                                    &mut timer,
                                    TimerEvent::SharedMessageBufferDrain,
                                    self.shared_message_buffer.next_fire_at().expect("due fire"),
                                );
                            }
                        }
                    }
                }

                Some(writer_event) = self.log_writer_rx.recv() => {
                    match writer_event {
                        LogWriterResponse::Ping => {},
                        LogWriterResponse::Durable{ outbox_range } => {
                            self.semantic_client.trim_outbox(outbox_range)?;
                        }
                    }
                }

                Some(response) = self.semantic_client.next() => {
                    match response {
                        SemanticClientResponse::ApplyInitScan(_) => {
                            eyre::bail!("sync engine received init-only semantic response");
                        }
                        SemanticClientResponse::ApplyScan(result) |
                        SemanticClientResponse::CommitImportEpoch(result) => {
                            self.handle_import_boundary_next_work(result?)?;
                        }
                        SemanticClientResponse::CommitProjectionEpoch(result) |
                        SemanticClientResponse::GetNextWork(result) => {
                            self.handle_next_work(result?);
                        }
                        SemanticClientResponse::ApplySharedMessageBatch(result) => {
                            result?;
                            if let Some(next_fire_at) = self.shared_message_buffer.next_fire_at() {
                                Self::coalescing_arm_at(&mut timer, TimerEvent::SharedMessageBufferDrain, next_fire_at);
                            }
                        }
                        SemanticClientResponse::ReadOutbox(result) => {
                            for (outbox_id, shared_message) in result? {
                                self.log_writer_tx.send(LogWriterRequest::Append {
                                    outbox_id,
                                    shared_message,
                                })?;
                            }
                        }
                    }
                }

                Some(response) = self.fs_client.next() => {
                    self.handle_fs_response(response)?;
                }

                Some(event) = self.semantic_event_rx.recv() => {
                    match event {
                        SemanticEvent::OutboxReady{ num_messages } => {
                            self.semantic_client.read_outbox(num_messages)?;
                        }
                        SemanticEvent::ProjectionChanged{ generation } => {
                            self.handle_projection_changed(generation)?;
                        }
                    }
                }

                else => {
                    return Err(eyre!("unrecoverable error"));
                }
            }
        }
    }
}
