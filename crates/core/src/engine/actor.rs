use super::shared_message_buffer::SharedMessageBuffer;
use crate::app::connectivity::{ConnectivityRole, ConnectivitySnapshot, LinkStatus};
use crate::app::control::{DaemonStatus, EnginePhaseStatus};
use crate::crdt::types::SharedMessage;
use crate::fs::client::{FsClient, FsClientResponse};
use crate::fs::types::{GuardedWriteResult, RelativePath, ScanScope};
use crate::log::types::{
    LogReaderEvent, LogReaderRequest, LogWriterRequest, LogWriterResponse, SequenceNumber,
};
use crate::semantic::client::{SemanticClient, SemanticClientResponse};
use crate::semantic::types::{
    ImportAction, ImportActionId, ImportEpoch, NextWork, ProjectionAction, ProjectionActionId,
    ProjectionActionResult, ProjectionEpoch, ProjectionEpochEndReason, ProjectionGeneration,
    SemanticEvent,
};
use crate::spy::{NamespaceSpyTracker, NamespaceUpdateSummary, SpyEvent, SpyOpen};
use crate::types::{DaemonWriterId, SharedMessageBatch, WorkspaceId};
use enum_ordinalize::Ordinalize;
use eyre::eyre;
use std::collections::{BTreeSet, VecDeque};
use std::ops::{RangeInclusive, RangeTo};
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::Instant;
use tokio_muxt::{CoalesceMode, MuxTimer};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

const MAX_SHARED_MESSAGE_BUFFER_MS: Duration = Duration::from_millis(10);
const FULL_SCAN_INTERVAL: Duration = Duration::from_millis(120000);
const READ_OUTBOX_BATCH_SIZE: u64 = 1024;
const RECONNECT_INTERVAL: Duration = Duration::from_secs(5);
const STOP_RESPONSE_GRACE: Duration = Duration::from_millis(50);

#[derive(Ordinalize, Debug, Clone, Copy, PartialEq, Eq)]
enum TimerEvent {
    SharedMessageBufferDrain,
    FullScan,
    ReaderReconnect,
    WriterReconnect,
    Stop,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LinkRuntimeState {
    Online,
    Reconnecting,
    Offline,
}

#[derive(Debug, Clone)]
struct LinkRuntime {
    state: LinkRuntimeState,
    last_error: Option<String>,
    retry_at: Option<Instant>,
    retry_at_wall: Option<OffsetDateTime>,
}

impl LinkRuntime {
    fn reconnecting() -> Self {
        Self {
            state: LinkRuntimeState::Reconnecting,
            last_error: None,
            retry_at: None,
            retry_at_wall: None,
        }
    }

    fn online(&mut self) {
        self.state = LinkRuntimeState::Online;
        self.last_error = None;
        self.retry_at = None;
        self.retry_at_wall = None;
    }

    fn offline(&mut self, reason: String, retry_at: Instant, retry_at_wall: OffsetDateTime) {
        self.state = LinkRuntimeState::Offline;
        self.last_error = Some(reason);
        self.retry_at = Some(retry_at);
        self.retry_at_wall = Some(retry_at_wall);
    }

    fn reconnect(&mut self) {
        self.state = LinkRuntimeState::Reconnecting;
        self.retry_at = None;
        self.retry_at_wall = None;
    }

    fn is_online(&self) -> bool {
        self.state == LinkRuntimeState::Online
    }

    fn status(&self) -> LinkStatus {
        match self.state {
            LinkRuntimeState::Online => LinkStatus::online(),
            LinkRuntimeState::Reconnecting => LinkStatus::reconnecting(self.last_error.clone()),
            LinkRuntimeState::Offline => LinkStatus::offline(
                self.last_error
                    .clone()
                    .unwrap_or_else(|| "connection failed".to_string()),
                self.retry_at_wall.unwrap_or_else(OffsetDateTime::now_utc),
            ),
        }
    }
}

#[derive(Debug, Clone)]
struct EngineConnectivity {
    reader: LinkRuntime,
    writer: LinkRuntime,
}

impl EngineConnectivity {
    fn new() -> Self {
        Self {
            reader: LinkRuntime::reconnecting(),
            writer: LinkRuntime::reconnecting(),
        }
    }

    fn link(&self, role: ConnectivityRole) -> &LinkRuntime {
        match role {
            ConnectivityRole::Reader => &self.reader,
            ConnectivityRole::Writer => &self.writer,
        }
    }

    fn link_mut(&mut self, role: ConnectivityRole) -> &mut LinkRuntime {
        match role {
            ConnectivityRole::Reader => &mut self.reader,
            ConnectivityRole::Writer => &mut self.writer,
        }
    }

    fn snapshot(&self) -> ConnectivitySnapshot {
        ConnectivitySnapshot::from_links(self.reader.status(), self.writer.status())
    }
}

fn coalesce_scan_scopes(left: ScanScope, right: ScanScope) -> ScanScope {
    match (&left, &right) {
        (ScanScope::Full, _) | (_, ScanScope::Full) => ScanScope::Full,
        (ScanScope::SingleFile(left_path), ScanScope::SingleFile(right_path))
            if left_path == right_path =>
        {
            left
        }
        (ScanScope::Subtree(left_path), scope) if subtree_contains_scope(left_path, scope) => left,
        (scope, ScanScope::Subtree(right_path)) if subtree_contains_scope(right_path, scope) => {
            right
        }
        _ => ScanScope::Full,
    }
}

fn subtree_contains_scope(subtree: &RelativePath, scope: &ScanScope) -> bool {
    match scope {
        ScanScope::Full => false,
        ScanScope::SingleFile(path) | ScanScope::Subtree(path) => {
            path == subtree || path.as_components().starts_with(subtree.as_components())
        }
    }
}

fn semantic_response_kind(response: &SemanticClientResponse) -> &'static str {
    match response {
        SemanticClientResponse::ApplyInitScan(_) => "apply_init_scan",
        SemanticClientResponse::ApplyScan(_) => "apply_scan",
        SemanticClientResponse::CommitImportEpoch(_) => "commit_import_epoch",
        SemanticClientResponse::CommitProjectionEpoch(_) => "commit_projection_epoch",
        SemanticClientResponse::GetNextWork { .. } => "get_next_work",
        SemanticClientResponse::ApplySharedMessageBatch { .. } => "apply_shared_message_batch",
        SemanticClientResponse::ReadOutbox(_) => "read_outbox",
        SemanticClientResponse::ReleaseOutbox(_) => "release_outbox",
        SemanticClientResponse::ReadStableCursor(_) => "read_stable_cursor",
        SemanticClientResponse::ReadStableNamespace(_) => "read_stable_namespace",
    }
}

/// Monotone id stamped on each `get_next_work` request so its response can be
/// bound to the boundary that issued it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BoundarySeq(u64);

/// Engine phase machine. Every phase waits for exactly one kind of response,
/// every phase ends by entering a boundary (`enter_boundary`), and each
/// variant carries only the state that is legal to act on in that phase —
/// e.g. `Projecting::Committing` holds no plan, so late dispatch is
/// unrepresentable rather than guarded against.
#[derive(Debug)]
enum EnginePhase {
    /// No work and nothing pending. Exits via a scan request or a
    /// projection-changed signal.
    Idle,
    /// Exactly one `get_next_work` in flight; the only phase in which a
    /// next-work response is admissible.
    AwaitingNextWork {
        boundary: BoundarySeq,
        /// Stable changed while waiting; the in-flight response may predate
        /// the change, so a `None` answer must re-ask instead of going idle.
        dirty: bool,
    },
    /// Fs scan executing.
    Scanning {
        #[allow(dead_code)] // logged via Debug
        scope: ScanScope,
    },
    /// Semantic is turning the scan result into an import plan.
    PlanningImport,
    /// An import is in progress.
    Importing(ImportPhase),
    /// A projection plan is in progress.
    Projecting(ProjectionPhase),
}

impl EnginePhase {
    fn status(&self) -> EnginePhaseStatus {
        match self {
            Self::Idle => EnginePhaseStatus::Idle,
            Self::AwaitingNextWork { .. } => EnginePhaseStatus::AwaitingNextWork,
            Self::Scanning { .. } => EnginePhaseStatus::Scanning,
            Self::PlanningImport => EnginePhaseStatus::PlanningImport,
            Self::Importing(_) => EnginePhaseStatus::Importing,
            Self::Projecting(_) => EnginePhaseStatus::Projecting,
        }
    }
}

#[derive(Debug)]
enum ImportPhase {
    Dispatching {
        epoch: ImportEpoch,
        queue: VecDeque<ImportAction>,
        in_flight: BTreeSet<ImportActionId>,
    },
    Committing {
        #[allow(dead_code)] // logged via Debug
        epoch: ImportEpoch,
    },
}

#[derive(Debug)]
enum ProjectionPhase {
    Dispatching {
        epoch: ProjectionEpoch,
        generation: ProjectionGeneration,
        plan: VecDeque<ProjectionAction>,
        in_flight: BTreeSet<ProjectionActionId>,
    },
    /// An action invalidated the epoch: the plan was dropped on entry (no
    /// further dispatch is possible), and in-flight actions drain before the
    /// commit is requested.
    Draining {
        epoch: ProjectionEpoch,
        generation: ProjectionGeneration,
        reason: ProjectionEpochEndReason,
        in_flight: BTreeSet<ProjectionActionId>,
    },
    /// Epoch commit in flight. No plan and no in-flight set: an action
    /// completion in this phase is a bug, not a race.
    Committing {
        #[allow(dead_code)] // logged via Debug
        epoch: ProjectionEpoch,
        #[allow(dead_code)] // logged via Debug
        generation: ProjectionGeneration,
    },
}

#[derive(Debug)]
pub enum EngineCommand {
    Scan(ScanScope),
    Status {
        reply: oneshot::Sender<DaemonStatus>,
    },
    OpenSpy {
        reply: oneshot::Sender<Result<SpyOpen, String>>,
    },
    Stop {
        reply: oneshot::Sender<DaemonStatus>,
    },
}

#[derive(Debug, Clone)]
pub struct EngineStatusConfig {
    pub sync_root: PathBuf,
    pub workspace_id: WorkspaceId,
    pub daemon_writer_id: DaemonWriterId,
    pub stable_cursor: RangeTo<SequenceNumber>,
    pub started_at: OffsetDateTime,
}

#[derive(Debug, Clone)]
struct EngineStatusState {
    sync_root: PathBuf,
    workspace_id: WorkspaceId,
    daemon_writer_id: DaemonWriterId,
    denormalized_stable_cursor: RangeTo<SequenceNumber>,
    started_at: OffsetDateTime,
}

impl EngineStatusState {
    fn new(config: EngineStatusConfig) -> Self {
        Self {
            sync_root: config.sync_root,
            workspace_id: config.workspace_id,
            daemon_writer_id: config.daemon_writer_id,
            denormalized_stable_cursor: config.stable_cursor,
            started_at: config.started_at,
        }
    }

    fn update_stable_cursor_after_batch(
        &mut self,
        sequence_range: &RangeInclusive<SequenceNumber>,
    ) -> eyre::Result<()> {
        let stable_cursor_end = sequence_range
            .end()
            .checked_add(1)
            .ok_or_else(|| eyre!("shared message sequence number overflow"))?;
        self.denormalized_stable_cursor = ..stable_cursor_end;
        Ok(())
    }
}

pub struct Engine {
    fs_client: FsClient,
    semantic_client: SemanticClient,

    log_reader_rx: mpsc::Receiver<LogReaderEvent>,
    #[allow(dead_code)] // retained for future log reader commands
    log_reader_tx: mpsc::UnboundedSender<LogReaderRequest>,
    log_writer_rx: mpsc::UnboundedReceiver<LogWriterResponse>,
    log_writer_tx: mpsc::UnboundedSender<LogWriterRequest>,

    semantic_event_rx: mpsc::UnboundedReceiver<SemanticEvent>,
    command_rx: mpsc::UnboundedReceiver<EngineCommand>,
    spy_tx: Option<broadcast::Sender<SpyEvent>>,
    spy_namespace: NamespaceSpyTracker,

    status: EngineStatusState,
    phase: EnginePhase,
    /// Daemon-owned temp files left by failed guarded writes. These are GC-only:
    /// Semantic still receives the original projection conflict result.
    cleanup_queue: VecDeque<RelativePath>,

    /// Buffer shared messages received from log reader before sending
    /// for semantic actor to apply.
    shared_message_buffer: SharedMessageBuffer,

    /// `read_outbox` reserves rows in the DB. Keep only one reservation request
    /// in flight so batches are handed to LogWriter in outbox-id order.
    outbox_read_in_flight: bool,
    outbox_read_requested: bool,
    outbox_release_in_flight: bool,
    outbox_release_requested: bool,
    reader_reconnect_cursor_in_flight: bool,
    connectivity: EngineConnectivity,

    /// Monotone source for `BoundarySeq` stamps. `GetNextWork` reserves
    /// projection epochs in Semantic, so it is single-flight by construction:
    /// only `AwaitingNextWork` has one outstanding.
    next_boundary: u64,
    pending_scan_scope: Option<ScanScope>,
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
    pub commands: mpsc::UnboundedReceiver<EngineCommand>,
    pub spy: Option<broadcast::Sender<SpyEvent>>,
}

pub struct EngineConfig {
    pub clients: EngineClients,
    pub events: EngineEvents,
    pub status: EngineStatusConfig,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Self {
        let EngineConfig {
            clients,
            events,
            status,
        } = config;

        Self {
            fs_client: clients.fs,
            semantic_client: clients.semantic,
            log_reader_rx: events.log_reader,
            log_reader_tx: clients.log_reader,
            log_writer_rx: events.log_writer,
            log_writer_tx: clients.log_writer,
            semantic_event_rx: events.semantic,
            command_rx: events.commands,
            spy_tx: events.spy,
            spy_namespace: NamespaceSpyTracker::new(),
            status: EngineStatusState::new(status),
            phase: EnginePhase::Idle,
            cleanup_queue: VecDeque::new(),
            shared_message_buffer: SharedMessageBuffer::new(MAX_SHARED_MESSAGE_BUFFER_MS),
            outbox_read_in_flight: false,
            outbox_read_requested: false,
            outbox_release_in_flight: false,
            outbox_release_requested: false,
            reader_reconnect_cursor_in_flight: false,
            connectivity: EngineConnectivity::new(),
            next_boundary: 0,
            pending_scan_scope: None,
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

    fn daemon_status(&self) -> DaemonStatus {
        DaemonStatus {
            workspace_id: self.status.workspace_id.0.clone(),
            root: self.status.sync_root.display().to_string(),
            pid: std::process::id(),
            stable_cursor_end: self.status.denormalized_stable_cursor.end,
            daemon_writer_id_b64: self.status.daemon_writer_id.encode_b64(),
            started_at_ns: i64::try_from(self.status.started_at.unix_timestamp_nanos())
                .expect("started_at timestamp nanos fit in i64"),
            engine_phase: self.phase.status(),
            connectivity: self.connectivity.snapshot(),
        }
    }

    async fn initialize_spy_namespace(&mut self) -> eyre::Result<()> {
        self.semantic_client.read_stable_namespace()?;
        match self
            .semantic_client
            .next()
            .await
            .ok_or_else(|| eyre!("semantic actor stopped while reading stable namespace"))?
        {
            SemanticClientResponse::ReadStableNamespace(result) => {
                self.spy_namespace.seed(result?.as_ref())?;
                Ok(())
            }
            response => eyre::bail!(
                "unexpected semantic response while initializing spy namespace: {}",
                semantic_response_kind(&response)
            ),
        }
    }

    fn mark_link_online(&mut self, role: ConnectivityRole) {
        let link = self.connectivity.link(role);
        let previous_state = link.state;
        let previous_error = link.last_error.clone();

        self.connectivity.link_mut(role).online();
        if previous_error.is_some() || matches!(previous_state, LinkRuntimeState::Offline) {
            info!(
                role = role.as_str(),
                ?previous_state,
                last_error = previous_error.as_deref(),
                "shared log link resumed online"
            );
        } else {
            debug!(role = role.as_str(), "shared log link online");
        }
    }

    fn mark_link_disconnected(
        &mut self,
        role: ConnectivityRole,
        reason: String,
        timer: &mut Pin<&mut MuxTimer<{ TimerEvent::VARIANT_COUNT }>>,
    ) -> eyre::Result<()> {
        let retry_at = Instant::now() + RECONNECT_INTERVAL;
        let retry_at_wall = OffsetDateTime::now_utc()
            + time::Duration::try_from(RECONNECT_INTERVAL)
                .expect("reconnect interval fits time::Duration");
        self.connectivity
            .link_mut(role)
            .offline(reason.clone(), retry_at, retry_at_wall);
        warn!(
            role = role.as_str(),
            %reason,
            retry_in_ms = RECONNECT_INTERVAL.as_millis(),
            "shared log link offline"
        );
        let event = match role {
            ConnectivityRole::Reader => TimerEvent::ReaderReconnect,
            ConnectivityRole::Writer => TimerEvent::WriterReconnect,
        };
        Self::coalescing_arm_at(timer, event, retry_at);
        if role == ConnectivityRole::Writer {
            self.outbox_read_requested = true;
            self.request_outbox_release()?;
        }
        Ok(())
    }

    fn mark_link_reconnecting(&mut self, role: ConnectivityRole) {
        self.connectivity.link_mut(role).reconnect();
        debug!(role = role.as_str(), "shared log link reconnecting");
    }

    fn link_is_online(&self, role: ConnectivityRole) -> bool {
        self.connectivity.link(role).is_online()
    }

    fn set_phase(&mut self, phase: EnginePhase) {
        let from = &self.phase;
        let to = &phase;
        trace!(?from, ?to, "engine phase changed");
        self.phase = phase;
    }

    /// Every phase ends here. Pending scan evidence wins over asking for
    /// stable-derived work; otherwise issue exactly one boundary-stamped
    /// `get_next_work`.
    fn enter_boundary(&mut self) -> eyre::Result<()> {
        if let Some(scope) = self.pending_scan_scope.take() {
            self.fs_client.scan(scope.clone())?;
            self.set_phase(EnginePhase::Scanning { scope });
        } else {
            self.next_boundary += 1;
            let boundary = BoundarySeq(self.next_boundary);
            self.semantic_client.get_next_work(boundary.0)?;
            self.set_phase(EnginePhase::AwaitingNextWork {
                boundary,
                dirty: false,
            });
        }
        Ok(())
    }

    fn request_scan(&mut self, scope: ScanScope) -> eyre::Result<()> {
        self.pending_scan_scope = Some(match self.pending_scan_scope.take() {
            Some(pending) => coalesce_scan_scopes(pending, scope),
            None => scope,
        });
        if matches!(self.phase, EnginePhase::Idle) {
            self.enter_boundary()?;
        }
        Ok(())
    }

    /// Dispatch admissible work and request epoch commits. Idempotent; runs
    /// at the top of every loop iteration. Commit requests transition to the
    /// `Committing` variants in the same step, so they cannot be re-sent and
    /// nothing can be dispatched afterwards.
    fn drive_phase(&mut self) -> eyre::Result<()> {
        match &mut self.phase {
            EnginePhase::Idle
            | EnginePhase::AwaitingNextWork { .. }
            | EnginePhase::Scanning { .. }
            | EnginePhase::PlanningImport => {}
            EnginePhase::Importing(ImportPhase::Dispatching {
                queue, in_flight, ..
            }) => {
                while self.fs_client.can_accept_import() && !queue.is_empty() {
                    let action = queue.pop_front().expect("non-empty import queue");
                    let action_id = action.id;
                    self.fs_client.apply_import_action(action)?;
                    in_flight.insert(action_id);
                }
            }
            EnginePhase::Importing(ImportPhase::Committing { .. }) => {}
            EnginePhase::Projecting(ProjectionPhase::Dispatching {
                plan, in_flight, ..
            }) => {
                while self.fs_client.can_accept_projection() && !plan.is_empty() {
                    let action = plan.pop_front().expect("non-empty projection plan");
                    let action_id = action.id;
                    trace!(?action_id, "projection action dispatched");
                    self.fs_client.apply_projection_action(action)?;
                    in_flight.insert(action_id);
                }
            }
            EnginePhase::Projecting(
                ProjectionPhase::Draining { .. } | ProjectionPhase::Committing { .. },
            ) => {}
        }

        enum CommitTransition {
            Import(ImportEpoch),
            Projection(
                ProjectionEpoch,
                ProjectionGeneration,
                ProjectionEpochEndReason,
            ),
        }

        let commit = match &self.phase {
            EnginePhase::Importing(ImportPhase::Dispatching {
                epoch,
                queue,
                in_flight,
            }) if queue.is_empty() && in_flight.is_empty() => {
                Some(CommitTransition::Import(*epoch))
            }
            EnginePhase::Projecting(ProjectionPhase::Dispatching {
                epoch,
                generation,
                plan,
                in_flight,
            }) if plan.is_empty() && in_flight.is_empty() => Some(CommitTransition::Projection(
                *epoch,
                *generation,
                ProjectionEpochEndReason::PlanExhausted,
            )),
            EnginePhase::Projecting(ProjectionPhase::Draining {
                epoch,
                generation,
                reason,
                in_flight,
            }) if in_flight.is_empty() => {
                Some(CommitTransition::Projection(*epoch, *generation, *reason))
            }
            _ => None,
        };

        match commit {
            Some(CommitTransition::Import(epoch)) => {
                self.semantic_client.commit_import_epoch(epoch)?;
                self.set_phase(EnginePhase::Importing(ImportPhase::Committing { epoch }));
            }
            Some(CommitTransition::Projection(epoch, generation, reason)) => {
                self.semantic_client
                    .commit_projection_epoch(epoch, reason)?;
                self.set_phase(EnginePhase::Projecting(ProjectionPhase::Committing {
                    epoch,
                    generation,
                }));
            }
            None => {}
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

    fn request_outbox_read(&mut self) -> eyre::Result<()> {
        self.outbox_read_requested = true;
        self.maybe_start_outbox_read()
    }

    fn maybe_start_outbox_read(&mut self) -> eyre::Result<()> {
        if self.outbox_read_requested
            && !self.outbox_read_in_flight
            && !self.outbox_release_in_flight
            && !self.outbox_release_requested
            && self.link_is_online(ConnectivityRole::Writer)
        {
            self.semantic_client.read_outbox(READ_OUTBOX_BATCH_SIZE)?;
            self.outbox_read_requested = false;
            self.outbox_read_in_flight = true;
        }

        Ok(())
    }

    fn request_outbox_release(&mut self) -> eyre::Result<()> {
        self.outbox_release_requested = true;
        self.maybe_start_outbox_release()
    }

    fn maybe_start_outbox_release(&mut self) -> eyre::Result<()> {
        if self.outbox_release_requested && !self.outbox_release_in_flight {
            self.semantic_client.release_outbox()?;
            self.outbox_release_requested = false;
            self.outbox_release_in_flight = true;
        }

        Ok(())
    }

    fn handle_get_next_work_response(
        &mut self,
        response_boundary: u64,
        result: eyre::Result<NextWork>,
    ) -> eyre::Result<()> {
        let EnginePhase::AwaitingNextWork { boundary, dirty } = &self.phase else {
            eyre::bail!(
                "get-next-work response (boundary {response_boundary}) while engine was not awaiting next work"
            );
        };
        if response_boundary != boundary.0 {
            eyre::bail!(
                "get-next-work response for boundary {response_boundary}, but engine is awaiting boundary {}",
                boundary.0
            );
        }
        let dirty = *dirty;

        match result? {
            NextWork::Project(started) => {
                self.set_phase(EnginePhase::Projecting(ProjectionPhase::Dispatching {
                    epoch: started.epoch,
                    generation: started.generation,
                    plan: started.plan.actions.into(),
                    in_flight: BTreeSet::new(),
                }));
            }
            NextWork::Import(started) => {
                eyre::bail!(
                    "get-next-work returned import epoch {:?}; imports start from scans",
                    started.epoch
                );
            }
            NextWork::None => {
                if dirty || self.pending_scan_scope.is_some() {
                    // Stable changed while the answer was being computed, or
                    // scan evidence is pending: re-enter the boundary instead
                    // of going idle on a possibly stale None.
                    self.enter_boundary()?;
                } else {
                    self.set_phase(EnginePhase::Idle);
                }
            }
        }

        Ok(())
    }

    fn handle_projection_changed(&mut self) -> eyre::Result<()> {
        match &mut self.phase {
            EnginePhase::Idle => self.enter_boundary(),
            EnginePhase::AwaitingNextWork { dirty, .. } => {
                *dirty = true;
                Ok(())
            }
            // Every other phase reaches a boundary that re-queries
            // authoritative next work; an active projection finishes as an
            // intermediate snapshot.
            _ => Ok(()),
        }
    }

    fn handle_fs_response(&mut self, response: FsClientResponse) -> eyre::Result<()> {
        match response {
            FsClientResponse::Scan { result } => {
                let EnginePhase::Scanning { .. } = &self.phase else {
                    eyre::bail!("scan completed while engine was not scanning");
                };
                self.semantic_client.apply_scan(result?)?;
                self.set_phase(EnginePhase::PlanningImport);
            }
            FsClientResponse::ApplyImportAction { result } => {
                let action_id = result.action_id();
                let import_invalidated = result.invalidates_import();
                let EnginePhase::Importing(ImportPhase::Dispatching {
                    epoch, in_flight, ..
                }) = &mut self.phase
                else {
                    eyre::bail!(
                        "import action {action_id:?} completed while engine was not dispatching imports"
                    );
                };

                if action_id.epoch != *epoch {
                    eyre::bail!(
                        "import action completed for epoch {:?}, but current epoch is {:?}",
                        action_id.epoch,
                        epoch
                    );
                }
                if !in_flight.remove(&action_id) {
                    eyre::bail!("import action {action_id:?} completed but was not in-flight");
                }
                if import_invalidated {
                    self.request_scan(ScanScope::Full)?;
                }
                self.semantic_client.commit_import_action(result)?;
            }
            FsClientResponse::ApplyProjectionAction { result } => {
                let result = result?;
                let id = result.action_id();
                if let ProjectionActionResult::WriteFile {
                    result: GuardedWriteResult::ConflictAfterSwap { observed },
                    ..
                } = &result
                {
                    warn!(
                        ?id,
                        ?observed,
                        "guarded write conflicted after swap; a local filesystem write may have been clobbered"
                    );
                }
                let cleanup_path = match &result {
                    ProjectionActionResult::WriteFile {
                        result: GuardedWriteResult::ConflictBeforeSwap { swap_path, .. },
                        ..
                    } => Some(swap_path.clone()),
                    _ => None,
                };
                let projection_invalidated = result.invalidates_projection();

                let invalidation_transition = match &mut self.phase {
                    EnginePhase::Projecting(ProjectionPhase::Dispatching {
                        epoch,
                        generation,
                        in_flight,
                        ..
                    }) => {
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
                        projection_invalidated.then(|| {
                            // Entering Draining drops the remaining plan:
                            // nothing further dispatches for this epoch.
                            EnginePhase::Projecting(ProjectionPhase::Draining {
                                epoch: *epoch,
                                generation: *generation,
                                reason: ProjectionEpochEndReason::ActionInvalidatedProjection {
                                    action_id: id,
                                },
                                in_flight: std::mem::take(in_flight),
                            })
                        })
                    }
                    EnginePhase::Projecting(ProjectionPhase::Draining {
                        epoch, in_flight, ..
                    }) => {
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
                        None
                    }
                    _ => {
                        eyre::bail!(
                            "projection action {id:?} completed while engine was not draining or dispatching projections"
                        );
                    }
                };
                if let Some(phase) = invalidation_transition {
                    self.set_phase(phase);
                }

                if let Some(path) = cleanup_path {
                    self.cleanup_queue.push_back(path);
                }
                if projection_invalidated {
                    self.request_scan(ScanScope::Full)?;
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

    fn apply_spy_namespace_update(
        &mut self,
        message: &SharedMessage,
    ) -> Option<NamespaceUpdateSummary> {
        let SharedMessage::NamespaceUpdate { yjs_update } = message else {
            return None;
        };

        match self.spy_namespace.try_apply_update(yjs_update.as_ref()) {
            Ok(summary) => Some(summary),
            Err(error) => {
                warn!(?error, "failed to update in-memory spy namespace");
                None
            }
        }
    }

    pub async fn run(mut self, token: CancellationToken) -> eyre::Result<()> {
        let timer = MuxTimer::<{ TimerEvent::VARIANT_COUNT }>::default();
        tokio::pin!(timer);

        self.initialize_spy_namespace().await?;
        self.request_scan(ScanScope::Full)?;
        self.request_outbox_read()?;
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
                                let envelopes = self.shared_message_buffer.drain();
                                assert_ne!(envelopes.len(), 0);
                                let batch = SharedMessageBatch::try_from(envelopes)?;
                                Self::cancel_timer(&mut timer, TimerEvent::SharedMessageBufferDrain);
                                self.semantic_client.apply_shared_message_batch(batch)?;
                            }
                        }
                        TimerEvent::FullScan => {
                            self.request_scan(ScanScope::Full)?;
                            Self::coalescing_arm_at(
                                &mut timer,
                                TimerEvent::FullScan,
                                Instant::now() + FULL_SCAN_INTERVAL,
                            );
                        }
                        TimerEvent::ReaderReconnect => {
                            if matches!(
                                self.connectivity.link(ConnectivityRole::Reader).state,
                                LinkRuntimeState::Offline
                            ) && !self.reader_reconnect_cursor_in_flight {
                                self.mark_link_reconnecting(ConnectivityRole::Reader);
                                self.semantic_client.read_stable_cursor()?;
                                self.reader_reconnect_cursor_in_flight = true;
                            }
                        }
                        TimerEvent::WriterReconnect => {
                            if matches!(
                                self.connectivity.link(ConnectivityRole::Writer).state,
                                LinkRuntimeState::Offline
                            ) {
                                self.mark_link_reconnecting(ConnectivityRole::Writer);
                                self.log_writer_tx.send(LogWriterRequest::Reconnect)?;
                            }
                        }
                        TimerEvent::Stop => {
                            info!("stop requested; engine exiting");
                            return Ok(());
                        }
                    }
                }

                Some(reader_event) = self.log_reader_rx.recv(), if self.shared_message_buffer.has_capacity() => {
                    trace!(?reader_event);
                    match reader_event {
                        LogReaderEvent::Connected => {
                            self.mark_link_online(ConnectivityRole::Reader);
                        }
                        LogReaderEvent::Disconnected { reason } => {
                            self.mark_link_disconnected(ConnectivityRole::Reader, reason, &mut timer)?;
                        }
                        LogReaderEvent::Status{ .. } => {}
                        LogReaderEvent::Read(envelope) => {
                            let namespace_summary = self.apply_spy_namespace_update(&envelope.shared_message);
                            if let Some(spy_tx) = &self.spy_tx {
                                let _ = spy_tx.send(SpyEvent::shared_message_with_namespace_summary(
                                    &envelope,
                                    namespace_summary,
                                ));
                            }
                            self.shared_message_buffer.insert(envelope);
                            if self.semantic_client.can_accept_apply_shared_message_batch() {
                                Self::coalescing_arm_at(
                                    &mut timer,
                                    TimerEvent::SharedMessageBufferDrain,
                                    self.shared_message_buffer.next_fire_at().expect("due fire"),
                                );
                            }
                        }
                        LogReaderEvent::Ended { cursor } => {
                            eyre::bail!(
                                "sync engine received unexpected bounded log reader end: {cursor:?}"
                            );
                        }
                    }
                }

                Some(writer_event) = self.log_writer_rx.recv() => {
                    match writer_event {
                        LogWriterResponse::Connected => {
                            self.mark_link_online(ConnectivityRole::Writer);
                            self.maybe_start_outbox_read()?;
                        }
                        LogWriterResponse::Disconnected { reason } => {
                            self.mark_link_disconnected(ConnectivityRole::Writer, reason, &mut timer)?;
                        }
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
                        SemanticClientResponse::ApplyScan(result) => {
                            let EnginePhase::PlanningImport = &self.phase else {
                                eyre::bail!("apply-scan response while engine was not planning an import");
                            };
                            match result? {
                                NextWork::Import(started) => {
                                    self.set_phase(EnginePhase::Importing(ImportPhase::Dispatching {
                                        epoch: started.epoch,
                                        queue: started.plan.actions.into(),
                                        in_flight: BTreeSet::new(),
                                    }));
                                }
                                NextWork::None => self.enter_boundary()?,
                                NextWork::Project(started) => {
                                    eyre::bail!(
                                        "apply-scan returned projection epoch {:?}; projections start at boundaries",
                                        started.epoch
                                    );
                                }
                            }
                        }
                        SemanticClientResponse::CommitImportEpoch(result) => {
                            result?;
                            let EnginePhase::Importing(ImportPhase::Committing { .. }) = &self.phase else {
                                eyre::bail!("import epoch commit response while engine was not committing an import epoch");
                            };
                            self.enter_boundary()?;
                        }
                        SemanticClientResponse::CommitProjectionEpoch(result) => {
                            result?;
                            let EnginePhase::Projecting(ProjectionPhase::Committing { .. }) = &self.phase else {
                                eyre::bail!("projection epoch commit response while engine was not committing a projection epoch");
                            };
                            self.enter_boundary()?;
                        }
                        SemanticClientResponse::GetNextWork { boundary, result } => {
                            self.handle_get_next_work_response(boundary, result)?;
                        }
                        SemanticClientResponse::ApplySharedMessageBatch {
                            sequence_range,
                            result,
                        } => {
                            result?;
                            self.status
                                .update_stable_cursor_after_batch(&sequence_range)?;
                            if let Some(next_fire_at) = self.shared_message_buffer.next_fire_at() {
                                Self::coalescing_arm_at(&mut timer, TimerEvent::SharedMessageBufferDrain, next_fire_at);
                            }
                        }
                        SemanticClientResponse::ReadOutbox(result) => {
                            assert!(
                                self.outbox_read_in_flight,
                                "read-outbox response without an in-flight read"
                            );
                            self.outbox_read_in_flight = false;
                            let messages = result?;
                            let read_any = !messages.is_empty();
                            if self.link_is_online(ConnectivityRole::Writer) {
                                for (outbox_id, shared_message) in messages {
                                    self.log_writer_tx.send(LogWriterRequest::Append {
                                        outbox_id,
                                        shared_message,
                                    })?;
                                }
                            } else if read_any {
                                self.request_outbox_release()?;
                            }
                            if read_any {
                                self.outbox_read_requested = true;
                            }
                            self.maybe_start_outbox_read()?;
                        }
                        SemanticClientResponse::ReleaseOutbox(result) => {
                            assert!(
                                self.outbox_release_in_flight,
                                "release-outbox response without an in-flight release"
                            );
                            self.outbox_release_in_flight = false;
                            let released = result?;
                            debug!(released, "released outbox reservations");
                            self.maybe_start_outbox_release()?;
                            self.maybe_start_outbox_read()?;
                        }
                        SemanticClientResponse::ReadStableCursor(result) => {
                            assert!(
                                self.reader_reconnect_cursor_in_flight,
                                "read-stable-cursor response without an in-flight reader reconnect"
                            );
                            self.reader_reconnect_cursor_in_flight = false;
                            let cursor = result?;
                            if matches!(
                                self.connectivity.link(ConnectivityRole::Reader).state,
                                LinkRuntimeState::Reconnecting
                            ) {
                                self.log_reader_tx.send(LogReaderRequest::Reconnect {
                                    start_at: cursor.end,
                                })?;
                            }
                        }
                        SemanticClientResponse::ReadStableNamespace(_) => {
                            eyre::bail!("sync engine received startup-only stable namespace response");
                        }
                    }
                }

                Some(response) = self.fs_client.next() => {
                    self.handle_fs_response(response)?;
                }

                Some(event) = self.semantic_event_rx.recv() => {
                    match event {
                        SemanticEvent::OutboxReady{ num_messages } => {
                            if num_messages > 0 {
                                self.request_outbox_read()?;
                            }
                        }
                        SemanticEvent::ProjectionChanged{ generation: _ } => {
                            self.handle_projection_changed()?;
                        }
                    }
                }

                Some(command) = self.command_rx.recv() => {
                    trace!(?command);
                    match command {
                        EngineCommand::Scan(scope) => {
                            self.request_scan(scope)?;
                        }
                        EngineCommand::Status { reply } => {
                            let _ = reply.send(self.daemon_status());
                        }
                        EngineCommand::OpenSpy { reply } => {
                            let result = self
                                .spy_tx
                                .as_ref()
                                .map(|spy_tx| SpyOpen {
                                    daemon_writer_id_b64: self.status.daemon_writer_id.encode_b64(),
                                    namespace_snapshot_b64: self.spy_namespace.snapshot_b64(),
                                    events: spy_tx.subscribe(),
                                })
                                .ok_or_else(|| "spy stream is not enabled".to_string());
                            let _ = reply.send(result);
                        }
                        EngineCommand::Stop { reply } => {
                            let _ = reply.send(self.daemon_status());
                            Self::coalescing_arm_at(
                                &mut timer,
                                TimerEvent::Stop,
                                Instant::now() + STOP_RESPONSE_GRACE,
                            );
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
