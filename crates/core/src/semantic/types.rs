use crate::crdt::types::{NamespaceClaimId, ObjectId, SharedMessage};
use crate::fs::types::{
    ExpectedBefore, FileFingerprint, GuardedDeleteResult, GuardedReadResult, GuardedWriteResult,
    RelativePath, ScanResult, TreeEntry,
};
use crate::types::{OutboxId, SharedMessageBatch};
use bytes::Bytes;
use std::ops::RangeToInclusive;
use tokio::sync::oneshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ImportEpoch(u64);

impl ImportEpoch {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionEpoch(u64);

impl ProjectionEpoch {
    pub fn new(value: u64) -> Self {
        Self(value)
    }
}

/// The generation is incremented any time Semantic commits new stable/prior
/// inputs that may change a projection.
///
/// Not every generation will be represented in a projection epoch, which
/// represents a specific attempt to project semantic state, (at a generation)
/// into the filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionGeneration(u64);

impl ProjectionGeneration {
    pub fn new(value: u64) -> Self {
        Self(value)
    }
}

/// Always assigned by Semantic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ImportActionId {
    pub epoch: ImportEpoch,
    pub seq: u64,
}

impl ImportActionId {
    pub fn new(epoch: ImportEpoch, seq: u64) -> Self {
        Self { epoch, seq }
    }
}

/// Always assigned by Semantic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionActionId {
    pub epoch: ProjectionEpoch,
    pub seq: u64,
}

impl ProjectionActionId {
    pub fn new(epoch: ProjectionEpoch, seq: u64) -> Self {
        Self { epoch, seq }
    }
}

pub enum NextWork {
    None,
    Import(ImportEpochStarted),
    Project(ProjectionEpochStarted),
}

pub struct ImportEpochStarted {
    pub epoch: ImportEpoch,
    pub plan: ImportPlan,
}

pub struct ProjectionEpochStarted {
    pub epoch: ProjectionEpoch,
    pub generation: ProjectionGeneration,
    pub target_namespace_doc_blob: Bytes,
    pub plan: ProjectionPlan,
}

pub struct ImportPlan {
    pub actions: Vec<ImportAction>,
}

impl ImportPlan {
    pub fn empty() -> Self {
        Self {
            actions: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct ImportAction {
    pub id: ImportActionId,
    pub kind: ImportActionKind,
}

#[derive(Clone, Debug)]
pub enum ImportActionKind {
    Read {
        path: RelativePath,
        expected_fingerprint: FileFingerprint,
    },
    Stat {
        path: RelativePath,
    },
}

#[derive(Debug)]
pub enum ImportActionResult {
    Read {
        action_id: ImportActionId,
        outcome: ImportReadOutcome,
    },
    Stat {
        action_id: ImportActionId,
        outcome: ImportStatOutcome,
    },
}

impl ImportActionResult {
    pub fn action_id(&self) -> ImportActionId {
        match self {
            Self::Read { action_id, .. } | Self::Stat { action_id, .. } => *action_id,
        }
    }

    pub fn invalidates_import(&self) -> bool {
        matches!(
            self,
            Self::Read {
                outcome: ImportReadOutcome::Completed(
                    GuardedReadResult::ChangedBetweenStats { .. }
                        | GuardedReadResult::ConflictBeforeRead { .. }
                ),
                ..
            }
        )
    }
}

pub struct ReadIntent {
    pub action_id: ImportActionId,
    pub path: RelativePath,
    pub expected_fingerprint: FileFingerprint,
}

pub struct StatIntent {
    pub action_id: ImportActionId,
    pub path: RelativePath,
}

#[derive(Debug)]
pub enum ImportReadOutcome {
    Completed(GuardedReadResult),
    Failed(eyre::Report),
}

#[derive(Debug)]
pub enum ImportStatOutcome {
    Completed(Option<TreeEntry>),
    Failed(eyre::Report),
}

/// Essentially the API accessible to the main engine.
pub enum SemanticRequest {
    /// Bootstrap a new workspace from an initial full scan.
    ///
    /// Unlike normal sync `ApplyScan`, this starts from an empty semantic
    /// baseline and creates prior/stable state together.
    ApplyInitScan {
        scan: ScanResult,
        reply: oneshot::Sender<eyre::Result<NextWork>>,
    },

    /// Deliver the result of a scan. If work is needed, Semantic creates the
    /// relevant epoch internally and returns it in `NextWork`.
    ApplyScan {
        scan: ScanResult,
        reply: oneshot::Sender<eyre::Result<NextWork>>,
    },

    /// Deliver one completed import action result. This is fire-and-forget;
    /// `CommitImportEpoch` is the durability/accounting barrier.
    CommitImportAction { result: ImportActionResult },

    /// Close an import epoch. Engine fetches next work at its boundary via
    /// `GetNextWork` once the commit confirms.
    CommitImportEpoch {
        epoch: ImportEpoch,
        reply: oneshot::Sender<eyre::Result<()>>,
    },

    /// Deliver one completed projection action result. This is fire-and-forget;
    /// `CommitProjectionEpoch` is the durability/accounting barrier.
    CommitProjectionAction { result: ProjectionActionResult },

    /// Close a projection epoch. Engine fetches next work at its boundary via
    /// `GetNextWork` once the commit confirms.
    CommitProjectionEpoch {
        epoch: ProjectionEpoch,
        reason: ProjectionEpochEndReason,
        reply: oneshot::Sender<eyre::Result<()>>,
    },

    /// Ask Semantic to compute the next phase-boundary work from current state.
    GetNextWork {
        reply: oneshot::Sender<eyre::Result<NextWork>>,
    },

    /// Read from the head of the durable outbox.
    ReadOutbox {
        num_messages: u64,
        reply: oneshot::Sender<eyre::Result<Vec<(OutboxId, SharedMessage)>>>,
    },

    /// Notify semantic state that it is safe to trim messages from the head of the outbox.
    ///
    /// This is safe only after engine has appended, and witnessed acknowledgements,
    /// the associated messages to the shared log.
    TrimOutbox { through: RangeToInclusive<OutboxId> },

    /// Apply shared log messages to stable semantic state.
    ApplySharedMessageBatch {
        batch: SharedMessageBatch,
        reply: oneshot::Sender<eyre::Result<()>>,
    },
}

/// Messages sent, unsolicited, from semantic state to engine.
pub enum SemanticEvent {
    /// Notify Engine that durable outbox has messages ready for the shared log.
    OutboxReady { num_messages: u64 },
    /// Hint that stable/prior projection changed and cached projection work may
    /// now be stale. Engine may defer acting on this until a phase boundary.
    ProjectionChanged {
        /// The new/current projection input generation after the semantic change.
        /// No projection epoch is necessarily associated with this generation yet.
        generation: ProjectionGeneration,
    },
}

/// A projection plan is a set of steps to perform on a filesystem to bring it
/// into line with Semantic's stable state as of the time the plan was generated.
pub struct ProjectionPlan {
    pub actions: Vec<ProjectionAction>,
}

impl ProjectionPlan {
    pub fn empty() -> Self {
        Self {
            actions: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct ProjectionAction {
    /// Assigned by Semantic.
    pub id: ProjectionActionId,
    pub kind: ProjectionActionKind,
    /// Lightweight data Semantic needs to accept a successful projection
    /// without retaining the action's possibly-large file bytes in actor memory.
    pub commit_context: ProjectionActionCommitContext,
}

#[derive(Clone, derivative::Derivative)]
#[derivative(Debug)]
pub struct ProjectionActionCommitContext {
    pub path: RelativePath,
    pub claimed_path: RelativePath,
    pub claim_id: NamespaceClaimId,
    pub object_id: ObjectId,
    #[derivative(Debug = "ignore")]
    pub target_text_doc_blob: Option<Bytes>,
}

#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub enum ProjectionActionKind {
    WriteFile {
        path: RelativePath,
        #[derivative(Debug = "ignore")]
        bytes: Bytes,
        expected_before: ExpectedBefore,
    },
    DeleteFile {
        path: RelativePath,
        expected_before: ExpectedBefore,
    },
}

pub enum ProjectionActionResult {
    WriteFile {
        action_id: ProjectionActionId,
        result: GuardedWriteResult,
    },
    DeleteFile {
        action_id: ProjectionActionId,
        result: GuardedDeleteResult,
    },
    Failed {
        action_id: ProjectionActionId,
        error: eyre::Report,
    },
}

impl ProjectionActionResult {
    pub fn action_id(&self) -> ProjectionActionId {
        match self {
            Self::WriteFile { action_id, .. }
            | Self::DeleteFile { action_id, .. }
            | Self::Failed { action_id, .. } => *action_id,
        }
    }

    pub fn invalidates_projection(&self) -> bool {
        match self {
            Self::WriteFile {
                result:
                    GuardedWriteResult::Written { .. } | GuardedWriteResult::AlreadyApplied { .. },
                ..
            }
            | Self::DeleteFile {
                result: GuardedDeleteResult::Deleted | GuardedDeleteResult::AlreadyDeleted,
                ..
            } => false,
            Self::WriteFile {
                result:
                    GuardedWriteResult::ConflictBeforeSwap { .. }
                    | GuardedWriteResult::ConflictAfterSwap { .. }
                    | GuardedWriteResult::Conflict { .. },
                ..
            }
            | Self::DeleteFile {
                result: GuardedDeleteResult::Conflict { .. },
                ..
            }
            | Self::Failed { .. } => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionEpochEndReason {
    PlanExhausted,
    ActionInvalidatedProjection { action_id: ProjectionActionId },
}
