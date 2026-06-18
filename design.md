# Design Notes

- v0 syncs UTF-8 text files only. Empty files are text. Non-UTF-8 files are ignored for sync but may become syncable later.
- Local semantic mutations update stable CRDT state and insert outbox rows in the same transaction. No publish debounce.
- `stable_*` means durable semantic state: shared log applied up to `stable_cursor` plus committed local outbox mutations.
- `prior_*` means last known projected/accepted local disk baseline.
- Quiescent means `prior == stable` and there is no in-flight import/projection work.
- Projection moves disk/prior toward stable.
- Import compares current disk evidence against prior, produces CRDT updates, applies them to stable, and writes outbox.
- Scans are evidence, not durable observed state. Full/partial scan scope defines where absence means delete.
- `stable_tree` represents stable desired filesystem view derived from stable namespace/objects.
- Prior namespace/tree/text state is separate from stable state; prior may reference paths/objects no longer present in stable while projection/delete is pending.
- `objects` is append-only object identity, not stable/prior. All prior object ids were once stable object ids.
- Core tables: `daemon_state`, `objects`, `stable_namespace`, `prior_namespace`, `stable_text_objects`, `prior_text_objects`, `stable_tree`, `prior_tree`, `outbox`.
- Startup modes are explicit: `init`, `pull`, `sync`.
- `init`: one-shot bootstrap of a new empty shared workspace from local disk. Local disk is accepted, so it writes stable and prior together and inserts outbox messages. Then exits.
- `pull`: one-shot checkout of an existing workspace into an empty local dir. Builds stable from shared log, projects to disk, records prior, then exits.
- `sync`: long-running mode for an already bootstrapped checkout. Loads DB, starts actors, runs startup scan, then processes scans/log/projection.
- Engine sequences work and may drop invalid projection plans. Semantic actor owns semantic decisions and follow-up import/projection requests.
- Binary/LWW support, watcher suppression, expected writes, and provenance/event tables are deferred. See `BINARY_LWW_DESIGN.md` for the current binary design sketch.

## Stable/Prior Tree

- `stable_tree` is a stored derived projection cache, updated transactionally whenever stable namespace/text state changes.
- Directories are implicit from file paths. v0 does not sync empty directories.
- `prior_tree` keeps tombstones inline with `deleted_at_ns`.
- `deleted_at_ns` means the time Semantic accepted/observed the delete, not necessarily the actual filesystem deletion time.
- Safe-save/replacement-window handling is deferred, but tombstones are shaped to support it later.
- If a path reappears after a tombstone, v0 treats it as a new file unless it can confidently match the tombstoned row by `file_key`.
- If `file_key` matches, reuse the object. If `file_key` is absent or differs, create a new object.

## Sync Startup Import

- `sync` always starts with a full scan before normal processing.
- Engine sends the scan to Semantic; Semantic compares scan evidence against both `prior_*` and `stable_*`.
- Scanning is an Engine phase, but not a Semantic epoch.
- `ApplyScan` returns `NextWork`. If import/projection work is needed, Semantic creates the epoch internally and returns `NextWork::Import(ImportEpochStarted)` or `NextWork::Project(ProjectionEpochStarted)`.
- `scan == prior`: no local import. If `prior != stable`, projection may still be needed.
- `scan != prior && scan == stable`: projection likely completed before `prior_*` was recorded; update `prior_*`, no outbox.
- `scan != prior && scan != stable`: local drift; Semantic returns an import plan with guarded reads for files whose contents are needed.
- New path with no prior entry: guarded read; if UTF-8, create object, update stable/prior namespace/tree/text state, and insert outbox in one transaction.
- Existing text path with changed fingerprint: guarded read, diff disk text against `prior_text_objects`, apply resulting CRDT update to `stable_text_objects`, update `prior_*`, and insert outbox.
- v0 uses full scans only. Partial/subtree scans are deferred.
- Missing path in a full scan is delete evidence. Semantic can commit namespace deletes/outbox without Engine file I/O follow-up.
- Deletes are committed immediately as tombstones rather than hard-deleting all prior evidence. This keeps enough information to reason about later same-path reappearance/safe-save patterns.
- Import action IDs are assigned by Semantic.
- Non-UTF-8 import results are import failures, not silent ignore/delete.
- Import and projection do not overlap in v0. Engine tracks a single phase: idle, scanning, importing, or projecting.
- Import completions carry an epoch/id. Semantic ignores stale completions from superseded import plans.
- `prior_text_objects` is the CRDT merge base for local imports. We do not do manual textual diff3; the generated CRDT update carries causality from the prior projected state.

## Engine Phase

- Engine stores active work in one phase enum so illegal combinations are unrepresentable.
- Shape:

```rust
enum EnginePhase {
    Idle,
    Scanning,
    Importing {
        epoch: ImportEpoch,
        queue: VecDeque<ImportAction>,
        in_flight: usize,
    },
    Projecting {
        epoch: ProjectionEngineEpoch,
        plan: VecDeque<ProjectionAction>,
        in_flight: BTreeMap<ProjectionActionId, ProjectionAction>,
        invalidated: bool,
    },
}
```

- v0 projection should run with `max_in_flight = 1`.
- The `Projecting` shape still supports multiple in-flight actions later, once we have a deterministic independence/admission policy.
- On projection guard failure or invalidation, Engine stops scheduling more projection actions, waits for in-flight actions to finish, reports results to Semantic, then requests a fresh plan.

## Import/Projection Epochs

- Import and projection epochs are cross-actor transaction-like scopes. Semantic opens the epoch, Engine executes filesystem side effects, Semantic records completions, then Semantic closes the epoch and returns next work.
- Engine owns operational sequencing: scan, schedule FS actions, track when FS actions finish, and decide when to ask Semantic to close an epoch.
- Semantic owns semantic truth and durability: expected action set, action completion accounting, stable/prior updates, outbox writes, and next-work decisions.
- Individual import/projection action completions can be fire-and-forget from Engine to Semantic.
- `CommitImportEpoch` and `CommitProjectionEpoch` are barriers. They must account for all expected action commits before returning next work.
- Import epoch start returns an `ImportEpoch` plus an `ImportPlan`. Projection epoch start returns a `ProjectionEpoch` plus a `ProjectionPlan`.
- Import epoch commit fails or asks for retry/rescan if expected imports are missing, failed, or stale. Successful commit returns `NextWork`.
- Projection epoch close can end cleanly or invalidated. Successful projection actions update `prior_*`; conflicts or invalidation cause Semantic to return fresh import/projection work.
- These epochs are not atomic DB+filesystem transactions. Filesystem side effects cannot be rolled back, so failure handling is rescan/retry/replan rather than rollback.
- `Idle` is only an Engine phase. Semantic responds with `NextWork::None` when no semantic work is needed.

```rust
enum NextWork {
    None,
    Import(ImportEpochStarted),
    Project(ProjectionEpochStarted),
}
```

```rust
SemanticRequest::StartImportEpoch { ... }
SemanticRequest::CommitImportAction { ... } // no reply
SemanticRequest::CommitImportEpoch { ... }  // barrier, returns NextWork

SemanticRequest::StartProjectionEpoch { ... }
SemanticRequest::CommitProjectionAction { ... } // no reply
SemanticRequest::CommitProjectionEpoch { ... }  // barrier, returns NextWork
```

## Projection Conflicts

- Projection write results distinguish success, pre-write conflict, and post-write conflict.

```rust
enum GuardedWriteResult {
    Written {
        new_fingerprint: FileFingerprint,
    },
    ConflictBeforeWrite {
        current_fingerprint: Option<FileFingerprint>,
    },
    ConflictAfterWrite {
        intended_fingerprint: FileFingerprint,
        current_fingerprint: Option<FileFingerprint>,
    },
}
```

- `ConflictBeforeWrite`: disk changed before any swap/write side effect. Semantic should invalidate the projection epoch and pivot to importing current disk evidence.
- `ConflictAfterWrite`: the guarded write performed the swap, but final stat did not match the intended fingerprint. Prior state for that path is unknown; Semantic should invalidate the projection epoch and force rescan/import for that path.
- `Written`: projected content reached disk. Semantic can update `prior_*` for that action.
- If stable semantic state changes during projection, Semantic emits `ProjectionChanged { generation }`.
- On `ProjectionChanged`, Engine marks the current projection invalidated, stops scheduling new projection actions, lets in-flight actions finish, commits finished action results, then commits the projection epoch. Semantic returns fresh `NextWork`.

## Namespace Conflict Resolution

- Namespace/path conflict resolution lives in Semantic materialization, not FsActor.
- Stable namespace may contain multiple active claims for the same logical path.
- v0 namespace claims use full relative file paths, not semantic directory parents. Directories are structural path prefixes only.
- Semantic materializes active claims into `stable_tree` deterministically so all daemons converge to the same projected disk state.
- For concurrent same-path creates tied to different object ids, one object wins the requested path and losers receive deterministic conflict paths.
- Winner selection uses a stable total order, e.g. lexicographic `(writer_id, object_id)`.
- Conflict paths preserve the directory prefix and include stable identity, e.g. `notes/a (Conflict <writer-short>).txt`; include an object-id suffix if needed to avoid secondary collisions.
- Conflict paths are derived projection names, not additional CRDT operations.
- v0 does not require an FsActor `move` operation. Projection can express conflict reshaping as guarded writes plus guarded deletes.
