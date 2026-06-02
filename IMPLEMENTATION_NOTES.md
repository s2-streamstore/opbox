# Implementation Notes

These notes are for the Dropbox-style tracer bullet. They identify streamfs code worth reusing and the places where opbox intentionally diverges.

## Streamfs Sources

- CRDT namespace: `/Users/sb/git/streamfs/crates/core/src/crdt/namespace.rs`
- CRDT text document: `/Users/sb/git/streamfs/crates/core/src/crdt/text_doc.rs`
- Shared-log codec: `/Users/sb/git/streamfs/crates/core/src/log/codec.rs`
- Shared-log reader: `/Users/sb/git/streamfs/crates/core/src/log/reader.rs`
- Shared-log writer: `/Users/sb/git/streamfs/crates/core/src/log/writer.rs`
- Storage actor/service/transaction patterns: `/Users/sb/git/streamfs/crates/core/src/storage/{actor.rs,service.rs,transaction.rs}`

## Port Mostly As-Is

- `text_doc.rs`: this is the best direct lift.
- Keep V2 Yrs encoding/decoding. V1 truncates client ids and should not be used.
- Keep the `TextObjectDoc` operations: full-state encode/decode, apply update, state vector, and `capture_text_change`.
- Use `prior_text_objects` as the merge base for local imports: diff `prior_text -> disk_text`, generate a Yjs update, apply that update to `stable_text_objects`, and insert the same update into outbox.
- Shared-log pointer packaging from `log/codec.rs` is reusable. For sim/test builds, keep a smaller inline threshold so pointer records get exercised.
- Shared-log reader/writer structure is reusable: reader starts at `daemon_state.stable_cursor`; writer appends outbox messages and reports durable outbox ranges.
- Stream creation should remain explicit and tolerate `resource_already_exists`.

## Adapt

- `namespace.rs` has the right CRDT idea but the wrong v0 shape.
- Keep the CRDT maps: `objects`, immutable `claims`, and `removed_claims`.
- opbox v0 claims should use full relative file paths, not semantic directory parents.
- Either remove `parent_object_id` from opbox's claim record or keep it internally fixed to a distinguished root object. Do not reintroduce real directory objects for v0.
- opbox v0 object kinds should be text-only, except for an internal root if the wrapper keeps one for compatibility.
- Namespace materialization should be much simpler than streamfs:

```text
active claims
-> group by full relative path
-> sort deterministically by writer/object/claim identity
-> winner keeps requested path
-> losers get deterministic conflict paths in the same directory prefix
-> write stable_tree
```

- Conflict names are derived projection names, not CRDT operations.
- Preserve path prefixes when rendering conflicts: `notes/a.txt` becomes `notes/a (conflict abc123).txt`.
- Streamfs has accounted/visible namespace docs. Do not map those names directly. opbox uses:
  - `stable_*`: stable semantic state from shared log plus local outbox mutations.
  - `prior_*`: last accepted/projected local disk baseline.
- Streamfs transaction/retry helpers are useful as a pattern, but opbox's Semantic service should implement the new epoch/`NextWork` flow, not streamfs StorageOps.

## Do Not Port For V0

- VFS/NFS/FUSE-facing code.
- `fs_nodes` and inode/handle-preservation logic.
- Directory object selection, cycle detection, and directory rename materialization.
- Binary LWW support and `BinaryObjectPut`.
- Publish debounce / `publish_due_at` / provisional namespace tracking.
- Migration machinery. opbox is still prototype schema-only.
- Streamfs's accounted/visible dirty namespace model. It was for hosted filesystem semantics, not opbox's stable/prior disk baseline.
- Replacement-window/safe-save handling. Keep tombstones shaped for it, but defer implementation.

## Tracer Bullet Target

- Schema should model `daemon_state`, `objects`, `stable_namespace`, `prior_namespace`, `stable_text_objects`, `prior_text_objects`, `stable_tree`, `prior_tree`, and `outbox`.
- Implement `ApplyScan -> NextWork`.
- `ApplyScan` can create an import/projection epoch internally and return `NextWork::{None, Import, Project}`.
- Import actions are assigned by Semantic.
- `CommitImportAction` is fire-and-forget. It records durable acceptance of one read/stat result.
- `CommitImportEpoch` is a barrier and returns `NextWork`.
- `CommitProjectionAction` is fire-and-forget. Success updates `prior_*`; conflicts invalidate the epoch.
- `CommitProjectionEpoch` is a barrier and returns `NextWork`.
- Engine owns phase sequencing; Semantic owns semantic truth, expected action sets, durability, and next-work decisions.

## Minimal Flow To Prove

1. Start `sync`; Engine enters `Scanning`.
2. FsActor returns a full scan.
3. Engine sends `ApplyScan`.
4. Semantic sees a new UTF-8 text file and returns `NextWork::Import`.
5. Engine runs guarded read import actions.
6. Semantic commits import actions by creating object/claim/text CRDT state, updating stable/prior tables, and inserting outbox messages.
7. Engine commits import epoch.
8. Semantic returns `NextWork::None` if `prior == stable`, or `NextWork::Project` if shared messages changed stable during import.
9. LogWriter sends outbox messages; LogReader can read the same messages back idempotently.

## Watch For

- A remote shared message applies to stable only. It should not directly mutate prior; projection does that after filesystem writes succeed.
- A local import updates stable and prior together because disk is being accepted as the new baseline.
- A projection success updates prior to match the projected stable state for that action.
- A projection conflict before write means disk changed before side effects. Pivot to import current disk evidence.
- A projection conflict after write means prior for that path is unknown. Force rescan/import.
- Idempotence comes from Yjs updates and namespace CRDT tombstones, not from deduping log records in application code.
