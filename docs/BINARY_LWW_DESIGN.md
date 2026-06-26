# Binary LWW Design

Status: deferred. v0 continues to sync UTF-8 text files only.

## Goal

- Support occasional binary files without compromising the CRDT/shared-log convergence model.
- Keep binary support deterministic, idempotent, commutative, and associative at the semantic merge layer.
- Avoid making Turso carry large blobs through SQL transactions, pages, caches, and outbox scans.
- Treat binary sync as a best-effort convenience for mostly-text workspaces, not as a high-throughput binary file sharing system.

## LWW Rule

Binary objects use last-writer-wins rather than a CRDT document.

The proposed total order for a binary version is:

```text
(wall_time_ns, writer_id, content_hash)
```

- `wall_time_ns` is client-reported real-world time.
- `writer_id` is the daemon writer id.
- `content_hash` is `xxh3_64(blob)` and is only a deterministic tie-breaker.
- Timestamp spoofing is accepted for this prototype.
- A same-writer same-timestamp collision should be vanishingly rare. If it happens, `content_hash` gives a deterministic answer, not necessarily the user's true intended last write.

## Blob Storage

Do not store binary blob bytes in Turso.

Store binary bytes in an immutable sidecar blob store under the checkout, for example:

```text
.opbox/blobs/<blob-ref>
```

Turso should store metadata only:

```text
stable_binary_objects:
  object_id
  wall_time_ns
  writer_id
  content_hash
  size_bytes
  blob_ref
  updated_at_ns

prior_binary_objects:
  object_id
  wall_time_ns
  writer_id
  content_hash
  size_bytes
  accepted_at_ns
```

`prior_binary_objects` may not need `blob_ref`: binary imports do not diff against prior bytes. The prior baseline mainly needs version metadata plus `prior_tree`'s filesystem fingerprint. This is an open implementation detail.

## Visible Files

The visible checkout file is not the stable source of truth for binary bytes.

Projection should materialize from the sidecar blob store into a temp file, then use the existing guarded swap flow.

Hardlinks and symlinks are rejected:

- Hardlinks let in-place editor writes mutate the immutable `.opbox/blobs` content.
- Symlinks expose implementation paths and can make editor atomic-save behavior stranger.

Preferred materialization:

- macOS/APFS: `clonefile` / `fclonefileat`.
- Linux reflink filesystems: `ioctl(FICLONE)`.
- Fallback: regular copy.

This gives two logical files while usually sharing physical blocks until one side changes.

## Shared Log

`BinaryObjectPut` messages still carry the binary bytes through S2.

The existing S2 pointer packaging handles large records by storing bytes in object streams and appending a pointer record to the main ops stream. Binary support should reuse that path.

Suggested message metadata:

```text
message_kind = binary_put
object_id = raw bytes
writer_id = raw bytes
wall_time_ns = ascii decimal
content_hash = 8 raw bytes or ascii decimal
size_bytes = ascii decimal
```

On apply:

- Verify the blob matches `size_bytes` and `content_hash`.
- Compare `(wall_time_ns, writer_id, content_hash)` against the current stable binary version.
- If the incoming version is older or equal, ignore it except for stable-cursor advancement.
- If the incoming version is newer, write the blob to the sidecar store, then commit Turso metadata.

## Outbox

The outbox should not store binary blob bytes directly.

Likely shape:

```text
outbox:
  outbox_id
  record_kind = binary_put
  object_id
  wall_time_ns
  writer_id
  content_hash
  size_bytes
  blob_ref
  created_at_ns
  inflight
```

Open question: which actor resolves `blob_ref -> Bytes` for S2 append?

- Option A: Semantic `ReadOutbox` loads the blob and returns a normal `SharedMessage::BinaryObjectPut`.
- Option B: LogWriter receives binary metadata plus `blob_ref` and owns loading the blob.

Option A keeps LogWriter close to the current message API. Option B keeps large blob reads out of Semantic transactions and may be cleaner if `ReadOutbox` is treated as queue metadata only.

## Import Flow

For a local binary file change:

1. Guarded read returns bytes and a content fingerprint.
2. If bytes are non-UTF-8, classify as binary.
3. Write bytes to the sidecar blob store first.
4. Commit Turso metadata and outbox row referencing `blob_ref`.
5. Update `prior_tree` to the accepted filesystem fingerprint.

If sidecar write succeeds but the DB transaction fails, the blob is orphaned. That is acceptable for v0; GC can clean it later.

If DB commit succeeds but blob write was not durable, semantic state can reference missing bytes. Avoid this by ordering blob write before DB commit.

## Projection Flow

For a stable binary object whose prior baseline differs:

1. Resolve stable `blob_ref`.
2. Materialize it to a daemon-owned temp path by COW clone/reflink or copy.
3. Guarded swap into the visible path.
4. On success, update `prior_binary_objects` metadata and `prior_tree`.
5. On guard conflict, invalidate projection and pivot to import/rescan like text.

## Object Kind Transitions

Open question: what happens when a path changes between text and binary?

Reasonable v0 rule:

- Same path, same kind: update/resurrect existing object when safe.
- Same path, different kind: treat as object replacement by removing the old claim and creating a new object/claim.

This avoids trying to reinterpret a text CRDT object as binary LWW state or vice versa.

## GC

Blob GC is deferred.

Eventually, a blob is live if referenced by:

- `stable_binary_objects`
- binary outbox rows
- any prior/projection state that keeps a `blob_ref`

For v0, never deleting sidecar blobs is acceptable.

## Open Questions

- Should `prior_binary_objects` store `blob_ref`, or only version metadata?
- Should outbox blob loading live in Semantic or LogWriter?
- Should `content_hash` be stored as raw 8 bytes or ASCII decimal in S2 headers and Turso?
- Should blob addressing use only `(size_bytes, xxh3)` or include a stronger hash later?
- How aggressive should type replacement be for text-to-binary and binary-to-text path changes?
- When do we add clone/reflink materialization versus starting with regular copy only?
