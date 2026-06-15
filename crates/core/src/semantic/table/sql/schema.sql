-- Append-only synced object identities.
-- v0 syncs text files only; binary/LWW support is deferred.
CREATE TABLE IF NOT EXISTS objects (
    object_id          BLOB PRIMARY KEY,
    object_kind        TEXT NOT NULL CHECK (object_kind = 'text'),
    creator_writer_id  BLOB NOT NULL,
    created_at_ns      INTEGER NOT NULL
);

-- Stable namespace CRDT: shared log applied up to stable_cursor plus local outbox mutations.
CREATE TABLE IF NOT EXISTS stable_namespace (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    doc_blob      BLOB NOT NULL,
    updated_at_ns INTEGER NOT NULL
);

-- Prior namespace CRDT: last accepted/projected local disk namespace baseline.
CREATE TABLE IF NOT EXISTS prior_namespace (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    doc_blob       BLOB NOT NULL,
    accepted_at_ns INTEGER NOT NULL
);

-- Stable text CRDT state, keyed by object identity.
CREATE TABLE IF NOT EXISTS stable_text_objects (
    object_id     BLOB PRIMARY KEY,
    doc_blob      BLOB NOT NULL,
    updated_at_ns INTEGER NOT NULL
);

-- Prior text CRDT state used as the merge base for local imports.
CREATE TABLE IF NOT EXISTS prior_text_objects (
    object_id      BLOB PRIMARY KEY,
    doc_blob       BLOB NOT NULL,
    accepted_at_ns INTEGER NOT NULL
);

-- Stable desired filesystem projection, transactionally derived from stable namespace/text state.
-- `path` is the rendered disk path. For conflict-rendered rows, `claimed_path` is the
-- original namespace path requested by the claim.
CREATE TABLE IF NOT EXISTS stable_tree (
    path          TEXT PRIMARY KEY,
    claimed_path  TEXT NOT NULL,
    claim_id      BLOB NOT NULL UNIQUE,
    object_id     BLOB NOT NULL REFERENCES objects(object_id),
    updated_at_ns INTEGER NOT NULL,

    CHECK (path <> ''),
    CHECK (claimed_path <> '')
);

CREATE INDEX IF NOT EXISTS stable_tree_object_id_idx
    ON stable_tree(object_id);

-- Last accepted/projected local disk baseline.
-- Tombstones remain inline so later same-path reappearance can be evaluated against prior
-- file identity. `deleted_at_ns` is when Semantic accepted/observed the delete.
--
-- Fingerprint invariant:
-- - `hash IS NULL` means the baseline is stat-only.
-- - `hash IS NOT NULL` means the hash was sampled while the file had the exact
--   same file_key/size_bytes/mtime_ns stored on this row.
-- - Any update that only refreshes stat evidence must clear hash.
CREATE TABLE IF NOT EXISTS prior_tree (
    path           TEXT PRIMARY KEY,
    claimed_path   TEXT NOT NULL,
    claim_id       BLOB NOT NULL,
    object_id      BLOB NOT NULL REFERENCES objects(object_id),
    file_key       TEXT NOT NULL,
    size_bytes     INTEGER NOT NULL CHECK (size_bytes >= 0),
    mtime_ns       INTEGER NOT NULL,
    hash           BLOB,
    accepted_at_ns INTEGER NOT NULL,
    deleted_at_ns  INTEGER,

    CHECK (path <> ''),
    CHECK (claimed_path <> ''),
    CHECK (deleted_at_ns IS NULL OR deleted_at_ns >= accepted_at_ns)
);

CREATE INDEX IF NOT EXISTS prior_tree_object_id_idx
    ON prior_tree(object_id);

CREATE INDEX IF NOT EXISTS prior_tree_claim_id_idx
    ON prior_tree(claim_id);

CREATE INDEX IF NOT EXISTS prior_tree_deleted_at_ns_idx
    ON prior_tree(deleted_at_ns);

CREATE INDEX IF NOT EXISTS prior_tree_file_key_deleted_at_ns_idx
    ON prior_tree(file_key, deleted_at_ns);

-- Scratch evidence collected during an import epoch.
-- These rows are not crash-recoverable semantic state; startup/import setup may
-- clear them before beginning new work.
CREATE TABLE IF NOT EXISTS import_staged_files (
    import_epoch   INTEGER NOT NULL CHECK (import_epoch >= 0),
    action_seq     INTEGER NOT NULL CHECK (action_seq >= 0),
    path           TEXT NOT NULL,
    object_id      BLOB NOT NULL,
    claim_id       BLOB NOT NULL,
    stage_kind     TEXT NOT NULL CHECK (stage_kind IN ('new', 'resurrect', 'update')),
    file_key       TEXT NOT NULL,
    size_bytes     INTEGER NOT NULL CHECK (size_bytes >= 0),
    mtime_ns       INTEGER NOT NULL,
    hash           BLOB NOT NULL,
    prior_doc_blob BLOB NOT NULL,
    text_update    BLOB NOT NULL,
    staged_at_ns   INTEGER NOT NULL,

    PRIMARY KEY (import_epoch, action_seq),
    UNIQUE (import_epoch, path),
    UNIQUE (import_epoch, object_id),
    UNIQUE (import_epoch, claim_id),

    CHECK (path <> ''),
    CHECK (length(object_id) > 0),
    CHECK (length(claim_id) > 0)
);

-- Durable projection write intents: one row per (path, target content) the
-- projection layer planned to write. They let a later import distinguish the
-- daemon's own writes — orphaned by an invalidated epoch or a crash before the
-- epoch commit — from genuine user edits, which would otherwise be re-imported
-- as new CRDT ops and duplicate text on every replica.
--
-- Lifecycle: upserted when a projection plan is built; deleted when a
-- projection epoch commits a write or delete for the path, or when an import
-- decision consumes the path. Multiple generations per path may be
-- outstanding. NOT scratch: rows must survive restart.
CREATE TABLE IF NOT EXISTS projection_write_intents (
    path            TEXT NOT NULL,
    target_hash     BLOB NOT NULL,
    object_id       BLOB NOT NULL,
    target_doc_blob BLOB NOT NULL,
    created_at_ns   INTEGER NOT NULL,

    PRIMARY KEY (path, target_hash),

    CHECK (path <> ''),
    CHECK (length(target_hash) > 0),
    CHECK (length(object_id) > 0)
);

-- Post-write evidence for projection_write_intents. A planned write intent is
-- only trusted as self-echo evidence once FsActor has reported the exact
-- fingerprint that landed on disk.
CREATE TABLE IF NOT EXISTS projection_applied_write_intents (
    path            TEXT NOT NULL,
    target_hash     BLOB NOT NULL,
    file_key        TEXT NOT NULL,
    size_bytes      INTEGER NOT NULL CHECK (size_bytes >= 0),
    mtime_ns        INTEGER NOT NULL,

    PRIMARY KEY (path, target_hash),
    FOREIGN KEY (path, target_hash)
        REFERENCES projection_write_intents(path, target_hash)
        ON DELETE CASCADE,

    CHECK (path <> ''),
    CHECK (length(target_hash) > 0),
    CHECK (file_key <> '')
);

-- Durable outbox: shared-log records not yet acknowledged by the log writer.
-- payload is Yjs update bytes for namespace/text records.
CREATE TABLE IF NOT EXISTS outbox (
    outbox_id     INTEGER PRIMARY KEY CHECK (outbox_id >= 0),
    record_kind   TEXT NOT NULL CHECK (record_kind IN ('namespace_update', 'text_update')),
    object_id     BLOB REFERENCES objects(object_id),
    payload       BLOB NOT NULL,
    created_at_ns INTEGER NOT NULL,
    inflight      INTEGER NOT NULL CHECK (inflight IN (0, 1)),

    CHECK (
        (record_kind = 'namespace_update' AND object_id IS NULL) OR
        (record_kind = 'text_update' AND object_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS outbox_object_id_idx
    ON outbox(object_id);

-- Daemon-wide durable state.
CREATE TABLE IF NOT EXISTS daemon_state (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    workspace_id   TEXT NOT NULL,
    s2_basin       TEXT NOT NULL,
    writer_id      BLOB NOT NULL,
    stable_cursor  INTEGER NOT NULL CHECK (stable_cursor >= 0),
    next_outbox_id INTEGER NOT NULL CHECK (next_outbox_id >= 0)
);
