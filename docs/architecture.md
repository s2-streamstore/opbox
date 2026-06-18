# `opbox` architecture

## Concepts

Let's get these out of the way.

- CRDTs
  - [Conflict-free replicate data type](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)
  - Sync works in `opbox` by distributing messages representing operations (ops) on CRDT data structures.
  - Our ops represent changes (to text files, or to the namespace representing what files exist); because they are CRDT operations, they can be applied in any order, any amount of times, and our workspaces should still meaningfully converge to a single stable representation.
- Workspace
  - Our equivalent of a "repo" in a git analogy. This is the thing being synced. You can have a workspace synced with a directory locally.
- Shared log
  - How we transmit CRDT ops and make them available to all daemons syncing with the same workspace
  - 
- Semantic engine
  - This owns all of the logic around CRDTs, tracking of files that exist locally, changes which exist remotely, etc.
  - This is the daemon-specific durability layer. Each local workspace clone uses a Turso (`sqlite`-style) database, stored in `$LOCAL_WORKSPACE/.opbox/storage.db`.
  - Among the types of data tracked by the semantic engine are:
    - CRDT data structures
    - File trees
      - Information about files on disk, their fingerprints, and what CRDT objects relate
    - The CRDT op message outbox
- Main "engine" actor
  - This is the central coordinator of all actions taken by the daemon
  - The engine maintains a state machine (see [`EnginePhase`](https://github.com/s2-streamstore/opbox/blob/02f48588ac5b5715c24498ed4390c048f31c5081/crates/core/src/engine/actor.rs#L171-L192)); three of these states are useful to talk about here:
    - Scanning
      - The daemon is crawling the local workspace directory; this produces a tree snapshot
        - The snapshot can then be used to determine things like:
          - If we need to apply remote changes to a local file, or create a new file entirely (projection)
          - If a local file has changed since we last scanned it, and therefore we need to capture those changes (import)
      - Trees contain fingerprints of files, not file content itself
        - Scans only stat files, they don't read
      - Other sources of info about fs-level changes (e.g. from `inotify` or `fsevents`) are hints that can trigger scans (either of the entire workspace, or subsets of it)
        - Complete scans happen on a cron schedule, but most changes should be represented via a notification stream, so we can still react in real time to most file writes or creations, etc.
    - Projecting
      - When remote changes are received by the semantic engine, typically they result in a projection plan being created
      - A projection plan is a materialization of the desired state of local files (what they need to look like to reflect the current state of CRDTs); this is then operationalized into filesystem actions, like writing or deleting files
      - Projection can be preempted, if newer updates mean that the plan is stale; but this is only an optimization.
      - Projections also can be invalidated while in progress -- for instance, if a file on disk changed while we tried to write it
        - Generally, this means that the file was edited by the local user concurrent with a remote edit being applied to the same file
        - We take care to catch these situations (files moving while we are attempting to write) by guarding our writes -- but we can't do this perfectly
    - Importing
      - This is the process of reading the content of files, discovered from a scan, and updating the semantic engine's CRDTs, and tree data, respectively
      - Imports can be triggered after a scan reveals that a file has changed, been created, deleted; it can also be triggered when a projection is invalidated because, for instance, a guarded write failed (indicating the file changed since we last saw it)


## Questions

- Why a db?
- Why Turso?
- 

## Sharp edges