# opbox-sim

Deterministic simulation testing (DST) for the opbox sync core.

Inspired by aspects of the s2.dev DST setup [detailed here](https://s2.dev/blog/dst).

## Setup

Each run boots the whole distributed system inside one simulated process:

- Two opbox daemons run on [turmoil](https://github.com/tokio-rs/turmoil)'s virtual network, with simulated time and asymmetric link latencies to the log.
- The shared log is a real [s2-lite](https://github.com/s2-streamstore/s2#s2-lite) server backed by an in-memory object store — real S2 semantics rather than a mock.
- Daemon filesystems are in-memory, optionally wrapped in fault injection (`--failure-rate`).
- All nondeterminism — network, time, scheduling, filesystem faults, workload timing — derives from a single `--seed`, so any failing run replays exactly.

A controller drives the chosen workload and polls for convergence (bounded by `--max-steps`). At the end, runs assert that the daemons' filesystems and semantic state converged, along with workload-specific expectations.

## Running

Run from the repo root (`--cfg tokio_unstable` is set by `.cargo/config.toml`):

```bash
# one run of the default workload, random seed
cargo run --release -p opbox-sim -- single

# one workload with a pinned seed, full logs
cargo run --release -p opbox-sim -- single same-file-edits --seed 12345

# determinism check: run the same seed twice and line-diff the two runs' output
cargo run --release -p opbox-sim -- meta same-file-edits --seed 12345

# one workload across many seeds, in parallel
cargo run --release -p opbox-sim -- parallel offline-reconnect-merge --trials 100

# everything: all workloads, 25 seeds each, with a summary at the end
cargo run --release -p opbox-sim -- sweep
```

`sim single --help` lists all workloads: conflict storms, offline and
partition scenarios, packet loss, filesystem fault injection, safe-save
patterns, ignore handling, `wrong-cipher-clone`, `clone-clobber`, and more.
