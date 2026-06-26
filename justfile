set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

install:
    cargo install --locked --path crates/client
    cargo install --locked --path crates/daemon
