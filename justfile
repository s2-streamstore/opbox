set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

install:
    cargo install --locked --path crates/client
    cargo install --locked --path crates/daemon

kill-all:
    pkill -TERM -u "$(id -u)" -f '(^|/)opbox-daemon( |$)' 2>/dev/null || true
