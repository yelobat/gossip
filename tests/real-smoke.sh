#!/usr/bin/env bash
# Two-Emacs smoke against the real Rust gossipd (not the mock).
# Builds the daemon, then runs two batch Emacs instances that load
# gossip.el, exchange tickets, and chat both ways.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build -q --manifest-path gossipd/Cargo.toml
export GOSSIPD_BIN="$PWD/gossipd/target/debug/gossipd"
export GOSSIP_DIR="$(mktemp -d)"
trap 'rm -rf "$GOSSIP_DIR"' EXIT

run_role() {
  GOSSIP_ROLE="$1" emacs -Q --batch -L . -l tests/real-smoke.el
}

run_role b &
B_PID=$!
run_role a
wait "$B_PID"
echo "REAL-DAEMON SMOKE PASS"
