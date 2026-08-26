#!/usr/bin/env bash
# Run both usage examples for real: the Rust stdio client
# (gossipd/daemon/examples/pair_demo.rs) and the Elisp library
# consumer (examples/gossip-ping.el, driven by two batch Emacs).
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build -q --manifest-path gossipd/Cargo.toml
export GOSSIPD_BIN="$PWD/gossipd/target/debug/gossipd"

( cd gossipd && cargo run -q --example pair_demo ) | tail -1 | grep -qx "PAIR DEMO OK"
echo "rust example: PAIR DEMO OK"

# The tour dials by EndpointId with zero addresses, so it only passes
# if real mDNS multicast discovery works — needs a multicast-capable
# network interface.
( cd gossipd && cargo run -q --example iroh_tour ) | tail -1 | grep -qx "IROH TOUR OK"
echo "rust example: IROH TOUR OK (mDNS discovery exercised)"

export GOSSIP_DIR="$(mktemp -d)"
trap 'rm -rf "$GOSSIP_DIR"' EXIT

# Outbox for the file-transfer leg: a binary blob and a unicode name,
# sent concurrently via the gossip-dired-send example and byte-compared
# on the receiving side.
mkdir "$GOSSIP_DIR/outbox"
head -c 1048576 /dev/urandom > "$GOSSIP_DIR/outbox/blob.bin"
printf 'unicode filename survives\n' > "$GOSSIP_DIR/outbox/héllo wörld.txt"

run_role() {
  GOSSIP_ROLE="$1" emacs -Q --batch -L . -L examples -l tests/examples-smoke.el
}

run_role b &
B_PID=$!
run_role a
wait "$B_PID"

# The tor example runs against the mock (the tor surface's executable
# spec; the real daemon stubs tor until P2).
emacs -Q --batch -L . -L examples -l tests/examples-smoke-tor.el
echo "EXAMPLES SMOKE PASS"
