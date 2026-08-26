#!/usr/bin/env bash
# Byte-compile gossip.el and run every e2e scenario against the mock daemon.
set -euo pipefail
cd "$(dirname "$0")/.."
emacs -Q --batch -L . -f batch-byte-compile gossip.el
for t in tests/e2e-*.el; do
  echo "== $t"
  emacs -Q --batch -l "$t"
done
rm -f gossip.elc
echo "ALL TESTS PASS"
