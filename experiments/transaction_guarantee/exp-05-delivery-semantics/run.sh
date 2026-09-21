#!/usr/bin/env bash
# exp-05/06/07 — at-most-once, at-least-once and effectively-once, as numbers.
#
# One stand, three semantics. The only difference between them is where the offset is
# committed relative to the write, and whether the write claims an inbox row first.
#
# Usage: make exp-05
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
payments="${PAYMENTS:-1000}"
die_after="${DIE_AFTER:-500}"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"

# Built once, outside the run: `go run` would recompile on the first call of every phase.
binary="$(mktemp -d)/exp05"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-05-delivery-semantics

{
  echo "exp-05/06/07 — delivery semantics under a killed consumer"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo "payments: $payments, consumer killed after handling $die_after"
  echo

  for mode in at-most-once at-least-once inbox; do
    run_id="exp05-$stamp-$mode"
    exp05() { "$binary" -mode "$mode" -run-id "$run_id" -payments "$payments" "$@"; }

    # Each mode owns the log it reads: the group resumes from committed offsets, so a
    # topic carrying another mode's records would replay them into this one's count.
    make reset-topic TOPIC=exp05.payments >/dev/null

    exp05 -phase produce

    # SIGKILL is expected here and is the experiment, so the non-zero exit is not a failure.
    set +e
    exp05 -phase consume -die-after "$die_after" 2>&1 | grep -v '^$' || true
    set -e

    # The second process resumes from whatever the first managed to commit.
    exp05 -phase consume

    exp05 -phase verify
    echo
  done
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
