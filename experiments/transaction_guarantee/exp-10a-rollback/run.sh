#!/usr/bin/env bash
# exp-10a — a crash mid-transaction aborts the output, and the offsets were never committed.
#
# 1000 payments through a read-process-write loop inside Kafka transactions, the processor
# killed mid-transaction, a second one resuming, and the result read back.
#
# Usage: make exp-10a
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
payments="${PAYMENTS:-1000}"
die_after="${DIE_AFTER:-525}"
run_id="exp-10a-$stamp"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"
for topic in exp10a.input exp10a.output; do make reset-topic TOPIC="$topic"; done

binary="$(mktemp -d)/exp10a"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-10a-rollback
run() { "$binary" -run-id "$run_id" -payments "$payments" "$@"; }

{
  echo "exp-10a — a crash mid-transaction aborts the output, and the offsets were never committed"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo "payments: $payments, processor killed mid-transaction at the first batch that takes it to $die_after handled"
  echo

  run -phase seed

  # SIGKILL is expected here and is the experiment, so the non-zero exit is not a failure.
  set +e
  run -phase process -die-after "$die_after"
  set -e

  # The replacement presents the same transactional id, which is how the broker learns the
  # old transaction is dead and aborts it; it then resumes from the last committed offsets.
  run -phase process

  run -phase verify
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
