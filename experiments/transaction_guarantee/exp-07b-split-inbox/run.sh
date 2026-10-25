#!/usr/bin/env bash
# exp-07b — the inbox split: the claim and the write in two transactions.
#
# 1000 payments, a consumer killed between a committed claim and the write it guards, and
# the counts read back out of Postgres. Everything else is exp-07's.
#
# Usage: make exp-07b
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
payments="${PAYMENTS:-1000}"
die_after="${DIE_AFTER:-500}"
run_id="exp-07b-$stamp"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"

# The experiment owns the log it reads: the group resumes from committed offsets, so
# records left by an earlier run would be replayed into this one's count.
make reset-topic TOPIC=exp07b.payments

# Built once, outside the run: `go run` would recompile on the first call of every phase.
binary="$(mktemp -d)/exp-07b"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-07b-split-inbox
run() { "$binary" -run-id "$run_id" -payments "$payments" "$@"; }

{
  echo "exp-07b — the inbox split: the claim and the write in two transactions"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo "payments: $payments, consumer killed after handling $die_after"
  echo

  run -phase produce

  # SIGKILL is expected here and is the experiment, so the non-zero exit is not a failure.
  set +e
  run -phase consume -die-after "$die_after"
  set -e

  # The second process resumes from whatever the first managed to commit.
  run -phase consume

  run -phase verify
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
