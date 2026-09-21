#!/usr/bin/env bash
# exp-06b — franz-go's default autocommit: the timer commits what the previous poll returned.
#
# 1000 payments, a consumer killed mid-batch, and the counts read back out of Postgres.
# The same stand as exp-05…07, with the manual commit replaced by franz-go's autocommit.
#
# Usage: make exp-06b
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
payments="${PAYMENTS:-1000}"
die_after="${DIE_AFTER:-500}"
run_id="exp-06b-$stamp"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"

# The experiment owns the log it reads: the group resumes from committed offsets, so
# records left by an earlier run would be replayed into this one's count.
make reset-topic TOPIC=exp06b.payments

# Built once, outside the run: `go run` would recompile on the first call of every phase.
binary="$(mktemp -d)/exp-06b"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-06b-autocommit-default
run() { "$binary" -run-id "$run_id" -payments "$payments" "$@"; }

{
  echo "exp-06b — franz-go's default autocommit: the timer commits what the previous poll returned"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo "payments: $payments, consumer killed at the first record past $die_after handled that follows an autocommit inside its batch, with records of the batch still unwritten"
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
