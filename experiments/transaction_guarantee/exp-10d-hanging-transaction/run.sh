#!/usr/bin/env bash
# exp-10d — a transaction left open, and everyone reading read_committed behind it.
#
# One producer opens a transaction and never ends it; another writes after it with no
# transaction at all. The run times how long each isolation level takes to see the second
# producer's records.
#
# Usage: make exp-10d
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
records="${RECORDS:-100}"
txn_timeout="${TRANSACTION_TIMEOUT:-20s}"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"
make reset-topic TOPIC=exp10d.stream

binary="$(mktemp -d)/exp10d"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-10d-hanging-transaction

{
  echo "exp-10d — a transaction left open"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo
  "$binary" -run-id "exp-10d-$stamp" -records "$records" -transaction-timeout "$txn_timeout"
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
