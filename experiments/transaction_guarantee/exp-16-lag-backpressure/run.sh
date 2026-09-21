#!/usr/bin/env bash
# exp-16 — consumer lag, and backpressure against blocking, when a dependency goes down.
#
# A steady stream over three partitions. Fifteen seconds in, the dependency every record
# needs stops answering for twenty seconds, and a second consumer joins in the middle of
# it. Once with a handler that retries inline, holding its batch and the rebalance; once
# with one that pauses fetching and rewinds. The group's lag is sampled every second.
#
# Usage: make exp-16
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"

binary="$(mktemp -d)/exp16"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-16-lag-backpressure

# HANDLERS=block reruns one shape only: whether a stale commit turns into duplicates depends
# on which member the rewound partition goes to next, and repeating the run is how to see both.
for handler in ${HANDLERS:-block pause}; do
  make reset-topic TOPIC=exp16.payments
  log="$here/results/$handler-$stamp.log"
  {
    echo "exp-16 — a dependency outage with a join in the middle, handler: $handler"
    echo "date: $stamp"
    echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
    echo
    "$binary" -handler "$handler"
  } 2>&1 | tee "$log"
done

echo "written to results/block-$stamp.log and results/pause-$stamp.log"
