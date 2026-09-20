#!/usr/bin/env bash
# exp-01 — does the key decide the order a payment is handled in?
#
# Both runs produce the same events over the same six partitions; only the key differs.
# Usage: make exp-01
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"

cd "$repo"
make exp-topics EXP="$(basename "$here")"

# Each run owns the log it reads. The run id already keeps a rerun from counting the
# previous run's events, but it does not keep the previous run's records out of the
# partitions this one is measuring the handling order of.
for topic in exp01.keyless exp01.keyed; do
  make reset-topic TOPIC="$topic"
done

{
  echo "exp-01 — partition keys, ordering and parallelism"
  echo "date: $stamp"
  echo "events: ${EVENTS:-10000} over ${PAYMENTS:-100} payments, 6 partitions, franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo
  for mode in keyless keyed; do
    go run ./experiments/exp-01-partition-keys -mode "$mode" \
      -events "${EVENTS:-10000}" -payments "${PAYMENTS:-100}"
    echo
  done
} 2>&1 | tee "$log"   # stderr too: a failed run has to be visible in the file, not only on the terminal

echo "written to ${log#"$repo"/}"
