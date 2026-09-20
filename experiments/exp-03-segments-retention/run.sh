#!/usr/bin/env bash
# exp-03 — what the cleaner keeps: compaction on one topic, retention on another.
#
# Usage: make exp-03
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"

cd "$repo"
make exp-topics EXP="$(basename "$here")"

# This experiment reads the log itself, so it has to start from an empty one: records left
# by an earlier run would be counted as its own.
for topic in exp03.compact exp03.retention; do
  make reset-topic TOPIC="$topic"
done

{
  echo "exp-03 — segments, retention and compaction"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo
  go run ./experiments/exp-03-segments-retention \
    -keys "${KEYS:-50}" -updates "${UPDATES:-40}" \
    -tombstones "${TOMBSTONES:-10}" -records "${RECORDS:-2000}"
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
