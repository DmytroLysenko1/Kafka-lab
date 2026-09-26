#!/usr/bin/env bash
# exp-02 — one merchant sends four events in five. Does adding consumers help?
#
# Usage: make exp-02
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"

runs="${RUNS:-3}"

{
  echo "exp-02 — hot partition: skewed keys against a consumer group, and the same drain with even keys"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo "runs per cell: $runs"
  for run in $(seq "$runs"); do
    # Every drain reads the topic from the start, so records left by an earlier run are
    # fetched and decoded before this run's begin: the drain times would grow run by run.
    make reset-topic TOPIC=exp02.hotkey >/dev/null
    make reset-topic TOPIC=exp02.uniform >/dev/null
    echo
    echo "== run $run, skewed cell: one merchant sends ${HOT_SHARE:-80}% of the events =="
    go run ./experiments/kafka_internals/exp-02-hot-partition -topic exp02.hotkey \
      -events "${EVENTS:-20000}" -hot-share "${HOT_SHARE:-80}" \
      -consumers "${CONSUMERS:-1,2,3,6,7}" -handler "${HANDLER:-200µs}"
    echo
    echo "== run $run, control cell: the same events over a thousand merchants, none hot =="
    go run ./experiments/kafka_internals/exp-02-hot-partition -topic exp02.uniform \
      -events "${EVENTS:-20000}" -hot-share 0 -merchants 1000 \
      -consumers "${CONSUMERS:-1,2,3,6,7}" -handler "${HANDLER:-200µs}"
  done
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
