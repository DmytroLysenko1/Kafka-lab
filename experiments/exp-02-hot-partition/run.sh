#!/usr/bin/env bash
# exp-02 — one merchant sends four events in five. Does adding consumers help?
#
# Usage: make exp-02
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"

cd "$repo"
make exp-topics EXP="$(basename "$here")"

{
  echo "exp-02 — hot partition: skewed keys against a consumer group"
  echo "date: $stamp"
  echo
  go run ./experiments/exp-02-hot-partition \
    -events "${EVENTS:-20000}" -hot-share "${HOT_SHARE:-80}" \
    -consumers "${CONSUMERS:-1,2,3,6,7}" -handler "${HANDLER:-200µs}"
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
