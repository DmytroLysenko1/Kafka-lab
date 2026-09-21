#!/usr/bin/env bash
# exp-17 — what linger, batch size and the codec cost and buy, under one fixed load.
#
# 32 cells, each offered the same records per second for the same time, each measured for
# what was acknowledged, how long the acknowledgement took, how many records shared a batch
# and how many bytes each record cost on the wire.
#
# Usage: make exp-17
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
rate="${RATE:-20000}"
duration="${DURATION:-10s}"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"
make reset-topic TOPIC=exp17.sweep

binary="$(mktemp -d)/exp17"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-17-batching-sweep

{
  echo "exp-17 — linger x batch size x codec under a fixed load"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo "offered: $rate records/s for $duration per cell, acks=all, 6 partitions, RF 3"
  echo "payload: generated payment JSON, seeded, identical for every cell"
  echo
  "$binary" -rate "$rate" -duration "$duration"
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
