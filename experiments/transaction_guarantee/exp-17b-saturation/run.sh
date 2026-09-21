#!/usr/bin/env bash
# exp-17b — the exp-17 producer with nothing holding it back: throughput and codec CPU.
#
# Eight cells — linger 0 and franz-go's 10 ms, each codec, the default batch ceiling — each
# producing flat out for the same time from the same pre-generated payment events. The
# program is exp-17's, run with -saturate, on exp-17's topic.
#
# Usage: make exp-17b
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
duration="${DURATION:-10s}"

cd "$repo"
make exp-topics EXP=transaction_guarantee/exp-17-batching-sweep
make reset-topic TOPIC=exp17.sweep

binary="$(mktemp -d)/exp17"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-17-batching-sweep

{
  echo "exp-17b — linger x codec with the producer flat out"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo "load: as fast as the client's 10 000-record buffer allows, for $duration per cell, acks=all, 6 partitions, RF 3"
  echo "payload: 200 000 generated payment events, seeded, generated before the sweep and cycled"
  echo "host: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(getconf _NPROCESSORS_ONLN) CPUs; brokers share it"
  echo
  "$binary" -saturate -duration "$duration"
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
