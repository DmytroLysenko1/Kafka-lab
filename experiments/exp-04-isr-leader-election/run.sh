#!/usr/bin/env bash
# exp-04 — ISR, leader election and recovery: kill the broker leading a partition and watch
# what the cluster does by itself, and what it refuses to do without being asked.
#
# Usage: make exp-04
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
records="${RECORDS:-300}"

# The kill has to be timed from the moment it happened, not from the moment the next
# process starts, and BSD date has no milliseconds.
now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }

cd "$repo"
make exp-topics EXP="$(basename "$here")"

# A fresh topic starts with leadership on the preferred replicas, which is what the
# baseline is supposed to be a baseline of.
make reset-topic TOPIC=exp04.isr

# Built once, outside the measured window: `go run` compiles on the first call, and on a
# cold build cache that compile would land inside the interval being timed.
binary="$(mktemp -d)/exp04"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/exp-04-isr-leader-election
exp04() { "$binary" -records "$records" "$@"; }

{
  echo "exp-04 — ISR, leader election and recovery"
  echo "date: $stamp"
  echo

  baseline="$(exp04 -phase baseline)"
  printf '%s\n\n' "$baseline"

  victim="$(printf '%s\n' "$baseline" | awk '/^LEADER_P0/ {print $2}')"
  case "$victim" in
    1|2|3) ;;
    *) echo "exp-04: could not read the leader of partition 0 from the baseline"; exit 1 ;;
  esac

  # A phase between the kill and the restart can fail — a deadline, a client error — and
  # `set -e` would end the run with the broker still dead, leaving every later experiment
  # measuring a degraded stand. docker start on a running container is a no-op, so this is
  # safe to arm unconditionally.
  trap 'docker start "kafka-lab-kafka$victim" >/dev/null 2>&1 || true' EXIT

  # kill, not stop: SIGTERM gives Kafka a controlled shutdown, which hands leadership over
  # politely and measures the good case. A crash is the case worth measuring.
  echo "killing kafka$victim, the broker leading partition 0"
  docker kill "kafka-lab-kafka$victim" >/dev/null
  killed="$(now_ms)"
  echo

  exp04 -phase degraded -since "$killed"
  echo

  echo "starting kafka$victim again"
  docker start "kafka-lab-kafka$victim" >/dev/null
  restarted="$(now_ms)"
  echo

  exp04 -phase recovered -since "$restarted"
  echo

  # The cluster will not do this on its own: auto.leader.rebalance.enable is off, and this
  # is the step an operator has to run after every broker restart.
  echo "asking for a preferred leader election"
  make elect-preferred >/dev/null
  elected="$(now_ms)"
  echo

  exp04 -phase elected -since "$elected"
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
