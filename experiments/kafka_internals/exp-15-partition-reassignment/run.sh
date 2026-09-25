#!/usr/bin/env bash
# exp-15 — raising a replication factor on a live topic, with a throttle and without.
#
# Two cells over the same 60 MiB topic: move every partition onto a third broker while
# producers keep writing, once at full speed and once at a megabyte a second. The question
# is not whether the move works — it is what the producers waiting behind it pay.
#
# Usage: make exp-15
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
megabytes="${MEGABYTES:-60}"
window="${WINDOW:-120s}"
move_after="${MOVE_AFTER:-15s}"
throttle="${THROTTLE:-1048576}"

cd "$repo"

# A throttle left behind would slow every later experiment's replication and nobody would
# know why. verify removes it, but only if the run reaches verify.
clear_throttles() {
  docker exec kafka-lab-kafka3 /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
    --alter --entity-type topics --entity-name exp15.reassign \
    --delete-config leader.replication.throttled.replicas,follower.replication.throttled.replicas >/dev/null 2>&1 || true
  for id in 1 2 3; do
    docker exec "kafka-lab-kafka$id" /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
      --alter --entity-type brokers --entity-name "$id" \
      --delete-config leader.replication.throttled.rate,follower.replication.throttled.rate >/dev/null 2>&1 || true
  done
}
trap clear_throttles EXIT

binary="$(mktemp -d)/exp-15"
go build -o "$binary" ./experiments/kafka_internals/exp-15-partition-reassignment

cell() {
  local name="$1" throttle_bytes="$2"

  # Dropped and recreated rather than re-applied. Once a cell has raised the replication
  # factor to 3, topicctl refuses to put it back — "Replication in topic config (2) is not
  # equal to observed max ISR (3); this cannot be resolved by topicctl" — which is the
  # experiment's own point arriving a run early: a replication factor is not a line in a
  # YAML, it is a migration. An empty log matters too: a topic still holding the previous
  # cell's records would copy twice what this cell measured.
  docker exec kafka-lab-kafka3 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --topic exp15.reassign >/dev/null 2>&1 || true
  until ! docker exec kafka-lab-kafka3 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --list 2>/dev/null | grep -qx exp15.reassign; do sleep 1; done
  make exp-topics EXP="${here#"$repo"/experiments/}"
  clear_throttles

  echo "== $name =="
  "$binary" -phase preload -megabytes "$megabytes"
  "$binary" -phase measure -window "$window" -move-after "$move_after" -throttle "$throttle_bytes"
  echo
}

{
  echo "exp-15 — raising a replication factor under load, throttled and not"
  echo "date: $stamp"
  echo "topic: 3 partitions starting at RF 2, preloaded with $megabytes MiB; producers write one 1 KiB record every 10 ms"
  echo

  cell "cell A: no throttle" 0
  cell "cell B: throttled to $((throttle / 1024)) KiB/s" "$throttle"
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
