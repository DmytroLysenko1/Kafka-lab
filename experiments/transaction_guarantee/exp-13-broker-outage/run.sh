#!/usr/bin/env bash
# exp-13 — payments taken through the real API while brokers die underneath the relay.
#
# Two cells over the same load. One broker down is a leader election: the cluster still has
# two replicas in sync, which is what min.insync.replicas=2 asks for. Two brokers down is
# below it, and the publisher is refused for the whole outage — which is the case the
# outbox exists for.
#
# Usage: make exp-13
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
rate="${RATE:-10}"
window="${WINDOW:-90s}"
kill_after="${KILL_AFTER:-20s}"
down_for="${DOWN_FOR:-30s}"

cd "$repo"

settled() {
  for id in 1 2 3; do
    out="$(docker exec "kafka-lab-kafka$id" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --describe --under-replicated-partitions 2>/dev/null)" || continue
    [ -z "$out" ] && return 0
    return 1
  done
  return 1
}

services=""
restore() {
  [ -n "$services" ] && kill $services 2>/dev/null || true
  for id in 1 2 3; do docker start "kafka-lab-kafka$id" >/dev/null 2>&1 || true; done
  for _ in $(seq 120); do settled && break; sleep 1; done
  settled || echo "exp-13: the cluster was still under-replicated after two minutes; run make elect-preferred by hand" >&2
  make elect-preferred >/dev/null 2>&1 || true
  return 0
}
trap restore EXIT

make exp-topics EXP="${here#"$repo"/experiments/}"
make reset-topic TOPIC=exp13.payments
make migrate

binaries="$(mktemp -d)"
go build -o "$binaries/exp-13" ./experiments/transaction_guarantee/exp-13-broker-outage
go build -o "$binaries/api" ./cmd/payments-api
go build -o "$binaries/relay" ./cmd/outbox-relay

cell() {
  local name="$1" victims="$2" merchant="$3"

  # Each cell starts from an empty outbox and its own merchant: a backlog left by the cell
  # before it would be counted as this one's.
  docker exec -i kafka-lab-postgres psql -U lab -d lab -q -c \
    "TRUNCATE outbox; DELETE FROM payments WHERE merchant_id = '$merchant';"

  PAYMENTS_API_KEY=lab-key "$binaries/api" >/dev/null 2>&1 &
  local api=$!
  OUTBOX_TOPIC=exp13.payments OUTBOX_INTERVAL=500ms "$binaries/relay" >/dev/null 2>&1 &
  local relay=$!
  services="$api $relay"
  sleep 4

  echo "== $name =="
  "$binaries/exp-13" -merchant "$merchant" -rate "$rate" -window "$window" \
    -kill-after "$kill_after" -down-for "$down_for" -victims "$victims"

  kill $api $relay 2>/dev/null || true
  wait $api $relay 2>/dev/null || true
  services=""

  # The next cell must start on a whole cluster, or it would measure the leftovers of this
  # one's outage rather than its own.
  for _ in $(seq 120); do settled && break; sleep 1; done
  make elect-preferred >/dev/null 2>&1 || true
  echo
}

{
  echo "exp-13 — a broker outage under load: does the front door keep answering, and what piles up"
  echo "date: $stamp"
  echo "load: $rate payments/s for $window; brokers killed at $kill_after for $down_for"
  echo

  cell "cell A: one broker down — still above min.insync.replicas" 1 exp13-one
  cell "cell B: two brokers down — below min.insync.replicas" "1,2" exp13-two
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
