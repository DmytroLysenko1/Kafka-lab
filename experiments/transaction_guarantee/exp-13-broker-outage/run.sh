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

# dashboard prints what the Grafana dashboard showed during a cell, read from Prometheus for
# the same window: the panels an operator would have looked at, as numbers in the log rather
# than a claim that the dashboard "shows it". One row per 5 s scrape.
dashboard() {
  local from="$1" to="$2"
  python3 - "$from" "$to" <<'PY'
import json, sys, urllib.parse, urllib.request
start, end = sys.argv[1], sys.argv[2]
panels = [
    ("backlog", 'sum(outbox_backlog_records{service="outbox-relay"})'),
    ("published/s", 'sum(rate(outbox_records_published_total{service="outbox-relay"}[30s]))'),
    ("api 2xx/s", 'sum(rate(http_requests_total{status=~"2.."}[30s]))'),
    ("under-repl", 'sum(kafka_topic_partition_under_replicated_partition{topic="exp13.payments"})'),
    ("brokers", 'kafka_brokers'),
]
columns = {}
for name, query in panels:
    url = "http://localhost:9090/api/v1/query_range?" + urllib.parse.urlencode(
        {"query": query, "start": start, "end": end, "step": "5"})
    with urllib.request.urlopen(url, timeout=10) as answer:
        result = json.load(answer)["data"]["result"]
    columns[name] = {int(float(t)): v for t, v in result[0]["values"]} if result else {}
times = sorted({t for column in columns.values() for t in column})
print("dashboard, from Prometheus, every 5 s (a dash is a scrape that had no answer):")
print("  t(s) " + "".join(f"{name:>13}" for name, _ in panels))
for t in times:
    row = "".join(f"{(format(float(columns[n][t]), '.1f') if t in columns[n] else '-'):>13}" for n, _ in panels)
    print(f"  {t - int(float(start)):>4} " + row)
PY
}

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
  local from
  from="$(date +%s)"
  "$binaries/exp-13" -merchant "$merchant" -rate "$rate" -window "$window" \
    -kill-after "$kill_after" -down-for "$down_for" -victims "$victims"
  # Two more scrapes, so the last seconds of the cell are in Prometheus before it is asked.
  sleep 10
  echo "grafana window: from=${from}000 to=$(date +%s)000"
  dashboard "$from" "$(date +%s)" || echo "dashboard: Prometheus did not answer"

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
