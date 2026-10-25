#!/usr/bin/env bash
# exp-13 — payments taken through the real API while brokers die underneath the relay.
#
# Cells A and B over the same load. One broker down is a leader election: the cluster still
# has two replicas in sync, which is what min.insync.replicas=2 asks for. Two brokers down is
# below it, and the publisher is refused for the whole outage — which is the case the outbox
# exists for. Cell C keeps every broker and stops the consumer instead, on the service's own
# topic and group, so the dashboard's lag panels have a run that moves them.
# CELLS picks which run (default all three).
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
go build -o "$binaries/consumer" ./cmd/payments-consumer

# dashboard prints what the Grafana dashboard showed during a cell, read from Prometheus for
# the same window: the panels an operator would have looked at, as numbers in the log rather
# than a claim that the dashboard "shows it". One row per 5 s scrape.
# The lag set queries what the dashboard's panels 5 and 102 query (payments.json, "Consumer
# group lag" and "Consumer group — members, and offsets going backwards"), summed or taken at
# the minimum over partitions to fit one column.
dashboard() {
  local from="$1" to="$2" set="${3:-outage}"
  python3 - "$from" "$to" "$set" <<'PY'
import json, sys, urllib.parse, urllib.request
start, end, which = sys.argv[1], sys.argv[2], sys.argv[3]
sets = {
    "outage": [
        ("backlog", 'sum(outbox_backlog_records{service="outbox-relay"})'),
        ("published/s", 'sum(rate(outbox_records_published_total{service="outbox-relay"}[30s]))'),
        ("api 2xx/s", 'sum(rate(http_requests_total{status=~"2.."}[30s]))'),
        ("under-repl", 'sum(kafka_topic_partition_under_replicated_partition{topic="exp13.payments"})'),
        ("brokers", 'kafka_brokers'),
    ],
    "lag": [
        ("lag", 'sum(kafka_consumergroup_lag{consumergroup="payments-consumer", topic="payments.main"})'),
        ("members", 'kafka_consumergroup_members{consumergroup="payments-consumer"}'),
        ("offset 1m min", 'min(delta(kafka_consumergroup_current_offset{consumergroup="payments-consumer", topic="payments.main"}[1m]))'),
        ("handled/s", 'sum(rate(payment_events_handled_total[30s]))'),
        ("published/s", 'sum(rate(outbox_records_published_total{service="outbox-relay"}[30s]))'),
        ("consumer up", 'up{job="payments", service="payments-consumer"}'),
    ],
}
panels = sets[which]
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

# consumer_cell takes the same payments through the API and the relay onto payments.main, with
# the service's own consumer reading it, and stops that consumer for the same thirty seconds
# the other cells stop brokers. Nothing else changes, so whatever the lag panels show is the
# consumer's absence and its catch-up.
consumer_cell() {
  local merchant="$1"
  docker exec -i kafka-lab-postgres psql -U lab -d lab -q -c \
    "TRUNCATE outbox; DELETE FROM payments WHERE merchant_id = '$merchant';"

  PAYMENTS_API_KEY=lab-key "$binaries/api" >/dev/null 2>&1 &
  local api=$!
  OUTBOX_INTERVAL=500ms "$binaries/relay" >/dev/null 2>&1 &
  local relay=$!
  "$binaries/consumer" >/dev/null 2>&1 &
  local consumer=$!
  services="$api $relay $consumer"
  sleep 4

  echo "== cell C: the consumer stopped — every broker up =="
  local from
  from="$(date +%s)"
  "$binaries/exp-13" -merchant "$merchant" -rate "$rate" -window "$window" \
    -kill-after "$kill_after" -down-for "$down_for" -stopped payments-consumer &
  local load=$!
  sleep "${kill_after%s}"
  kill -9 "$consumer" 2>/dev/null || true
  wait "$consumer" 2>/dev/null || true
  echo "payments-consumer killed"
  sleep "${down_for%s}"
  "$binaries/consumer" >/dev/null 2>&1 &
  consumer=$!
  services="$api $relay $consumer"
  echo "payments-consumer started again"
  wait "$load"
  sleep 10
  echo "grafana window: from=${from}000 to=$(date +%s)000"
  dashboard "$from" "$(date +%s)" lag || echo "dashboard: Prometheus did not answer"

  kill $api $relay $consumer 2>/dev/null || true
  wait $api $relay $consumer 2>/dev/null || true
  services=""
  echo
}

{
  # The cells run in this group's subshell, which is where services is set; the trap above
  # belongs to the parent and would see it empty, so the group arms its own.
  trap restore EXIT
  echo "exp-13 — a broker outage under load: does the front door keep answering, and what piles up"
  echo "date: $stamp"
  echo "load: $rate payments/s for $window; brokers killed at $kill_after for $down_for"
  echo

  for which in ${CELLS:-A B C}; do
    case "$which" in
      A) cell "cell A: one broker down — still above min.insync.replicas" 1 exp13-one ;;
      B) cell "cell B: two brokers down — below min.insync.replicas" "1,2" exp13-two ;;
      C) consumer_cell exp13-lag ;;
      *) echo "exp-13: unknown cell $which"; exit 1 ;;
    esac
  done
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
