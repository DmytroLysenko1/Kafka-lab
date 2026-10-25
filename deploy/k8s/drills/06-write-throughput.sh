#!/usr/bin/env bash
# Drill 06 — the write path end to end: payments at rising rates, and for each rate what the
# API answered, how far the outbox fell behind, and how long until every accepted payment was
# counted. The rate at which the backlog stops draining within its step is the stand's write
# ceiling, and the stage that holds the backlog — outbox (relay) or inbox (consumer) — is the
# bottleneck. Payments go to fifty merchants, so the numbers measure the path, not one
# merchant's row lock, which is contention (exp-18's subject) rather than throughput.
set -euo pipefail
cd "$(dirname "$0")"   # lib.sh is relative to this directory
source ./lib.sh

readonly rates=${RATES:-"50 100 200 400"} step=${STEP:-30} last_step=${LAST_STEP:-120} merchants=50
readonly log="$results_dir/06-write-throughput-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$results_dir"
exec > >(tee "$log") 2>&1

sql() {
  kubectl exec -n "$namespace" payments-db-1 -c postgres -- psql -d payments -Atc "$1"
}

targets() {
  local count=$1 run_id=$2 key body
  key=$(api_key)
  for i in $(seq 1 "$count"); do
    body=$(printf '{"merchant_id":"m-%s-%d","amount_minor":1,"currency":"EUR"}' "$run_id" $((i % merchants)) | base64)
    printf '{"method":"POST","url":"%s/payments","header":{"X-API-Key":["%s"],"Idempotency-Key":["%s-%d"],"Content-Type":["application/json"]},"body":"%s"}\n' \
      "$api_in_cluster" "$key" "$run_id" "$i" "$body"
  done
}

# Seconds, from the end of the step, until the query returns what it should (at most 5 min).
seconds_until() {
  local query=$1 want=$2 since=$3
  until [ "$(sql "$query")" = "$want" ]; do
    if [ $(($(date +%s) - since)) -gt 300 ]; then
      echo ">300"
      return
    fi
    sleep 1
  done
  echo $(($(date +%s) - since))
}

echo "drill 06 started $(stamp): rates $rates payments/s, ${step}s each, $merchants merchants per step"
printf '%6s %8s %8s %8s %9s %9s %9s %12s %9s %9s\n' rate sent accepted stored p50_ms p99_ms max_ms outbox_left drain_s counted_s
# The HPA every 10 s while a step runs: it takes 30–40 s to raise its desired count even far
# above target (drill 03), so one reading after a 30 s step could not show it moving. The last
# step is held longer for the same reason.
watch_hpa() {
  local began=$1
  local t reading
  while true; do
    t=$(( $(date +%s) - began ))
    # A reading the API server did not answer is logged as unread; the watcher keeps going.
    if reading=$(kubectl get hpa payments-api -n "$namespace" --no-headers \
      -o custom-columns=CPU:.status.currentMetrics[0].resource.current.averageUtilization,DESIRED:.status.desiredReplicas,CURRENT:.status.currentReplicas 2>&1); then
      awk -v t="$t" '{printf "    hpa t=%ss cpu=%s%% desired=%s current=%s\n", t, $1, $2, $3}' <<<"$reading"
    else
      printf '    hpa t=%ss unread: %s\n' "$t" "$(head -1 <<<"$reading")"
    fi
    sleep 10
  done
}

last_rate=${rates##* }
for rate in $rates; do
  run_id="d06-$rate-$(date +%s)"
  seconds=$step
  [ "$rate" = "$last_rate" ] && seconds=$last_step
  # Sent from a pod in the cluster to the Service (lib.sh says why). The step's end is its start
  # plus its length, and everything timed from it is read before anything else is waited for:
  # the report comes from a polled pod, and the HPA watcher dies only once its `sleep 10`
  # returns — waiting on either first put a floor of 4–8 s under drain_s and counted_s on
  # 25.10. counted_s is the time until the run's merchants hold as many minor units as the
  # database holds payments for them (each is 1 minor unit), so it needs no number from the
  # report; the report's accepted count is then printed next to the database's.
  targets $((rate * seconds)) "$run_id" | load_start load-06 "$rate" "$seconds" json json
  ended=$(( $(date +%s) + seconds ))
  # Started with the attack, not before the targets are made and copied, so t=0 is its start.
  watch_hpa "$(date +%s)" > "$results_dir/.hpa" &
  watcher=$!
  sleep "$seconds"
  left=$(sql "SELECT count(*) FROM outbox WHERE published_at IS NULL")
  drained=$(seconds_until "SELECT count(*) FROM outbox WHERE published_at IS NULL" 0 "$ended")
  run="merchant_id LIKE 'm-$run_id-%'"
  counted=$(seconds_until "SELECT (SELECT count(*) FROM payments WHERE $run) = (SELECT coalesce(sum(authorized_minor), 0) FROM merchant_currency_totals WHERE $run)" t "$ended")
  kill "$watcher" 2>/dev/null || true
  wait "$watcher" 2>/dev/null || true
  report=$(load_report load-06)
  stored=$(sql "SELECT count(*) FROM payments WHERE $run")
  sent=$(jq '.requests' <<<"$report")
  accepted=$(jq '(.status_codes["201"] // 0) + (.status_codes["200"] // 0)' <<<"$report")
  p50=$(jq '.latencies["50th"] / 1e6 | floor' <<<"$report")
  p99=$(jq '.latencies["99th"] / 1e6 | floor' <<<"$report")
  max=$(jq '.latencies.max / 1e6 | floor' <<<"$report")
  printf '%6s %8s %8s %8s %9s %9s %9s %12s %9s %9s\n' "$rate" "$sent" "$accepted" "$stored" "$p50" "$p99" "$max" "$left" "$drained" "$counted"
  jq -c '{status_codes, errors: (.errors // [] | .[:3])}' <<<"$report"
  echo "  step held ${seconds}s; the API's autoscaler during it:"
  cat "$results_dir/.hpa"
done
rm -f "$results_dir/.hpa"
kubectl get hpa payments-api -n "$namespace" --no-headers \
  -o custom-columns=CPU:.status.currentMetrics[0].resource.current.averageUtilization,CURRENT:.status.currentReplicas |
  awk '{printf "api after the last step: cpu=%s%% replicas=%s\n", $1, $2}'
echo "drill 06 done $(stamp)"
