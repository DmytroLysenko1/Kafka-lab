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

readonly rates=${RATES:-"50 100 200 400"} step=${STEP:-30} merchants=50
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
    printf '{"method":"POST","url":"%s/payments","header":{"Host":["%s"],"X-API-Key":["%s"],"Idempotency-Key":["%s-%d"],"Content-Type":["application/json"]},"body":"%s"}\n' \
      "$api" "$api_host" "$key" "$run_id" "$i" "$body"
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
printf '%6s %8s %8s %9s %9s %9s %12s %9s %9s\n' rate sent accepted p50_ms p99_ms max_ms outbox_left drain_s counted_s
for rate in $rates; do
  run_id="d06-$rate-$(date +%s)"
  targets $((rate * step)) "$run_id" |
    vegeta attack -format=json -rate="$rate" -duration="${step}s" > "$results_dir/.attack.bin"
  ended=$(date +%s)
  report=$(vegeta report -type=json < "$results_dir/.attack.bin")
  sent=$(jq '.requests' <<<"$report")
  accepted=$(jq '(.status_codes["201"] // 0) + (.status_codes["200"] // 0)' <<<"$report")
  p50=$(jq '.latencies["50th"] / 1e6 | floor' <<<"$report")
  p99=$(jq '.latencies["99th"] / 1e6 | floor' <<<"$report")
  max=$(jq '.latencies.max / 1e6 | floor' <<<"$report")
  left=$(sql "SELECT count(*) FROM outbox WHERE published_at IS NULL")
  drained=$(seconds_until "SELECT count(*) FROM outbox WHERE published_at IS NULL" 0 "$ended")
  counted=$(seconds_until "SELECT coalesce(sum(authorized_minor), 0) FROM merchant_currency_totals WHERE merchant_id LIKE 'm-$run_id-%'" "$accepted" "$ended")
  printf '%6s %8s %8s %9s %9s %9s %12s %9s %9s\n' "$rate" "$sent" "$accepted" "$p50" "$p99" "$max" "$left" "$drained" "$counted"
  jq -c '{status_codes, errors: (.errors // [] | .[:3])}' <<<"$report"
done
rm -f "$results_dir/.attack.bin"
kubectl get hpa payments-api -n "$namespace" --no-headers \
  -o custom-columns=CPU:.status.currentMetrics[0].resource.current.averageUtilization,CURRENT:.status.currentReplicas |
  awk '{printf "api after the last step: cpu=%s%% replicas=%s\n", $1, $2}'
echo "drill 06 done $(stamp)"
