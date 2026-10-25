#!/usr/bin/env bash
# Drill 07 — the logging and metrics wiring, shown working rather than configured: every
# payments target scraped (up{job="payments"}), the service's own counters moving by what a
# known load sent, and each pod's log lines arriving in Loki under the labels the
# runbook queries by. Prometheus and Loki are asked through the API server's service proxy,
# so the drill needs no port-forward and reads what an operator's Grafana would.
set -euo pipefail
cd "$(dirname "$0")"   # lib.sh is relative to this directory
source ./lib.sh

readonly rate=20 duration=15
readonly run="07-$(date +%s)"
readonly prometheus=/api/v1/namespaces/monitoring/services/monitoring-kube-prometheus-prometheus:9090/proxy
readonly loki=/api/v1/namespaces/monitoring/services/loki:3100/proxy
readonly log="$results_dir/07-observability-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$results_dir"
exec > >(tee "$log") 2>&1

# promql prints one line per series: its labels that matter, then the value.
promql() {
  kubectl get --raw "$prometheus/api/v1/query?query=$(jq -rn --arg q "$1" '$q|@uri')" |
    jq -r '.data.result[] | "  \(.metric.service // .metric.container // "-") \(.metric.pod // "") \(.value[1])"'
}

# counter sums one counter over the service's pods, so a value is comparable across scrapes.
counter() {
  kubectl get --raw "$prometheus/api/v1/query?query=$(jq -rn --arg q "sum($1)" '$q|@uri')" |
    jq -r '.data.result[0].value[1] // "none"'
}

echo "drill 07 started $(stamp), images $(deployed_tag)"
echo
echo "every payments target, as Prometheus scrapes it (up{job=\"payments\"}, 1 = scraped):"
promql 'up{job="payments"}'
echo

# Scraped every 5 s (the ServiceMonitor's interval); the wait after the load covers the
# relay's and the consumer's work and several scrapes. Three counters, one per component.
readonly api_requests='http_requests_total{job="payments",service="payments-api",route="POST /payments"}'
readonly published='outbox_records_published_total{job="payments"}'
readonly handled='payment_events_handled_total{job="payments"}'
api_before=$(counter "$api_requests") published_before=$(counter "$published") handled_before=$(counter "$handled")

payment_targets $((rate * duration)) "$run" > "$results_dir/.targets"
vegeta attack -format=json -targets="$results_dir/.targets" -rate="$rate" -duration="${duration}s" > "$results_dir/.attack.bin"
accepted=$(vegeta report -type=json < "$results_dir/.attack.bin" | jq '(.status_codes["201"] // 0) + (.status_codes["200"] // 0)')
echo "sent $((rate * duration)) payments at $rate/s, $accepted accepted; waiting 30 s for the pipeline and the scrapes"
sleep 30
echo "the service's own counters, summed over its pods, before and after:"
echo "  http_requests_total{route=\"POST /payments\"}: $api_before -> $(counter "$api_requests")"
echo "  outbox_records_published_total:                $published_before -> $(counter "$published")"
echo "  payment_events_handled_total:                  $handled_before -> $(counter "$handled")"
echo

# Every running pod's start line, looked up in Loki by the labels Alloy puts on it
# (platform/values/alloy.yaml), from the pod's start on. A recent window would not do: the
# API and the consumer log at start and on errors, not per request, and on 25.10 a 10-minute
# window held no consumer line at all although its lines were in Loki. A start line found
# for each pod shows every pod's output reaching Loki; the relay's per-batch line, that it
# keeps arriving.
echo "each running pod's start line in Loki, by {namespace=\"lab\", app, pod}:"
now=$(date +%s)
for app in payments-api outbox-relay payments-consumer; do
  kubectl get pods -n "$namespace" -l "app.kubernetes.io/name=$app" --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name} {.status.startTime}{"\n"}{end}' |
    while read -r pod started; do
      # python3, not date: parsing a given time takes -j -f on BSD and -d on GNU.
      from=$(( $(python3 -c 'import sys, datetime; print(int(datetime.datetime.strptime(sys.argv[1], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp()))' "$started") - 60 ))
      selector="{namespace=\"lab\", app=\"$app\", pod=\"$pod\"} |= \"started\""
      answer=$(kubectl get --raw "$loki/loki/api/v1/query_range?query=$(jq -rn --arg q "$selector" '$q|@uri')&start=${from}000000000&end=${now}000000000&limit=10")
      line=$(jq -r '[.data.result[].values[]] | sort_by(.[0]) | first | .[1] // "NONE FOUND"' <<<"$answer" | cut -c1-150)
      echo "  $pod (started $started): $line"
    done
done
selector='{namespace="lab", app="outbox-relay"} |= "outbox published"'
answer=$(kubectl get --raw "$loki/loki/api/v1/query_range?query=$(jq -rn --arg q "$selector" '$q|@uri')&start=$((now - 120))000000000&end=${now}000000000&limit=1000")
echo "  outbox-relay \"outbox published\" lines in the last 2 minutes, the load's included: $(jq '[.data.result[].values[]] | length' <<<"$answer")"
echo
echo "drill 07 done $(stamp)"
rm -f "$results_dir/.targets" "$results_dir/.attack.bin"
