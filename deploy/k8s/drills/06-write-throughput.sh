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

pod=$(load_name 06)
scratch=$(mktemp -d)
watcher=""
cleanup() {
  load_stop "$pod"
  [ -z "$watcher" ] || kill "$watcher" 2>/dev/null || true
  rm -rf "$scratch"
}
trap cleanup EXIT

sql() {
  kubectl exec -n "$namespace" payments-db-1 -c postgres -- psql -d payments -Atc "$1"
}

targets() {
  local count=$1 run_id=$2 key body
  key=$(api_key)
  for i in $(seq 1 "$count"); do
    body=$(printf '{"merchant_id":"m-%s-%d","amount_minor":1,"currency":"EUR"}' "$run_id" $((i % merchants)) | base64)
    printf '{"method":"POST","url":"%s/payments","header":{"Host":["%s"],"X-API-Key":["%s"],"Idempotency-Key":["%s-%d"],"Content-Type":["application/json"]},"body":"%s"}\n' \
      "$api_in_cluster" "$api_host" "$key" "$run_id" "$i" "$body"
  done
}

# One sample of the write path for a run's merchants, stamped by Postgres's own clock: the
# time, the unpublished outbox rows, the run's stored payments and the minor units counted
# into its totals (each payment is 1 minor unit, so counted = stored when all are in). The
# clock is the kind nodes' — the same as the load pod's, whose report says when the attack
# ended — so a duration is the difference of two readings of one clock, and taking a sample
# (a kubectl exec) costs resolution, never an offset.
sample() {
  local run="merchant_id LIKE 'm-$1-%'"
  sql "SELECT extract(epoch FROM clock_timestamp()), (SELECT count(*) FROM outbox WHERE published_at IS NULL), (SELECT count(*) FROM payments WHERE $run), (SELECT coalesce(sum(authorized_minor), 0) FROM merchant_currency_totals WHERE $run)" |
    tr '|' ' '
}

# Samples from just after the step's end until every payment sent is stored and counted and
# the outbox is empty, or for 5 min.
sample_until_done() {
  local run_id=$1 expected=$2 out=$3 began line
  began=$(date +%s)
  while [ $(( $(date +%s) - began )) -le 300 ]; do
    # A sample the API server did not answer is skipped, not taken for an empty outbox.
    line=$(sample "$run_id") || continue
    # shellcheck disable=SC2086
    set -- $line
    [ $# = 4 ] || continue
    echo "$line" >> "$out"
    if [ "$2" = 0 ] && [ "$3" -ge "$expected" ] && [ "$3" = "$4" ]; then return; fi
  done
}

# From the samples and vegeta's report: the backlog at the step's end (the first sample
# taken after it), and the seconds from the end until the outbox was empty and until every
# accepted payment was counted — each the first sample that shows it, so the figure is an
# upper bound by at most one sampling interval, which the line also prints.
timings() {
  python3 - "$1" "$2" "$3" <<'EOF'
import re
import sys
from datetime import datetime
samples_file, latest, accepted = sys.argv[1], sys.argv[2], int(sys.argv[3])
# vegeta prints nanoseconds and the zone as Z or ±hh:mm; datetime takes microseconds.
base, frac, zone = re.fullmatch(r'(.*T\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)', latest).groups()
zone = '+00:00' if zone == 'Z' else zone
end = datetime.fromisoformat(f"{base}.{(frac or '0')[:6].ljust(6, '0')}{zone}").timestamp()
rows = [tuple(float(x) for x in line.split()) for line in open(samples_file) if line.strip()]
after = [r for r in rows if r[0] >= end]
def first(cond):
    hit = next((r for r in after if cond(r)), None)
    return f"{hit[0] - end:.1f}" if hit else ">300"
left = int(after[0][1]) if after else "n/a"
drained = first(lambda r: r[1] == 0)
counted = first(lambda r: r[2] >= accepted and r[3] == r[2])
stored = int(rows[-1][2]) if rows else 0
gaps = [b[0] - a[0] for a, b in zip(after, after[1:])]
interval = f"{sum(gaps) / len(gaps):.2f}" if gaps else "n/a"
print(left, drained, counted, stored, len(after), interval)
EOF
}

# The HPA every 10 s for the whole drill, on the drill's clock: it takes 30–40 s to raise its
# desired count even far above target (drill 03), and a scale-out one step causes can show
# only after it, so the readings run on between steps and each step names its own span.
watch_hpa() {
  local began=$1 t reading
  while true; do
    t=$(( $(date +%s) - began ))
    # A reading the API server did not answer is logged as unread; the watcher keeps going.
    if reading=$(kubectl get hpa payments-api -n "$namespace" --no-headers \
      -o custom-columns=CPU:.status.currentMetrics[0].resource.current.averageUtilization,DESIRED:.status.desiredReplicas,CURRENT:.status.currentReplicas 2>&1); then
      awk -v t="$t" '{printf "  hpa t=%ss cpu=%s%% desired=%s current=%s\n", t, $1, $2, $3}' <<<"$reading"
    else
      printf '  hpa t=%ss unread: %s\n' "$t" "$(head -1 <<<"$reading")"
    fi
    sleep 10
  done
}

began=$(date +%s)
echo "drill 06 started $(stamp): rates $rates payments/s, ${step}s each and ${last_step}s for the last, $merchants merchants per step"
watch_hpa "$began" > "$scratch/hpa" &
watcher=$!
printf '%6s %9s %8s %8s %8s %9s %9s %9s %12s %9s %9s %8s\n' \
  rate step_t sent accepted stored p50_ms p99_ms max_ms outbox_left drain_s counted_s samples
last_rate=${rates##* }
for rate in $rates; do
  run_id="d06-$rate-$(date +%s)"
  seconds=$step
  [ "$rate" = "$last_rate" ] && seconds=$last_step
  expected=$((rate * seconds))
  # Sent from a pod in the cluster through Traefik (lib.sh says why).
  targets "$expected" "$run_id" | load_start "$pod" "$rate" "$seconds" json json
  from=$(( $(date +%s) - began ))
  # The attack starts within a second of load_start returning; sampling starts a second after
  # its nominal end, and timings() drops any sample taken before the real end all the same.
  sleep $((seconds + 1))
  : > "$scratch/samples"
  sample_until_done "$run_id" "$expected" "$scratch/samples"
  if ! report=$(load_report "$pod" "$seconds"); then
    echo "$report"
    exit 1
  fi
  sent=$(jq '.requests' <<<"$report")
  accepted=$(jq '(.status_codes["201"] // 0) + (.status_codes["200"] // 0)' <<<"$report")
  p50=$(jq '.latencies["50th"] / 1e6 | floor' <<<"$report")
  p99=$(jq '.latencies["99th"] / 1e6 | floor' <<<"$report")
  max=$(jq '.latencies.max / 1e6 | floor' <<<"$report")
  read -r left drained counted stored taken interval <<<"$(timings "$scratch/samples" "$(jq -r '.latest' <<<"$report")" "$accepted")"
  printf '%6s %9s %8s %8s %8s %9s %9s %9s %12s %9s %9s %8s\n' \
    "$rate" "${from}-$((from + seconds))s" "$sent" "$accepted" "$stored" "$p50" "$p99" "$max" "$left" "$drained" "$counted" "$taken@${interval}s"
  jq -c '{status_codes, errors: (.errors // [] | .[:3])}' <<<"$report"
done
kill "$watcher" 2>/dev/null || true
wait "$watcher" 2>/dev/null || true   # reaped here, so the shell prints no "Terminated" into the log
watcher=""
echo
echo "the API's autoscaler through the drill (t on the drill's clock, as step_t above):"
cat "$scratch/hpa"
echo "drill 06 done $(stamp)"
