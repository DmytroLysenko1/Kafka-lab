#!/usr/bin/env bash
# Drill 03 — the API's HPA under load: how long until it scales out, how far, and how long
# the scale-in waits after the load stops (the HPA's 300 s downscale stabilization).
# Load is reads, not payments: CPU is what the HPA watches, and reads cost no database rows.
set -euo pipefail
cd "$(dirname "$0")"   # the chart path and lib.sh are relative to this directory
source ./lib.sh

readonly rate=${RATE:-400} duration=${DURATION:-180} tail_seconds=${TAIL:-420}
readonly log="$results_dir/03-autoscaling-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$results_dir"

echo "drill 03 started $(stamp), $rate req/s for ${duration}s, watching ${tail_seconds}s after" | tee "$log"
# From a pod in the cluster to the Service (lib.sh says why): the load reaches the API pods
# without passing through the host's port forward.
printf 'GET %s/merchants/m-drill-hpa/total\nX-API-Key: %s\n' "$api_in_cluster" "$(api_key)" |
  load_start load-03 "$rate" "$duration" http text

started=$(date +%s)
end=$((started + duration + tail_seconds))
# Under this load the laptop is saturated, and the API server sometimes misses a TLS
# handshake (it did, on 25.10): a reading that could not be taken is logged as unread and the
# drill goes on, rather than ending it with the load still running and nothing measured.
while [ "$(date +%s)" -lt "$end" ]; do
  t=$(( $(date +%s) - started ))
  if reading=$(kubectl get hpa payments-api -n "$namespace" --no-headers \
    -o custom-columns=CPU:.status.currentMetrics[0].resource.current.averageUtilization,DESIRED:.status.desiredReplicas,CURRENT:.status.currentReplicas 2>&1); then
    awk -v t="$t" '{printf "t=%4ss cpu=%s%% desired=%s current=%s\n", t, $1, $2, $3}' <<<"$reading" | tee -a "$log"
  else
    printf 't=%4ss unread: %s\n' "$t" "$(head -1 <<<"$reading")" | tee -a "$log"
  fi
  sleep 10
done

load_report load-03 | tee -a "$log"
