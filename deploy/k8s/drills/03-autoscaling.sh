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
printf 'GET %s/merchants/m-drill-hpa/total\nHost: %s\nX-API-Key: %s\n' "$api" "$api_host" "$(api_key)" |
  vegeta attack -rate="$rate" -duration="${duration}s" > "$results_dir/.attack.bin" &
attack=$!

started=$(date +%s)
end=$((started + duration + tail_seconds))
while [ "$(date +%s)" -lt "$end" ]; do
  kubectl get hpa payments-api -n "$namespace" --no-headers \
    -o custom-columns=CPU:.status.currentMetrics[0].resource.current.averageUtilization,DESIRED:.status.desiredReplicas,CURRENT:.status.currentReplicas |
    awk -v t=$(( $(date +%s) - started )) '{printf "t=%4ss cpu=%s%% desired=%s current=%s\n", t, $1, $2, $3}' | tee -a "$log"
  sleep 10
done

wait "$attack"
vegeta report < "$results_dir/.attack.bin" | tee -a "$log"
rm -f "$results_dir/.attack.bin"
