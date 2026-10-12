#!/usr/bin/env bash
# Drill 02 — helm rollback under load: back to the revision before the last upgrade, named by
# number (a bare rollback goes to current-1, which is not always the good one — runbook.md),
# without an error, and without the migrate hook (rollback runs no pre-upgrade hook).
set -euo pipefail
cd "$(dirname "$0")"   # the chart path and lib.sh are relative to this directory
source ./lib.sh

readonly rate=20 duration=60
readonly run="02-$(date +%s)"
readonly log="$results_dir/02-rollback-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$results_dir"

payment_targets $((rate * duration)) "$run" > "$results_dir/.targets"
{
  echo "drill 02 started $(stamp)"
  helm history payments -n "$namespace" --max 3
  echo "migrate job before: $(kubectl get job migrate -n "$namespace" -o jsonpath='{.metadata.creationTimestamp}')"
} | tee "$log"

vegeta attack -format=json -targets="$results_dir/.targets" -rate="$rate" -duration="${duration}s" \
  > "$results_dir/.attack.bin" &
attack=$!

# The last revision that is not the current one and deployed successfully at the time.
target=$(helm history payments -n "$namespace" -o json |
  jq -r '.[:-1] | map(select(.status == "superseded")) | last | .revision')
echo "rolling back to revision $target" | tee -a "$log"

sleep 10
started=$(date +%s)
helm rollback payments "$target" -n "$namespace" --wait --timeout 5m
echo "rollback took $(( $(date +%s) - started ))s" | tee -a "$log"
{
  helm history payments -n "$namespace" --max 3
  echo "migrate job after:  $(kubectl get job migrate -n "$namespace" -o jsonpath='{.metadata.creationTimestamp}')"
  kubectl get deploy -n "$namespace" -l app.kubernetes.io/part-of=payments \
    -o custom-columns=DEPLOYMENT:.metadata.name,IMAGE:.spec.template.spec.containers[0].image
} | tee -a "$log"

wait "$attack"
vegeta report < "$results_dir/.attack.bin" | tee -a "$log"
rm -f "$results_dir/.targets" "$results_dir/.attack.bin"
