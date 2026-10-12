#!/usr/bin/env bash
# Drill 01 — a rolling update under load loses no request.
# 20 payments/s for 90 s through the ingress; 15 s in, the release moves to IMAGE_TAG_NEXT.
# Counts every non-2xx answer and times the rollout. IMAGE_TAG_NEXT must be loaded
# (make k8s-images IMAGE_TAG=…) and differ from the deployed tag: the same tag changes no
# pod template, replaces no pod, and would log a rollout that never happened.
set -euo pipefail
cd "$(dirname "$0")"   # the chart path and lib.sh are relative to this directory
source ./lib.sh

readonly next=${IMAGE_TAG_NEXT:?IMAGE_TAG_NEXT is required, a tag other than the deployed one}
readonly current=$(deployed_tag)
if [ "$next" = "$current" ]; then
  echo "IMAGE_TAG_NEXT=$next is already deployed: nothing would roll" >&2
  exit 1
fi
require_loaded "$next"
readonly rate=20 duration=90
readonly run="01-$(date +%s)"
readonly log="$results_dir/01-rolling-update-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$results_dir"

payment_targets $((rate * duration)) "$run" > "$results_dir/.targets"
{
  echo "drill 01 started $(stamp), rolling $current -> $next under $rate req/s for ${duration}s"
  helm history payments -n "$namespace" --max 1
} | tee "$log"

vegeta attack -format=json -targets="$results_dir/.targets" -rate="$rate" -duration="${duration}s" \
  > "$results_dir/.attack.bin" &
attack=$!

sleep 15
started=$(date +%s)
helm upgrade payments ../../helm/payments -n "$namespace" --reset-then-reuse-values --set image.tag="$next" \
  --wait --timeout 5m >/dev/null
echo "rollout to $next took $(( $(date +%s) - started ))s (helm --wait, migrate hook included)" | tee -a "$log"
kubectl get pods -n "$namespace" -l app.kubernetes.io/part-of=payments \
  -o custom-columns=POD:.metadata.name,IMAGE:.spec.containers[0].image,READY:.status.containerStatuses[0].ready | tee -a "$log"

wait "$attack"
vegeta report < "$results_dir/.attack.bin" | tee -a "$log"
rm -f "$results_dir/.targets" "$results_dir/.attack.bin"
