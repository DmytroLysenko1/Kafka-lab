#!/usr/bin/env bash
# Drill 05 — a Kafka broker pod dies under payment load. Kubernetes brings the pod back on
# its own (compose left it dead on purpose, restart: "no"), so the question changes from
# "how fast does the cluster react" (exp-04: 10.6 s, docs/00-journal.md) to "how long until every partition
# is fully replicated again", and whether a payment was lost on the way.
set -euo pipefail
cd "$(dirname "$0")"   # the chart path and lib.sh are relative to this directory
source ./lib.sh

readonly victim=${VICTIM:-kafka-lab-dual-1} witness=${WITNESS:-kafka-lab-dual-0}
readonly rate=20 duration=120
readonly run="05-$(date +%s)"
readonly log="$results_dir/05-broker-pod-loss-$(date +%Y%m%d-%H%M%S).log"
mkdir -p "$results_dir"

# The admin tool is a JVM of its own inside the witness broker's container, and it inherits
# the broker's KAFKA_HEAP_OPTS (-Xmx512m): the first runs pushed the witness to 918Mi of a
# 1Gi limit, while the victim was down — an OOM there would have been two brokers of three.
# Prints the count, or "error" when the tool could not answer: a failed exec must never read
# as zero under-replicated partitions, which the loop would take for healed.
under_replicated() {
  local out
  if ! out=$(kubectl exec -n "$namespace" "$witness" -c kafka -- \
    env KAFKA_HEAP_OPTS=-Xmx96m bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --under-replicated-partitions 2>/dev/null); then
    echo error
    return
  fi
  grep -c 'Partition:' <<<"$out" || true
}

# The witness's memory, from metrics-server (15 s resolution): the drill's own admin JVM runs
# in that container, so this is where it would show.
witness_memory() {
  kubectl top pod -n "$namespace" "$witness" --containers --no-headers 2>/dev/null |
    awk '$2 == "kafka" {print $4}'
}

payment_targets $((rate * duration)) "$run" > "$results_dir/.targets"
echo "drill 05 started $(stamp), killing $victim under $rate payments/s" | tee "$log"
vegeta attack -format=json -targets="$results_dir/.targets" -rate="$rate" -duration="${duration}s" \
  > "$results_dir/.attack.bin" &
attack=$!

sleep 15
killed=$(date +%s)
kubectl delete pod -n "$namespace" "$victim" --grace-period=0 --force >/dev/null 2>&1
echo "t=0 $victim deleted (no grace period)" | tee -a "$log"

ready_at="" healed_at="" degraded=""
while true; do
  t=$(( $(date +%s) - killed ))
  urp=$(under_replicated)
  ready=$(kubectl get pod -n "$namespace" "$victim" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo false)
  echo "t=${t}s pod_ready=${ready:-false} under_replicated=$urp witness_memory=$(witness_memory)" | tee -a "$log"
  if [ -z "$ready_at" ] && [ "$ready" = "true" ]; then ready_at=$t; fi
  # Healing is counted on its own: the first run tied it to readiness and reported 22 s for
  # partitions its own samples showed back in sync at 15 s.
  # A zero sampled before the controller noticed the loss is not healing, so it only counts
  # after partitions were seen under-replicated.
  if [ "$urp" != "0" ] && [ "$urp" != "error" ]; then degraded=yes; fi
  if [ -z "$healed_at" ] && [ -n "$degraded" ] && [ "$urp" = "0" ]; then healed_at=$t; fi
  if [ -n "$healed_at" ] && [ -n "$ready_at" ]; then break; fi
  if [ "$t" -gt 600 ]; then echo "not healed after 600 s" | tee -a "$log"; break; fi
  sleep 2
done
echo "pod ready after ${ready_at}s, no under-replicated partition after ${healed_at}s" | tee -a "$log"

wait "$attack"
vegeta report < "$results_dir/.attack.bin" | tee -a "$log"
sleep 20
key=$(api_key)
echo "merchant total for the run: $(curl -s "$api/merchants/m-drill-$run/total" -H "Host: $api_host" -H "X-API-Key: $key")" | tee -a "$log"
echo "payments accepted (2xx): $(vegeta report -type=json < "$results_dir/.attack.bin" | jq '.status_codes["201"] // 0')" | tee -a "$log"
rm -f "$results_dir/.targets" "$results_dir/.attack.bin"
