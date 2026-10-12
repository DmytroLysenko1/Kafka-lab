#!/usr/bin/env bash
# Drill 04f — the database goes away (drill 04's case f again), and every component that
# depends on it is watched: the API and the schema registry must stay up and only go unready;
# the relay retries every tick, and the consumer exits only on a record it cannot count
# (docs/k8s/decisions.md). The drill records their restarts, and how long after the database
# is back a payment is accepted and counted again.
set -euo pipefail
cd "$(dirname "$0")"   # lib.sh is relative to this directory
source ./lib.sh

readonly log="$results_dir/04f-database-outage-$(date +%Y%m%d-%H%M%S).log"
readonly watched='app.kubernetes.io/name in (apicurio,payments-api,payments-consumer,outbox-relay)'
readonly merchant="m-drill-04f-$(date +%s)"
mkdir -p "$results_dir"
exec > >(tee "$log") 2>&1

pods() {
  kubectl get pods -n "$namespace" -l "$watched" --sort-by=.metadata.name \
    -o custom-columns='POD:.metadata.name,READY:.status.containerStatuses[0].ready,RESTARTS:.status.containerStatuses[0].restartCount,WAITING:.status.containerStatuses[0].state.waiting.reason,LAST:.status.containerStatuses[0].lastState.terminated.reason'
}

seconds_since() { echo $(($(date +%s) - $1)); }

# Waits for a check to pass, at most five minutes: a stand that never recovers ends the drill
# with a message instead of hanging it.
wait_until() {
  local what=$1
  shift
  for _ in $(seq 1 300); do
    if "$@"; then
      return 0
    fi
    sleep 1
  done
  echo "gave up after 300 s waiting for $what" >&2
  exit 1
}

# One payment: the time to the first 2xx is how long the outage lasted for a caller.
authorized() {
  local status
  status=$(curl -s -o /dev/null -w '%{http_code}' -H "Host: $api_host" -H "X-API-Key: $(api_key)" \
    -H "Idempotency-Key: $merchant" -H 'Content-Type: application/json' \
    -d "{\"merchant_id\":\"$merchant\",\"amount_minor\":100,\"currency\":\"EUR\"}" "$api/payments")
  [[ $status == 20[01] ]]
}

counted() {
  curl -s -H "Host: $api_host" -H "X-API-Key: $(api_key)" "$api/merchants/$merchant/total" |
    jq -e '.totals[] | select(.currency == "EUR" and .authorized_minor == 100)' >/dev/null
}

echo "drill 04f started $(stamp)"
# Everything ready first, so an unready pod during the outage is the outage's doing.
kubectl wait --for=condition=Ready pod -l "$watched" -n "$namespace" --timeout=5m >/dev/null
pods
kubectl annotate cluster payments-db -n "$namespace" --overwrite cnpg.io/hibernation=on >/dev/null
kubectl wait --for=delete pod -l cnpg.io/cluster=payments-db -n "$namespace" --timeout=8m >/dev/null
echo "database pod gone $(stamp); sampling every 30 s for 90 s, more than three 20 s liveness periods"
for _ in 1 2 3; do
  sleep 30
  echo "--- $(stamp)"
  pods
done

kubectl annotate cluster payments-db -n "$namespace" --overwrite cnpg.io/hibernation=off >/dev/null
unhibernated=$(date +%s)
# The cluster's Ready condition can still read true from before the hibernation for a moment,
# so wait for the database pod itself: created, then ready.
kubectl wait --for=create pod/payments-db-1 -n "$namespace" --timeout=5m >/dev/null
kubectl wait --for=condition=Ready pod/payments-db-1 -n "$namespace" --timeout=8m >/dev/null
back=$(date +%s)
echo "database back $(stamp), $((back - unhibernated)) s after the hibernation was lifted"
pods

wait_until "the first payment to be accepted" authorized
echo "first payment accepted $(seconds_since "$back") s after the database was back"
wait_until "that payment to be counted" counted
echo "that payment counted $(seconds_since "$back") s after the database was back"
kubectl wait --for=condition=Ready pod -l "$watched" -n "$namespace" --timeout=10m >/dev/null
echo "every watched pod ready $(seconds_since "$back") s after the database was back ($(stamp))"
pods
