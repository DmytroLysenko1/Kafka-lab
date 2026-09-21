#!/usr/bin/env bash
# exp-09 — does a retried batch overtake the one after it?
#
# The same stream of captures and refunds, produced twice: once with idempotence off and
# five requests in flight, once idempotent. Each time a follower of a min.insync.replicas=3
# topic is frozen and thawed several times, so acks=all requests are refused and retried
# at the edges where the in-sync set shrinks and comes back.
#
# Usage: make exp-09
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
duration="${DURATION:-120s}"
flaps="${FLAPS:-3}"

cd "$repo"

# A frozen broker must never be left frozen: it holds its connections open and answers
# nothing, and every later experiment would hang on it rather than fail. The freeze happens
# inside a piped block, which is a subshell, so a variable set there never reaches this
# trap — every broker is thawed unconditionally instead. Unpausing one that is not paused
# fails harmlessly.
settled() {
  for id in 1 2 3; do
    out="$(docker exec "kafka-lab-kafka$id" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
      --describe --under-replicated-partitions 2>/dev/null)" || continue
    [ -z "$out" ] && return 0
    return 1
  done
  return 1
}
restore() {
  for id in 1 2 3; do docker unpause "kafka-lab-kafka$id" >/dev/null 2>&1 || true; done
  for _ in $(seq 120); do settled && break; sleep 1; done
  make elect-preferred >/dev/null 2>&1 || true
  return 0
}
trap restore EXIT

make exp-topics EXP="${here#"$repo"/experiments/}"

binary="$(mktemp -d)/exp09"
go build -o "$binary" ./experiments/transaction_guarantee/exp-09-reordering
exp09() { "$binary" "$@"; }

for producer in plain idempotent; do
  make reset-topic TOPIC=exp09.payments
  log="$here/results/$producer-$stamp.log"
  produced_out="$(dirname "$binary")/produced-$producer.txt"

  {
    echo "exp-09 — reordering under a frozen follower, producer: $producer"
    echo "date: $stamp"
    echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
    echo "stream: $duration, follower frozen and thawed $flaps times"
    echo

    placement="$(exp09 -phase leader)"
    printf '%s\n\n' "$placement"
    frozen_target="$(printf '%s\n' "$placement" | awk '/^FOLLOWER_P0/ {print $2}')"
    case "$frozen_target" in 1|2|3) ;; *) echo "exp-09: could not find a follower"; exit 1 ;; esac

    exp09 -phase produce -producer "$producer" -duration "$duration" >"$produced_out" 2>&1 &
    producing=$!

    for flap in $(seq "$flaps"); do
      echo "flap $flap: freezing kafka$frozen_target"
      docker pause "kafka-lab-kafka$frozen_target" >/dev/null
      exp09 -phase await-shrunk -timeout 2m
      docker unpause "kafka-lab-kafka$frozen_target" >/dev/null
      exp09 -phase await-full -timeout 2m
      echo
    done

    wait "$producing"
    cat "$produced_out"
    echo
    produced="$(awk '/^PRODUCED/ {print $2}' "$produced_out")"
    exp09 -phase verify -producer "$producer" -produced "$produced"
  } 2>&1 | tee "$log"
done

rm -rf "$(dirname "$binary")"
echo "written to results/plain-$stamp.log and results/idempotent-$stamp.log"
