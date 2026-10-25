#!/usr/bin/env bash
# exp-14 — what a consumer joining the group costs, under each assignment strategy.
#
# A steady stream over six partitions, one consumer, and a second joining after 15 s: eager
# (range), cooperative-sticky and KIP-848's broker-side assignment, each with an instant
# handler and with one that holds rebalances off while it works. Each run prints, per
# partition, the longest stretch nothing was handled before and around the join.
#
# Usage: make exp-14
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"

binary="$(mktemp -d)/exp14"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-14-rebalance-strategies

# The KIP-848 run assigns on the broker's heartbeat, so its timer is part of the result.
docker exec kafka-lab-kafka1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type brokers --entity-name 1 --describe --all 2>/dev/null \
  | grep -E 'group\.consumer\.heartbeat\.interval\.ms|group\.initial\.rebalance\.delay\.ms' \
  > "$here/results/broker-group-timers.log"

# Two handlers. "instant" is franz-go's default, where a rebalance does not wait for the
# handler. "blocking" holds rebalances off while a batch is handled, as the Java consumer
# does, at a load one member can still keep up with.
# slow-member is the case the blocking runs cannot show: one member holds every batch 2 s, a
# fast one runs beside it, and a third joins — under eager the fast member gives up all it
# has and waits for the slow one to finish; under cooperative it keeps what does not move.
for mode in ${MODES:-instant blocking slow-member}; do
  case "$mode" in
    instant) flags=() ;;
    blocking) flags=(-block -work 2ms -rate 400) ;;
    slow-member) flags=(-block -with-peer -hold 2s -rate 400) ;;
    *) echo "exp-14: unknown mode $mode"; exit 1 ;;
  esac
  for strategy in eager cooperative server; do
    make reset-topic TOPIC=exp14.payments
    log="$here/results/$strategy-$mode-$stamp.log"
    {
      echo "exp-14 — a consumer joins, strategy: $strategy, handler: $mode"
      echo "date: $stamp"
      echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
      echo
      "$binary" -strategy "$strategy" ${flags[@]+"${flags[@]}"}
    } 2>&1 | tee "$log"
  done
done

echo "written to results/*-$stamp.log"
