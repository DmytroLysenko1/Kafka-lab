#!/usr/bin/env bash
# exp-14b — what a member leaving costs, and what static membership changes about it.
#
# exp-14's program, four cells: a second consumer that leaves for good, and one that leaves
# and comes back two seconds later — each with dynamic membership and with
# group.instance.id. Cooperative-sticky throughout, so the only thing that differs is
# membership. The session timeout is 12 s, not the 45 s default, so the run can outlast it.
#
# Usage: make exp-14b
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"

cd "$repo"
make exp-topics EXP=transaction_guarantee/exp-14-rebalance-strategies

binary="$(mktemp -d)/exp14"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-14-rebalance-strategies

for membership in dynamic static; do
  case "$membership" in
    dynamic) identity=() ;;
    static) identity=(-static) ;;
  esac
  for departure in gone restart; do
    case "$departure" in
      gone) comeback=() ;;
      restart) comeback=(-rejoin-after 2s) ;;
    esac
    make reset-topic TOPIC=exp14.payments
    log="$here/results/$membership-$departure-$stamp.log"
    {
      echo "exp-14b — a member leaves, membership: $membership, then: $departure"
      echo "date: $stamp"
      echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
      echo
      "$binary" -strategy cooperative -join-after 10s -leave-after 25s -duration 45s \
        ${identity[@]+"${identity[@]}"} ${comeback[@]+"${comeback[@]}"}
    } 2>&1 | tee "$log"
  done
done

echo "written to results/{dynamic,static}-{gone,restart}-$stamp.log"
