#!/usr/bin/env bash
# exp-18 — what the retry chain buys when one merchant's row is held by another transaction.
#
# Three cells over the same shape: 600 payments over six partitions, every hundredth one
# for a merchant whose row a second transaction holds FOR UPDATE. The consumer is the
# service's own; so are the detours and the replay.
#   A — no chain: contention looks like any other database failure, so the consumer holds
#       the offset and restarts until the lock is gone.
#   B — the chain, with the lock released after HOLD.
#   C — the chain, with the lock held until every contended payment has been archived;
#       then it is released and the dead letters are replayed.
#
# Usage: make exp-18
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
hold="${HOLD:-30s}"
budget="${BUDGET:-3m}"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"

# The experiment owns the logs it reads: a tier holding an earlier run's copies would be
# counted into this one, and its consumer would try to handle them.
for topic in "$here"/topics/*.yaml; do
  make reset-topic TOPIC="$(basename "$topic" .yaml)"
done

# The inbox and the totals are the evidence, so they start empty. Each cell has its own
# merchants and its own range of event ids, from 9.180e15 up — far past anything the outbox,
# whose row ids are the service's event ids, will reach in the shared inbox (see exp-11).
make migrate
docker exec -i kafka-lab-postgres psql -U lab -d lab -q -c \
  "DELETE FROM merchant_currency_totals WHERE merchant_id LIKE 'exp18-%';
   DELETE FROM inbox WHERE event_id ~ '^918[0-2]0{9}[0-9]{3}$';"

binary="$(mktemp -d)/exp-18"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-18-retry-chain

{
  echo "exp-18 — contention on one merchant, with and without the retry chain"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo "lock wait before a payment counts as contended: 2s (postgres.lockWait); tiers 3s / 6s / 12s"
  echo "hold in cells A and B: $hold; budget per cell: $budget"
  echo

  echo "== cell A: no chain =="
  "$binary" -cell a -hold "$hold" -first-event-id 9180000000000000 -budget "$budget"
  echo

  echo "== cell B: the chain, lock released after $hold =="
  "$binary" -cell b -hold "$hold" -first-event-id 9181000000000000 -budget "$budget"
  echo

  echo "== cell C: the chain, lock held until every contended payment is archived, then replayed =="
  "$binary" -cell c -first-event-id 9182000000000000 -budget "$budget"
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
