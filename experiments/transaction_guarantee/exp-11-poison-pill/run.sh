#!/usr/bin/env bash
# exp-11 — one record nobody can decode, with and without somewhere to put it.
#
# Two cells over the same content: 100 good records with one undecodable record after the
# tenth. The consumer is the service's own; the only difference between the cells is
# whether it has a dead letter topic. Cell A is what a service looks like when it does not.
#
# Usage: make exp-11
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
records="${RECORDS:-100}"
poison_at="${POISON_AT:-10}"
budget="${BUDGET:-30s}"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"

# The experiment owns the logs it reads: records left by an earlier run would be replayed
# into this one's counts, and a dead letter topic carrying yesterday's record would be
# counted as today's.
make reset-topic TOPIC=exp11.blocked
make reset-topic TOPIC=exp11.archived
make reset-topic TOPIC=exp11.archived.dlq

# The inbox and the projection are the experiment's evidence, so they start empty. Each
# cell addresses its own merchant and its own range of event ids, so neither can be
# mistaken for the other — or deduplicated against it.
make migrate
docker exec -i kafka-lab-postgres psql -U lab -d lab -q -c \
  "DELETE FROM merchant_totals WHERE merchant_id LIKE 'exp11-%';
   DELETE FROM inbox WHERE event_id LIKE 'exp11-%' OR event_id BETWEEN '110000' AND '120099';"

binary="$(mktemp -d)/exp-11"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-11-poison-pill

{
  echo "exp-11 — a record nobody can decode, with and without a dead letter topic"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo "records: $records good, one undecodable after $poison_at of them; budget per cell: $budget"
  echo

  echo "== cell A: no dead letter route =="
  "$binary" -phase produce -topic exp11.blocked -merchant exp11-blocked -first-event-id 110000 \
    -records "$records" -poison-at "$poison_at"
  "$binary" -phase consume -topic exp11.blocked -group exp11-blocked -merchant exp11-blocked \
    -records "$records" -poison-at "$poison_at" -budget "$budget"
  echo

  echo "== cell B: the same records, archived to a dead letter topic =="
  "$binary" -phase produce -topic exp11.archived -merchant exp11-archived -first-event-id 120000 \
    -records "$records" -poison-at "$poison_at"
  "$binary" -phase consume -topic exp11.archived -dlq-topic exp11.archived.dlq -group exp11-archived \
    -merchant exp11-archived -records "$records" -poison-at "$poison_at" -budget "$budget"
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
