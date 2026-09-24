#!/usr/bin/env bash
# exp-12 — schema evolution: what the registry allows, and what the bytes do anyway.
#
# Two halves. The registry half asks three changes at three compatibility settings and
# records which are let through. The wire half writes the same three changes as records and
# has a consumer built against the schema before them read every one.
#
# Usage: make exp-12
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/run-$stamp.log"
budget="${BUDGET:-20s}"

cd "$repo"
make exp-topics EXP="${here#"$repo"/experiments/}"

# The experiment owns the logs it reads.
make reset-topic TOPIC=exp12.evolution
make reset-topic TOPIC=exp12.evolution.dlq

# The inbox decides what counts as already handled, so a rerun starts with this run's
# events gone from it.
make migrate
docker exec -i kafka-lab-postgres psql -U lab -d lab -q -c \
  "DELETE FROM inbox WHERE event_id LIKE 'exp12-%';
   DELETE FROM merchant_totals WHERE merchant_id = 'exp12';"

binary="$(mktemp -d)/exp-12"
trap 'rm -rf "$(dirname "$binary")"' EXIT
go build -o "$binary" ./experiments/transaction_guarantee/exp-12-schema-evolution

{
  echo "exp-12 — schema evolution: what the registry allows, and what the bytes do anyway"
  echo "date: $stamp"
  echo "registry: $(curl -sS "${SCHEMA_REGISTRY_URL%/apis/*}/apis/registry/v3/system/info" | sed 's/.*"version":"\([^"]*\)".*/Apicurio \1/')"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo

  echo "== what the registry lets through =="
  "$binary" -phase registry
  echo

  echo "== what a v1 consumer does with each of them =="
  "$binary" -phase produce -budget "$budget"
  "$binary" -phase consume -budget "$budget"
} 2>&1 | tee "$log"

echo "written to ${log#"$repo"/}"
