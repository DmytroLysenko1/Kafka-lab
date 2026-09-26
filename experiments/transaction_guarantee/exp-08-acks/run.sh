#!/usr/bin/env bash
# exp-08 — what `acks` actually promises, in two halves.
#
#   acks=1   the leader answers alone. The followers are paused before the write, so the
#            window between the acknowledgement and replication is held open on purpose,
#            and the leader is killed inside it.
#   acks=all with min.insync.replicas=3 and a broker down: refused outright, not lost.
#
# Usage: make exp-08
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
records="${RECORDS:-2000}"
record_bytes="${RECORD_BYTES:-4096}"

cd "$repo"

# Any killed broker goes back whatever happens, and leadership goes back with it: without
# that, every later `make exp-NN` is blocked behind `make check`.
# A preferred election run while the restarted broker is still catching up moves nothing
# onto it, and the next `make exp-NN` then fails `make check` on leadership. So the
# election waits until no partition in the cluster is under-replicated — bounded, because a
# clean-up that can hang forever is worse than one that gives up and says so.
victim=""
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
  [ -n "$victim" ] && docker start "kafka-lab-kafka$victim" >/dev/null 2>&1 || true
  for _ in $(seq 120); do settled && break; sleep 1; done
  settled || echo "exp-08: the cluster was still under-replicated after two minutes; run make elect-preferred by hand" >&2
  make elect-preferred >/dev/null 2>&1 || true
  return 0
}
trap restore EXIT

make exp-topics EXP="${here#"$repo"/experiments/}"
for topic in exp08.acks1 exp08.isr3 exp08.all2; do make reset-topic TOPIC="$topic"; done

binary="$(mktemp -d)/exp08"
go build -o "$binary" ./experiments/transaction_guarantee/exp-08-acks
exp08() { "$binary" -records "$records" -record-bytes "$record_bytes" "$@"; }

leader_of() { exp08 -topic "$1" -phase leader | awk '/^LEADER_P0/ {print $2}'; }
require_broker() { case "$1" in 1|2|3) ;; *) echo "exp-08: could not read the leader of $2"; exit 1 ;; esac; }

# ---------------------------------------------------------------- half one: acks=1
victim="$(leader_of exp08.acks1)"
require_broker "$victim" exp08.acks1
session_timeout_ms="$(docker exec "kafka-lab-kafka$victim" printenv KAFKA_BROKER_SESSION_TIMEOUT_MS)"
followers=""
for id in 1 2 3; do [ "$id" != "$victim" ] && followers="$followers $id"; done

# Killing the leader after the write proves nothing: on a local network the followers have
# every record within milliseconds, so the window is closed before any kill lands — the
# first version of this run read "lost 0" and called it luck. Pausing both followers holds
# the window open. They stay in the in-sync set, because shrinking it needs the controller
# quorum and two of the three combined nodes are frozen, so when they are thawed one of them
# is elected cleanly — with none of the records the dead leader acknowledged. With dedicated
# controllers the quorum would stay up and fence a follower silent for longer than
# broker.session.timeout.ms, so the pause is timed: under that timeout, a live quorum would
# have left the followers in the set too, and the result does not depend on this stand.
log="$here/results/acks-one-$stamp.log"
{
  echo "exp-08a — what acks=1 acknowledges, and whether it survives"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo "records: $records of $record_bytes bytes, uncompressed"
  echo

  exp08 -topic exp08.acks1 -phase leader
  echo

  echo "pausing the followers:$followers"
  paused_at="$(date +%s)"
  for id in $followers; do docker pause "kafka-lab-kafka$id" >/dev/null; done
  # Only the leader is reachable, so it is the only seed: a metadata request sent to a
  # frozen broker would hang instead of failing.
  written="$(exp08 -topic exp08.acks1 -phase write-acks-one -brokers "localhost:${victim}9092" -timeout 1m)"
  echo "$written"
  acknowledged="$(awk '/^acknowledged/ {print $2}' <<<"$written")"
  echo

  echo "killing kafka$victim, the leader that acknowledged them, then thawing the followers"
  docker kill "kafka-lab-kafka$victim" >/dev/null
  for id in $followers; do docker unpause "kafka-lab-kafka$id" >/dev/null; done
  echo "followers paused for $(( $(date +%s) - paused_at ))s, against a broker.session.timeout.ms of ${session_timeout_ms}ms"
  exp08 -topic exp08.acks1 -phase await-shrunk -timeout 3m
  echo

  exp08 -topic exp08.acks1 -phase leader
  echo
  exp08 -topic exp08.acks1 -phase count -acknowledged "$acknowledged" -timeout 3m
} 2>&1 | tee "$log"

# The restarted broker has to be back in the in-sync set before the next half asks it
# anything: a booting broker answers an admin call by closing the connection.
docker start "kafka-lab-kafka$victim" >/dev/null
exp08 -topic exp08.acks1 -phase await-full -timeout 3m
victim=""

# ---------------------------------------------------------------- half two: acks=all
victim="$(leader_of exp08.isr3)"
require_broker "$victim" exp08.isr3

log="$here/results/acks-all-$stamp.log"
{
  echo "exp-08b — acks=all with min.insync.replicas=3 refuses instead of losing"
  echo "date: $stamp"
  echo

  exp08 -topic exp08.isr3 -phase leader
  echo
  echo "every broker up:"
  exp08 -topic exp08.isr3 -phase write-acks-all
  echo
  # Counted before the kill: once the in-sync set is short of min.insync.replicas the
  # leader answers offset requests with OFFSET_NOT_AVAILABLE, and the interesting number
  # here is the refusal, not the log.
  exp08 -topic exp08.isr3 -phase count -timeout 1m
  echo

  echo "killing kafka$victim, dropping the in-sync set below min.insync.replicas"
  docker kill "kafka-lab-kafka$victim" >/dev/null
  exp08 -topic exp08.isr3 -phase await-shrunk -timeout 3m
  echo

  echo "one broker down:"
  exp08 -topic exp08.isr3 -phase write-acks-all
} 2>&1 | tee "$log"

# The restarted broker has to rejoin before the third cell kills a leader again.
docker start "kafka-lab-kafka$victim" >/dev/null
exp08 -topic exp08.isr3 -phase await-full -timeout 3m
victim=""

# ---------------------------------------------------------------- cell three: acks=all, the same failure
# The acks=1 cell's failure step for step — followers paused, write, leader killed,
# followers thawed — with only the producer's acks changed, and min.insync.replicas 2 as in
# the catalog. This is the comparison the first two cells cannot make: they are different
# failures.
victim="$(leader_of exp08.all2)"
require_broker "$victim" exp08.all2
followers=""
for id in 1 2 3; do [ "$id" != "$victim" ] && followers="$followers $id"; done

log="$here/results/acks-all-paused-$stamp.log"
{
  echo "exp-08c — acks=all under the acks=1 cell's failure: what is acknowledged, and what survives"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo "records: $records of $record_bytes bytes, uncompressed"
  echo

  exp08 -topic exp08.all2 -phase leader
  echo

  echo "pausing the followers:$followers"
  paused_at="$(date +%s)"
  for id in $followers; do docker pause "kafka-lab-kafka$id" >/dev/null; done
  written="$(exp08 -topic exp08.all2 -phase write-acks-all-bounded -brokers "localhost:${victim}9092" -timeout 1m)"
  echo "$written"
  acknowledged="$(awk '/^acknowledged/ {print $2}' <<<"$written")"
  echo

  echo "killing kafka$victim, the leader that held the records, then thawing the followers"
  docker kill "kafka-lab-kafka$victim" >/dev/null
  for id in $followers; do docker unpause "kafka-lab-kafka$id" >/dev/null; done
  echo "followers paused for $(( $(date +%s) - paused_at ))s, against a broker.session.timeout.ms of ${session_timeout_ms}ms"
  exp08 -topic exp08.all2 -phase await-shrunk -timeout 3m
  echo

  exp08 -topic exp08.all2 -phase leader
  echo
  exp08 -topic exp08.all2 -phase count -acknowledged "$acknowledged" -timeout 3m
} 2>&1 | tee "$log"

rm -rf "$(dirname "$binary")"
echo "written to ${log#"$repo"/} and its acks-one and acks-all siblings"
