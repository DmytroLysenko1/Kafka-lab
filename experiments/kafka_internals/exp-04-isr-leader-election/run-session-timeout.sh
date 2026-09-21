#!/usr/bin/env bash
# exp-04c — the run that decides whether exp-04's explanation is right.
#
# exp-04 measures how long the cluster takes to notice a crashed broker and argues the
# figure is governed by broker.session.timeout.ms (9 s) rather than replica.lag.time.max.ms
# (30 s). Nothing in that run varies either timer, so it is an argument. This one raises the
# session timeout and measures again: if the explanation holds, the reaction moves with it.
#
# Usage: make exp-04c
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
stamp="$(date +%Y-%m-%d-%H%M%S)"
log="$here/results/exp-04c-$stamp.log"
records="${RECORDS:-300}"
raised="${SESSION_TIMEOUT_MS:-20000}"

now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }
compose() { docker compose -f "$repo/deploy/docker-compose.yml" "$@"; }

# Whatever happens, put the stand back: the default timeout and every broker running.
restore() {
  compose up -d --wait >/dev/null 2>&1 || true
  make -C "$repo" elect-preferred >/dev/null 2>&1 || true
}
trap restore EXIT

cd "$repo"
binary="$(mktemp -d)/exp04"
go build -o "$binary" ./experiments/kafka_internals/exp-04-isr-leader-election
exp04() { "$binary" -records "$records" "$@"; }

# Recreating the containers keeps the named volumes, so the topic and its data survive.
echo "raising broker.session.timeout.ms to $raised and recreating the brokers"
KAFKA_BROKER_SESSION_TIMEOUT_MS="$raised" compose up -d --wait >/dev/null
make exp-topics EXP="${here#"$repo"/experiments/}" >/dev/null
make elect-preferred >/dev/null

{
  echo "exp-04c — does the reaction to a crash move with broker.session.timeout.ms?"
  echo "date: $stamp"
  echo "client: franz-go $(go list -m github.com/twmb/franz-go | awk '{print $2}')"
  echo

  # The timers as the broker itself now reports them: the setting is evidence, not intent.
  echo "timers the broker is enforcing"
  docker exec kafka-lab-kafka1 /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server localhost:9092 --entity-type brokers --entity-name 1 --describe --all 2>/dev/null |
    grep -E "^  (broker\.session\.timeout\.ms|broker\.heartbeat\.interval\.ms|replica\.lag\.time\.max\.ms)="
  echo

  baseline="$(exp04 -phase baseline)"
  printf '%s\n\n' "$baseline"

  victim="$(printf '%s\n' "$baseline" | awk '/^LEADER_P0/ {print $2}')"
  case "$victim" in
    1|2|3) ;;
    *) echo "exp-04c: could not read the leader of partition 0 from the baseline"; exit 1 ;;
  esac

  trap 'docker start "kafka-lab-kafka$victim" >/dev/null 2>&1 || true' EXIT

  echo "killing kafka$victim, the broker leading partition 0"
  docker kill "kafka-lab-kafka$victim" >/dev/null
  killed="$(now_ms)"
  echo

  exp04 -phase degraded -since "$killed" -timeout 3m
  echo

  echo "starting kafka$victim again"
  docker start "kafka-lab-kafka$victim" >/dev/null
  restarted="$(now_ms)"
  echo

  exp04 -phase recovered -since "$restarted" -timeout 3m
} 2>&1 | tee "$log"

rm -rf "$(dirname "$binary")"
echo "written to ${log#"$repo"/}"
echo "restoring the default session timeout"
