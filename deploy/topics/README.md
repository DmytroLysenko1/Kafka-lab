# Topic catalog

Every topic the stand keeps permanently, one YAML file each, applied by
[topicctl](https://github.com/segmentio/topicctl) with `make topics` and checked with
`make check`. This page records *why* each number is what it is — the YAML says what, this
says why.

## The catalog

| Topic | Purpose | Partitions | RF | min.isr | Retention | Timestamp |
|---|---|---|---|---|---|---|
| `payments.main` | payment events, key `payment_id` | 6 | 3 | 2 | 7 days | CreateTime |
| `payments-consumer.retry.5s` | retry tier 1 of group `payments-consumer` | 6 | 3 | 2 | 7 days | LogAppendTime |
| `payments-consumer.retry.1m` | retry tier 2 | 6 | 3 | 2 | 7 days | LogAppendTime |
| `payments-consumer.retry.10m` | retry tier 3 | 6 | 3 | 2 | 7 days | LogAppendTime |
| `payments-consumer.dlq` | dead letters of the group, bytes verbatim | 6 | 3 | 2 | 30 days | LogAppendTime |

All five declare `unclean.leader.election.enable=false` explicitly, so the setting is
visible in the files rather than inherited from a broker default. The `true` contrast is
exp-04b, which this stand cannot run: collapsing an ISR onto a stale replica needs two of
three combined broker/controller nodes dead, and that takes the KRaft quorum with it.

## Why these numbers

**Six partitions on `payments.main`.** It is the smallest count that does three jobs at
once. It is a multiple of the three brokers, so each broker leads two partitions and a dead
broker moves exactly two leaderships per topic — what exp-04 and exp-13 read off the
table. It divides evenly among 1, 2, 3 and 6 consumers, while four split 2/2/1/1 (exp-14)
and a seventh sits idle (exp-02: parallelism is capped by partitions, not by pods). Three
could not show either of those; twelve shows nothing new. In production the count comes
from load instead — `max(throughput / per-partition produce rate, throughput /
per-partition consume rate)` plus headroom — and the headroom is generous because
partitions can only be added, and adding them moves existing keys to other partitions.

**Six partitions on the retry tiers and the DLQ.** Not for capacity: in normal operation the
retry flow is a small fraction of main, and a systemic outage is not meant to reach the
chain at all — a circuit breaker pauses the main consumer instead ([07](../../docs/static/07-retry-dlq.md)).
Ordering does not need it either: the key survives every hop, so one payment's events stay
in one partition of a tier whatever the count. The same count keeps partition numbers
aligned — p3 of main retries into p3 of each tier — which makes a failure easy to follow
by hand, and on a lab stand it costs nothing.

**RF 3 and `min.insync.replicas=2` on every topic, the DLQ included.** Once the main offset
is committed, the record in a retry tier or in the DLQ is the only copy of that payment in
flight; losing it is losing the payment. Two in-sync replicas means an acknowledged write
survives the loss of one broker; three would make every broker failure stop writes, which
is exp-08's lesson and deliberately not the catalog's setting.

**Retention.**
- `payments.main`, 7 days: Postgres is the system of record, the topic is transport plus a
  week of replay buffer.
- Retry tiers, 7 days: not the delay but how long the retry consumer may be down before a
  payment in flight is deleted. A ten-minute retention on the ten-minute tier would turn a
  ten-minute outage of that consumer into lost payments.
- DLQ, 30 days: the window for fix, release and replay. No `retention.bytes`: a storm of
  poison records must not push payments out by size. The alert that matters is the age of
  the oldest unreplayed letter.

Every topic has a finite, stated retention, which is the catalog's answer to *unbounded
retention*: nothing here grows forever by accident. The one legitimately unbounded shape,
a compacted state topic, is bounded by its number of keys — and a compacted per-payment
state topic without tombstones after the terminal status would be exactly the anti-pattern.

**Timestamp type.** The retry tiers and the DLQ run on `LogAppendTime`. The retry consumer
forwards the record it received, and a forwarded record keeps the original event's
timestamp; on `CreateTime` the delay would already be over on arrival. On the DLQ the same
setting makes retention count from the letter's death instead of the event's birth. The
original event time travels in a header. `payments.main` is pinned to `CreateTime` so the
event time is the producer's.

## Declared, and deliberately left at the default

| Setting | Decision | Why |
|---|---|---|
| `min.insync.replicas` | declared on every topic | Kafka 4.x keeps a cluster-wide dynamic default for it, and topicctl reads that as the topic's own value — leaving it out reports drift forever. `make check` refuses a YAML without it |
| `retention.ms`, `message.timestamp.type`, `unclean.leader.election.enable` | declared | each decides the correctness of something above; a broker default can change under them |
| `compression.type` | default, `producer` | a topic-level codec makes the broker recompress every batch whose codec differs, which would distort exp-17's throughput and CPU numbers |
| `cleanup.policy` | default, `delete` | none of these topics is state; compaction belongs to exp-03 |
| `max.message.bytes` | default, ~1 MiB | a dead letter is the original record plus headers; error text is truncated to a kilobyte instead of raising the limit |
| segment settings | default | only exp-03 needs small segments, on its own topic |

## Naming

- `<topic>` for a stream of events, `<consumer group>.retry.<delay>` and
  `<consumer group>.dlq` for a group's failure path. A retry is a second attempt by one
  handler, so the chain belongs to the group; a second group on `payments.main` gets its
  own chain and never reads another group's failures.
- The delay is part of the name on purpose: a different delay is a different topic.
- Dots, never underscores — Kafka warns that `.` and `_` collide in metric names, and a
  Kubernetes resource name (Strimzi `KafkaTopic`) does not allow `_`.

## Experiments and their topics

An experiment that reshapes a topic, or fills it with test traffic, gets its own YAML in
`experiments/<exp>/topics/`, applied with `make exp-topics EXP=<exp>`. The catalog stays
the shape the service runs on, and `make check` keeps checking only that.

| Experiment | Topic | Why not the catalog |
|---|---|---|
| exp-01 keys and ordering | own, keyless and keyed, 6 partitions | 10 000 test events would sit in `payments.main` for a week and be consumed by the service |
| exp-02 hot partition | own, six partitions fed a deliberately skewed key | needs a key that concentrates load, which `payments.main` must not have |
| exp-03 segments and compaction | own, `cleanup.policy=compact`, small segments | reshapes the log itself |
| exp-04 ISR and leader election | own, 3 partitions so a single kill is easy to read | gets killed under; the catalog topics should not |
| exp-05…07 delivery semantics | own | counted duplicates and losses must not mix with any other traffic |
| exp-08 acks and min.insync | own, `min.insync.replicas=3` | one dead broker has to drop the ISR below the threshold without losing the KRaft quorum ([04](../../docs/static/04-isr-leader-election.md)) |
| exp-09 producer idempotence | own | runs a deliberately unsafe producer |
| exp-10 transactions | own input plus `payments.enriched` | the EOS stage is group `payments-enricher`, not part of the service |
| exp-12 schema evolution | own | deliberately incompatible versions would stay in the history of `payments.main-value` |
| exp-15 partition reassignment | own | changes replica placement and the replication factor |
| exp-17 batching and compression | own | a load sweep, not payment traffic |
| exp-11, 13, 14, 16 | the catalog | they measure the service on the topics it actually runs on: poison into the DLQ, a broker killed under load, a rebalance, a lag spike |

## Changing the catalog

- **Partitions** only grow, and growing them moves existing keys: events of one payment
  before and after the change land in different partitions. Treat it as a migration, not
  as a one-line diff — exp-02 shows the cost.
- **Replication factor** is a reassignment with throttling (exp-15), which topicctl
  declines to do — it refuses outright once the observed ISR no longer matches the YAML.
  exp-15 measured the move: 60 MiB onto a third replica took 2.2–2.5 s unthrottled and
  24.1–24.8 s at 1 MiB/s, which is the per-broker rate rather than the cluster's. Producer
  latency did not move either way on this stand. `--verify` is not optional: it is what
  removes the throttle `--execute` set.
- **Settings** change in the YAML, then `make topics`. A setting added by hand is reported
  by `make check`; `make reset-topic TOPIC=<name>` drops the topic and recreates it from
  its YAML, log included.
