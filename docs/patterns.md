# Patterns and anti-patterns, each with the run that paid for it

Every pattern here points at a case file or an experiment in this repository. Every
anti-pattern points at the run where the damage is a number. Where nothing was run, the
section says so in its own words rather than borrowing confidence from the ones that were:
three of the sections below — CDC, stream enrichment and the retry chain's own experiment —
are reasoning, not measurement, and are marked.

The recommendations at the end are for the services we actually build: payments, where a
lost event is money and a duplicated one is a second charge.

---

## 1. What goes inside the event

### Event notification versus event-carried state transfer

A **notification** carries an id and a type: `payment.captured`, `pay-0007`. The consumer
calls back to learn the rest. A **state transfer** carries the payment — amount, currency,
merchant, status — and the consumer needs nobody.

| | Notification | State transfer |
|---|---|---|
| Event size | small | the aggregate, every time |
| Consumer needs the producer's API | yes, on every event | no |
| Producer can change its internals freely | yes | no — the event is now a contract ([case 09](static/09-schema-evolution.md)) |
| Replay rebuilds a read model | no, the API only answers *now* | yes, if the topic is compacted |
| Load on the producer | one call per event per consumer | none |

**Compaction is what makes the second one replayable, and it is measured.** A compacted
topic keeps the last value per key: exp-03 wrote 2 010 records over 40 keys and the cleaner
left 50 — the 40 live values plus 10 tombstones
([run](../experiments/kafka_internals/exp-03-segments-retention/results/run-2026-09-20-200653.log)).
That is the difference between a topic that is a log of what happened and a topic that is
also the current state.

**For payments:** notification across team boundaries, where the producer must stay free to
change; state transfer where a consumer maintains a read model or a projection it has to be
able to rebuild. In both cases the key is `payment_id`, for the reason in
[case 01](static/01-write-path.md) — the key is the ordering unit, not a formality.

### Stream enrichment — argued, not run

Joining a payment with reference data (the merchant's country, its risk tier) has two
shapes. Either the consumer **looks the merchant up** when the event arrives, or the
merchant table is **published as a compacted topic** and the consumer keeps a local copy.

The lookup is simpler and couples the consumer's availability to the lookup service's:
exp-16 measured what a dependency outage does to a consumer that waits on it — the group
removed the member after the rebalance timeout and its stale commit later rewound a
partition by 1 726–1 732 records
([exp-16](../experiments/transaction_guarantee/exp-16-lag-backpressure/)). The local copy
removes that dependency and pays for it in a second topic to operate and in staleness
between the update and the join.

No enrichment is implemented in this repository, and no run here measures a join. What is
measured is the failure mode the lookup shape inherits.

---

## 2. Atomicity: the database and the topic

### Transactional outbox

**The Kafka transaction stops at Kafka**, and that is measured rather than asserted: in
exp-10b a crash mid-transaction left the Kafka output at exactly 1 000 records and the
Postgres rows at 1 050 — the abort discarded the records and did nothing to the database
([run](../experiments/transaction_guarantee/exp-10b-database-boundary/results/run-2026-09-22-022201.log)).
Anything that must be true of both a row and an event therefore goes through an outbox:
the event is written to a table in the same transaction as the state, and a relay publishes
it afterwards. The mechanics, including what the relay must get right, are
[case 06](static/06-transactional-outbox.md).

What it costs: one more table, one more moving part, and publication that is at-least-once
by construction — the relay can publish and die before marking the row. That duplicate is
not a defect to be designed away; it is the reason for the inbox below.

### CDC as the same pattern with a different reader — argued, not run

Debezium reads the database's replication log instead of an outbox table, which removes the
relay and the table at the price of an operational surface: a Connect cluster, a replication
slot that will fill the disk if the connector stops, and the table's own schema becoming a
public contract. [Case 06](static/06-transactional-outbox.md) has the comparison. Nothing
here runs Debezium; the PDP names CDC, and this is where the trade lives.

### Idempotent consumer (inbox)

Delivery is at-least-once, so the consumer has to make the *effect* exactly once. exp-06 and
exp-07 are the same crash twice: without an inbox the replayed batch charged 50 payments a
second time; with the claim and the write in one transaction the table held 1 000 rows and
the inbox refused exactly 50 redeliveries
([exp-07](../experiments/transaction_guarantee/exp-07-inbox/results/run-2026-09-21-234218.log)).
*Exactly-once effect, at-least-once delivery* — the distinction worth taking from this whole
repository.

---

## 3. Failure handling that does not stop the partition

### Retry topics and a dead letter queue

A record that fails for its own reasons — a declined card, a malformed field — must not hold
the partition behind it. It moves to a retry topic with a delay, and after the chain is
exhausted to a DLQ. [Case 07](static/07-retry-dlq.md) has the shape, including two
non-obvious parts: the retry topics run on `LogAppendTime`, or a forwarded record keeps its
original timestamp and its delay evaluates to zero; and the chain is for single failures,
not for an outage.

exp-11 — a poison record straight to the DLQ against a partition stuck retrying it forever —
is listed in the plan and has not been run. This section is the design, not a measurement.

### What blocking instead costs, measured

Waiting inside the handler is the alternative everyone writes first, and exp-16 priced it
against pausing: through a 20 s dependency outage the lag was identical either way, but the
blocking handler held the rebalance, the group removed the member after the 8 s rebalance
timeout, its next accepted commit rewound a partition by 1 726–1 732 records, and one run in
three handled 1 739 records twice. The handler that paused fetching and rewound to its first
unhandled record showed none of it
([exp-16](../experiments/transaction_guarantee/exp-16-lag-backpressure/)).

**Backpressure is choosing the rate at which work arrives**, not sleeping in the handler:
`PauseFetchPartitions` plus a rewind, fetch sizing, bounded handler concurrency, a breaker
around the dependency. Section 6 of the [tuning checklist](tuning-checklist.md) lists them.

---

## 4. Anti-patterns, each with its number

| Anti-pattern | What it costs, measured | Run |
|---|---|---|
| Assuming a global order across partitions | 6 827 – 8 721 of 10 000 events handled out of their business order, every payment split across partitions; keyed: 0, every run | [exp-01](../experiments/kafka_internals/exp-01-partition-keys/) |
| A key that concentrates traffic (hot partition) | 85% of records on one partition; seven consumers drained it 15% faster than one, and the seventh handled nothing | [exp-02](../experiments/kafka_internals/exp-02-hot-partition/) |
| Retention as an afterthought | a 5 s retention left 0 of 2 000 records readable; compaction is the opposite lever and left 50 of 2 010 | [exp-03](../experiments/kafka_internals/exp-03-segments-retention/) |
| Committing the offset before the work | 49 of 1 000 payments gone with no error anywhere; with `GreedyAutoCommit`, the unwritten rest of a batch — 38 and 41 | [exp-05](../experiments/transaction_guarantee/exp-05-at-most-once/), [exp-05b](../experiments/transaction_guarantee/exp-05b-autocommit-greedy/) |
| `acks=1` on a topic that carries money | 2 000 records acknowledged, 0 readable after the leader died inside the replication window | [exp-08](../experiments/transaction_guarantee/exp-08-acks/) |
| Turning idempotence off to "go faster" | 80–291 duplicates per run, each one a record the leader had appended before answering `REQUEST_TIMED_OUT` | [exp-09](../experiments/transaction_guarantee/exp-09-reordering/) |
| Blocking retry inside the handler | member removed from the group, one commit refused, a partition rewound 1 726–1 732 records, 1 739 handled twice | [exp-16](../experiments/transaction_guarantee/exp-16-lag-backpressure/) |
| Leaving a transaction open | `read_committed` consumers saw nothing for 23.2 s behind a 20 s transaction timeout — lag with no messages, on a partition whose producer had nothing to do with the transaction | [exp-10d](../experiments/transaction_guarantee/exp-10d-hanging-transaction/) |
| Treating a Kafka transaction as covering the database | Kafka exactly 1 000, Postgres 1 050 | [exp-10b](../experiments/transaction_guarantee/exp-10b-database-boundary/) |
| Porting a Java config to franz-go unchanged | the defaults differ where it matters: one in-flight request instead of five, snappy instead of none, 10 ms of linger instead of 5, `AtStart` instead of `latest` | [checklist §7](tuning-checklist.md) |

---

## 5. What to do in our services

| Situation | Do this | Why, with the number |
|---|---|---|
| An event must accompany a database write | outbox in the same transaction, relay afterwards | a Kafka transaction does not reach the database: 1 000 against 1 050 (exp-10b) |
| A consumer writes anything that must not happen twice | inbox: claim and write in one transaction | 50 redeliveries refused, 1 000 rows (exp-07) |
| Events of one entity must stay in order | key by the entity id; never rely on order across keys | keyless: up to 8 721 violations per 10 000; keyed: 0 (exp-01) |
| A single tenant dominates the traffic | split the key or route that tenant to its own topic | adding consumers bought 15% (exp-02) |
| A payment topic's durability | `acks=all` **and** `min.insync.replicas=2`; idempotence on | `acks=1` lost 2 000 acknowledged records (exp-08); at min.isr 1 `acks=all` means one machine |
| One record keeps failing | retry topics on `LogAppendTime`, then DLQ | a `CreateTime` retry topic evaluates a forwarded record's delay as zero ([case 07](static/07-retry-dlq.md)) |
| A dependency is down for everybody | pause fetching, rewind, keep polling — do not wait in the handler | the blocking handler lost its membership and rewound the group (exp-16) |
| A consumer group is deployed several times a day | `group.instance.id`, session timeout sized to cover a restart | static: a restart costs 2.05 s and no rebalance; a death costs the whole session timeout (exp-14b) |
| Throughput matters more than a few milliseconds | linger and a codec; zstd on full batches | compression raised throughput at saturation and the ratio is set by batching, 2.9× to 5.65× (exp-17, exp-17b) |

---

## What this document does not claim

- **CDC and stream enrichment** are reasoned from the mechanics, not run here.
- **The retry chain itself** (exp-11) and **schema evolution** (exp-12,
  [case 09](static/09-schema-evolution.md)) are designed and written up; neither has a run
  behind it yet.
- The anti-pattern table reports what these experiments measured on a three-broker stand on
  one laptop. The shapes transfer; the magnitudes are the stand's.
