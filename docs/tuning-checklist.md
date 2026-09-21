# Producer and consumer tuning checklist

**The question this file answers:** for each setting that matters — what does it buy, what
does it cost, and when do you reach for it?

KR2. Every franz-go default here was read out of `pkg/kgo/config.go` at v1.22.0, not
remembered; the Java column is from the Kafka 4.x documentation. Numbers that can only
come from a run are marked `TBD (exp-NN)` until that run exists — a tuning guide with
invented figures is worse than none.

The column that matters most is the third one. A setting is not "recommended" or
"optimal": it buys something and charges for it, and the charge is what you have to be
able to name.

## 1. Durability: what survives a broker dying

| Setting | Default | When to change it, and what it costs |
|---|---|---|
| `acks` (`kgo.RequiredAcks`) | `all` in Java since 3.0, `AllISRAcks` in franz-go | Never lower it on payments. `acks=1` returns before any follower has the record, so a leader crash inside the replication window loses an acknowledged write — rarely, silently, which is worse than often. Lowering it buys latency and pays in invisible loss (exp-08) |
| `min.insync.replicas` | **1** — a topic setting, not a producer one | Set it to 2 on every topic that holds money. At 1, `acks=all` means "all of the one replica left in the ISR" and protects nothing. The cost is availability: with RF 3 and min.isr 2, losing two brokers stops writes — the write fails loudly instead of vanishing (exp-08) |
| `enable.idempotence` (`kgo.DisableIdempotentWrite` to turn off) | on in both | Leave it on. It removes duplicates from producer-side retries and keeps batches in order within a partition: exp-09 ran 266 980 events through three refusal windows with it on and read back exactly that many, zero duplicated and zero out of order. It holds only within one producer session — a process restart needs a `transactional.id` to keep the guarantee, which no run here measures yet |
| `unclean.leader.election.enable` | `false` | Leave it false. True converts an offline partition into a promoted out-of-sync replica: availability bought with acknowledged records that are then truncated away (exp-04b) |
| `retries` / `recordRetries` | Java `retries=MAX_INT` bounded by `delivery.timeout.ms` 120 s; franz-go record retries **unbounded**, request retries 20, backoff 250 ms → 5 s jittered | Bound the wall-clock, not the count: in franz-go that is `kgo.RecordDeliveryTimeout`. Unbounded retries turn a broker outage into unbounded latency and a full memory buffer instead of a visible error |

## 2. Ordering and parallelism

| Setting | Default | When to change it, and what it costs |
|---|---|---|
| record key | — | The key is the ordering unit: same key, same partition, order preserved. `payment_id` keeps one payment's events in order and spreads payments across partitions. A null key gives up ordering entirely (exp-01) |
| partitioner | Java `DefaultPartitioner` (sticky batching for null keys); franz-go `UniformBytesPartitioner(64 KiB, adaptive, keys)` | Leave it. Note the shape for experiments: with a null key franz-go stays on one partition until **64 KiB** have been written, so a small test run shows no disorder at all and invites a false conclusion (exp-01) |
| partition count | 6 here | Only grows, and growing moves existing keys to other partitions — one payment's history splits across a boundary. Size it for the consumer parallelism you will need, since it is the ceiling (exp-02, [`deploy/topics/README.md`](../deploy/topics/README.md)) |
| `max.in.flight.requests.per.connection` (`kgo.MaxProduceRequestsInflightPerBroker`) | **Java 5, franz-go 1** | The clients differ here. Raising franz-go to 5 buys throughput on a high-latency link; it is safe *only* with idempotence on, which keeps sequence numbers in order. **What it costs without idempotence is client-specific.** In Java the textbook failure is reordering — a retried batch lands after a later one. franz-go rewinds a partition to its oldest pending batch on a retriable error and drops to one request in flight until a success, which keeps the order and resends batches that had in fact landed: exp-09 produced 217 840 events through three refusal windows and read back 291 duplicates and zero out of order. Duplication is still a double charge; it is just not the failure the Java-shaped advice warns about |

## 3. Throughput against latency

| Setting | Default | When to change it, and what it costs |
|---|---|---|
| `linger.ms` (`kgo.ProducerLinger`) | **Java 5 ms since Kafka 4.0 (0 before), franz-go 10 ms** | Waiting fills batches: fewer, larger requests, better compression. **Linger is an upper bound on the added latency, not its price**: at 20 000 records/s exp-17 measured ~5 records per batch at 0, ~22 at 5 ms, ~34 at 10 ms, and a median of 2–3 ms, ~7 ms, ~8.5 ms respectively. franz-go's 10 ms against Java's 5 ms costs 1.5–2 ms of median and buys batches half as large again. Linger 0 has the lowest median and the least stable tail (p99 11–33 ms across cells and runs, against 11–19 ms at 5 ms): more, smaller requests queue more unevenly |
| `batch.size` (`kgo.ProducerBatchMaxBytes`) | **Java 16 KB, franz-go ≈1 MB (1 000 012 bytes)** | Raise with linger for bulk work; on its own it does nothing unless batches are actually filling. **It also caps linger**: at 50 ms a 16 KiB batch filled and shipped after ~12 ms (40 records), while the default ≈1 MB batch waited the linger out (~31 ms median, ~140 records). At 0–10 ms the two sizes measured the same, because no batch came near 16 KiB. Cost: memory per partition in flight (exp-17) |
| `compression.type` (`kgo.ProducerBatchCompression`) | **Java none, franz-go snappy then none** | **The ratio belongs to the batch, not the codec.** On the same payment payloads exp-17 measured zstd at 3.0× with linger 0, 4.7× at 5 ms, 5.1× at 10 ms and 5.6× at 50 ms; snappy, franz-go's default, at 2.1×, 2.8×, 2.95× and 3.3×. Small batches waste most of what any codec can do. At 5 MB/s the codec made no measurable difference to latency, so zstd — 44–85 bytes per 250-byte record — cost nothing here; its CPU price appears under saturation, which this sweep does not reach. Cost is producer CPU and, if the topic sets its own codec, broker recompression |
| `buffer.memory` / `kgo.MaxBufferedRecords` | franz-go 10 000 records | This is the producer's backpressure valve: once full, `Produce` blocks (or fails, with `kgo.ManualFlushing`). Raising it hides a slow broker for longer; lowering it surfaces the problem sooner. Never raise it to "fix" a stall |
| `kgo.ProduceRequestTimeout` / `request.timeout.ms` | franz-go 10 s produce timeout, 10 s request overhead | Lower only with a retry budget that fits inside the caller's deadline; a timeout after the request left the client means the outcome is unknown, not failed |

## 4. Consumer: fetching

| Setting | Default | When to change it, and what it costs |
|---|---|---|
| `fetch.min.bytes` (`kgo.FetchMinBytes`) | 1 | Raising it batches server-side: fewer, fuller responses at the cost of up to `fetch.max.wait` of latency. Useful for high-volume backfill, wrong for a low-latency path |
| `fetch.max.wait.ms` (`kgo.FetchMaxWait`) | Java 500 ms, franz-go resolved at validation | The ceiling on how long the broker holds a fetch waiting for `fetch.min.bytes` |
| `fetch.max.bytes` (`kgo.FetchMaxBytes`) | 50 MiB in both | The size of one poll's worth of work. **This, not a record count, is how you shrink a poll in franz-go** — there is no `max.poll.records` |
| `max.partition.fetch.bytes` (`kgo.FetchMaxPartitionBytes`) | 1 MiB in both | Caps one partition's share of a fetch, so one busy partition cannot crowd out the others |
| `kgo.MaxConcurrentFetches` | unbounded | Bound it when fetching from many partitions at once would outrun the handler and inflate memory |

## 5. Consumer: offsets and the group

| Setting | Default | When to change it, and what it costs |
|---|---|---|
| `enable.auto.commit` (`kgo.DisableAutoCommit`) | **on** in both, every 5 s | Turn it off for anything that must not lose work. Autocommit advances the offset on a timer regardless of whether the handler finished: a crash between the commit and the write silently drops records — at-most-once by default, chosen by nobody (exp-05) |
| commit strategy | — | Commit *after* the business write, and make the write idempotent (inbox). That is at-least-once plus deduplication, which is the only combination that survives a crash without loss or double effect (exp-06, exp-07) |
| `auto.offset.reset` | Java `latest`; franz-go splits it in two: `ConsumeStartOffset` = `AtStart`, `ConsumeResetOffset` = `RewindOffset(1 minute)` | The single most misread default pair. A new group in Java skips everything written before it existed; in franz-go it replays the topic. And franz-go's *reset* — what happens when a committed offset has fallen out of retention — rewinds a minute, a behaviour Kafka has no equivalent for |
| `partition.assignment.strategy` (`kgo.Balancers`) | Java `[range, cooperative-sticky]`, which negotiates down to eager `range`; franz-go `CooperativeStickyBalancer` | Cooperative rebalancing stops only the partitions that move, instead of every partition in the group. The two clients differ out of the box, so an eager-versus-cooperative comparison has to configure eager explicitly (exp-14) |
| `session.timeout.ms` / `heartbeat.interval.ms` | 45 s / 3 s in both | How long a dead member goes unnoticed. Shorter detects failure sooner and rebalances on transient pauses |
| `max.poll.interval.ms` vs `kgo.RebalanceTimeout` | Java 5 min; **franz-go has no poll watchdog**, its rebalance timeout is 60 s | A slow handler is not noticed until a rebalance happens, and then it has 60 s rather than Java's 300 s. Budget the handler against that, or raise it deliberately (exp-16b) |
| `isolation.level` (`kgo.FetchIsolationLevel`) | `read_uncommitted` | Set `read_committed` for any consumer reading a transactional topic, or it sees aborted records. Cost: reads stop at the LSO, so an open transaction blocks progress until it commits or times out — and blocks *everyone* on the partition, including producers with no transaction: exp-10d held 100 unrelated records back for 23.2 s behind one stuck transaction with a 20 s timeout, while `read_uncommitted` saw them at once |

## 6. Backpressure: slowing the flow on purpose

Named verbatim in KR2, and the thing most people reach for a `sleep` to get. Backpressure
is choosing the rate at which work arrives, not blocking inside the handler.

| Lever | What it does | When |
|---|---|---|
| `kgo.PauseFetchPartitions` / `ResumeFetchPartitions` | stops delivery for chosen partitions while the client keeps polling and heartbeating, so the member stays in the group | the deliberate way to hold back: a downstream is down, a retry tier is waiting out its delay |
| `FetchMaxBytes` / `FetchMaxPartitionBytes` | sizes one poll's worth of work — franz-go's replacement for `max.poll.records` | the handler cannot finish a full poll inside the rebalance timeout |
| bounded handler concurrency | caps the work in flight behind one poll | the handler fans out to a slow dependency |
| circuit breaker around the dependency | stops sending doomed calls, and pauses the topic while open | a systemic outage — the retry chain is for single failures, not for an outage ([07](static/07-retry-dlq.md)) |
| `sleep` in the handler | **not backpressure** | never: it blocks the whole partition, hides the problem from every metric except lag on one partition, and eventually trips the rebalance timeout (exp-16a, exp-16b) |

What each of these does to lag and to group stability: `TBD (exp-16)`.

## 7. Defaults that differ between the two clients

The compact version of everything above, because this is what actually bites when a Java
config is ported to Go.

| Setting | Java | franz-go |
|---|---|---|
| `acks` | `all` (since 3.0) | `AllISRAcks` |
| `enable.idempotence` | on | on |
| `max.in.flight.requests.per.connection` | 5 | **1** |
| `linger.ms` | **0** | **10 ms** |
| `compression.type` | **none** | **snappy** |
| `auto.offset.reset` | `latest` | **two options**: start `AtStart`, reset `RewindOffset(1m)` |
| `partition.assignment.strategy` | `[range, cooperative-sticky]` → eager in practice | `CooperativeStickyBalancer` |
| `max.poll.records` | 500 | **does not exist** — size the poll in bytes |
| `max.poll.interval.ms` | 5 min | **no poll watchdog**; `RebalanceTimeout` 60 s |
| record retry bound | `delivery.timeout.ms` 120 s | **unbounded** unless `RecordDeliveryTimeout` is set |
| default partitioner for null keys | sticky batching | `UniformBytesPartitioner`, switches after **64 KiB** |

## To be measured

| Run | What it fills in |
|---|---|
| exp-14 | eager versus cooperative: processing stalled, in seconds |
| exp-16 | the backpressure table: lag and group stability under each lever |
| exp-17b | the same sweep at saturation, where throughput and codec CPU finally differ |
