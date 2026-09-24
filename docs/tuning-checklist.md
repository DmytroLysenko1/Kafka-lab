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
| `acks` (`kgo.RequiredAcks`) | `all` in Java since 3.0, `AllISRAcks` in franz-go | Never lower it on payments. `acks=1` returns before any follower has the record, so a leader crash inside the replication window loses an acknowledged write, silently. exp-08 held the window open by pausing the followers and lost 2 000 of 2 000 acknowledged records; unpaused, the window is as wide as replication lag, so the loss is rare, which is worse than often. Lowering it buys latency and pays in invisible loss (exp-08) |
| `min.insync.replicas` | **1** — a topic setting, not a producer one | Set it to 2 on every topic that holds money. At 1, `acks=all` means "all of the one replica left in the ISR" and protects nothing. The cost is availability: once the in-sync set is below it, `acks=all` writes are refused — loudly, instead of vanishing. With RF 3 and one broker dead, min.isr 2 still accepted every write — 300 of 300 with the in-sync set at exactly 2 (exp-04) — and min.isr 3 refused 2 000 of 2 000 (exp-08); with min.isr 2 it takes a second failure. Not measured: `acks=all` at min.isr 1 losing like `acks=1`. That needs the in-sync set shrunk to the leader alone, which on three combined broker and controller nodes means two nodes down and the KRaft quorum with them, so the set cannot shrink at all — the same limit that blocks exp-04b |
| `enable.idempotence` (`kgo.DisableIdempotentWrite` to turn off) | on in both | Leave it on. It removes duplicates from producer-side retries: in exp-09 the idempotent producer retried 100 records the leader had already appended before answering `REQUEST_TIMED_OUT`, and the broker dropped every retry by its sequence number — zero duplicated, in three runs of 207 000–267 000 events. Its effect on ordering is not isolated here: the run without it did not reorder either. It holds only within one producer session — a process restart needs a `transactional.id` to keep the guarantee, which no run here measures yet |
| `unclean.leader.election.enable` | `false` | Leave it false. True converts an offline partition into a promoted out-of-sync replica: availability bought with acknowledged records that are then truncated away (exp-04b) |
| `retries` / `recordRetries` | Java `retries=MAX_INT` bounded by `delivery.timeout.ms` 120 s; franz-go record retries **unbounded**, request retries 20, backoff 250 ms → 5 s jittered | Bound the wall-clock, not the count: in franz-go that is `kgo.RecordDeliveryTimeout`. Unbounded retries turn a broker outage into unbounded latency and a full memory buffer instead of a visible error |

## 2. Ordering and parallelism

| Setting | Default | When to change it, and what it costs |
|---|---|---|
| record key | — | The key is the ordering unit: same key, same partition, order preserved. `payment_id` keeps one payment's events in order and spreads payments across partitions. A null key gives up ordering entirely (exp-01) |
| partitioner | Java `DefaultPartitioner` (sticky batching for null keys); franz-go `UniformBytesPartitioner(64 KiB, adaptive, keys)` | Leave it. Note the shape for experiments: with a null key franz-go stays on one partition until **64 KiB** have been written, so a small test run shows no disorder at all and invites a false conclusion (exp-01) |
| partition count | 6 here | Only grows, and growing moves existing keys to other partitions — one payment's history splits across a boundary. Size it for the consumer parallelism you will need, since it is the ceiling (exp-02, [`deploy/topics/README.md`](../deploy/topics/README.md)) |
| `max.in.flight.requests.per.connection` (`kgo.MaxProduceRequestsInflightPerBroker`) | **Java 5, franz-go 1** | The clients differ here. Raising franz-go to 5 buys throughput on a high-latency link; it is safe *only* with idempotence on, which keeps sequence numbers in order. **What it costs without idempotence is client-specific.** In Java the textbook failure is reordering — a retried batch lands after a later one. franz-go drops to one request in flight on a partition after an error until a request succeeds, which is the likely reason exp-09 saw zero reordered in three runs with five in flight. What it did see was duplication — 291, 80 and 80 — and the third run counted the cause: every duplicate was a record the leader appended and then answered `REQUEST_TIMED_OUT` for, which any client without idempotence resends. So the duplicates are not a franz-go trait; the absence of reordering probably is |

## 3. Throughput against latency

| Setting | Default | When to change it, and what it costs |
|---|---|---|
| `linger.ms` (`kgo.ProducerLinger`) | **Java 5 ms since Kafka 4.0 (0 before), franz-go 10 ms** | Waiting fills batches: fewer, larger requests, better compression. **Linger is an upper bound on the added latency, not its price**: at 20 000 records/s exp-17 measured 4.2–8.5 records per batch at 0, 21–24 at 5 ms, 34–37 at 10 ms, and a median of 1.9–3.3 ms, 6.5–7.3 ms, 8.5–10.2 ms respectively. franz-go's 10 ms against the same client set to Java's 5 ms costs about 2 ms of median and buys batches half as large again — a franz-go measurement at Java's value, not a Java one: franz-go's linger flushes every partition bound for a broker once any one of them is due, and Java's 16 KiB batch ceiling is not in that row. Linger 0 has the lowest median and the least stable tail (p99 10–34 ms across cells and three runs, against 12.5–18.7 ms at 5 ms, with two single-cell spikes left out): more, smaller requests queue more unevenly |
| `batch.size` (`kgo.ProducerBatchMaxBytes`) | **Java 16 KB, franz-go ≈1 MB (1 000 012 bytes)** | Raise with linger for bulk work; on its own it does nothing unless batches are actually filling. **It also caps linger**: at 50 ms with a 16 KiB ceiling the median was 11.7–12.2 ms, while the default ≈1 MB ceiling waited the linger out (30.8–36.6 ms median, 138–153 records). The 16 KiB batches averaged ~40 records, about 10 KB: franz-go sends every partition bound for a broker once any one is ready, so the first to fill takes the others with it half-full. At 0–10 ms the two sizes measured the same, because no batch came near 16 KiB. Cost: memory per partition in flight (exp-17) |
| `compression.type` (`kgo.ProducerBatchCompression`) | **Java none, franz-go snappy then none** | **The ratio belongs to the batch, not the codec.** On the same payment payloads exp-17 measured zstd at 2.9–3.7× with linger 0, 4.75–4.81× at 5 ms, 5.14–5.17× at 10 ms and 5.64–5.65× at 50 ms; snappy, franz-go's default, at 2.05–2.29×, 2.82–2.84×, 2.95–2.97× and 3.28×. Small batches waste most of what any codec can do. At 5 MB/s the codec made no measurable difference to latency, so zstd — 44–85 bytes per 250-byte record — cost nothing there. Flat out (exp-17b) the codecs finally differ: without compression the producer stalled at 45–89 MB/s on the wire and 180 000–356 000 records/s, while zstd delivered 2.1–4.7× the records for 34–67% more producer CPU per megabyte, snappy 1.7–4.6× and lz4 1.1–3.1× for no measurable extra CPU — fewer bytes to send saves about as much as compressing them costs. The spread across four runs is wide because the brokers share the host; the ordering held in every run. On this stand bytes were the ceiling, because the brokers share the host and write every byte three times; where CPU is the ceiling instead, the order can reverse. Cost is producer CPU and, if the topic sets its own codec, broker recompression |
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
| `enable.auto.commit` (`kgo.DisableAutoCommit`) | **on** in both, every 5 s | By default autocommit is at-least-once, not at-most-once: both clients commit only what the *previous* poll returned, so a handler that finishes a batch before polling again is never ahead of its commit (franz-go says so in `consumer_group.go`: the one-poll lag "is what makes default autocommit at-least-once"). It turns into at-most-once in two ways: the handler hands records to another goroutine and polls on, or franz-go's `GreedyAutoCommit` commits what was just returned. exp-05b and exp-06b measured both flavours, killed at the same moment right after an autocommit landed mid-batch: greedy lost the unwritten rest of the batch (38 and 41), the default lost nothing and replayed what was written (12 and 12). The hand-off case is argued, not run. Turn it off anyway where the commit has to follow a database write, since only a manual commit can be placed after it |
| commit strategy | — | Commit *after* the business write, and make the write idempotent (inbox). That is at-least-once plus deduplication, which is the only combination that survives a crash without loss or double effect (exp-06, exp-07) |
| `auto.offset.reset` | Java `latest`; franz-go splits it in two: `ConsumeStartOffset` = `AtStart`, `ConsumeResetOffset` = `RewindOffset(1 minute)` | The single most misread default pair. A new group in Java skips everything written before it existed; in franz-go it replays the topic. And franz-go's *reset* — what happens when a committed offset has fallen out of retention — rewinds a minute, a behaviour Kafka has no equivalent for |
| `partition.assignment.strategy` (`kgo.Balancers`) | Java `[range, cooperative-sticky]`, which negotiates down to eager `range`; franz-go `CooperativeStickyBalancer` | Eager revokes every partition, cooperative only the ones that move — exp-14 saw exactly that. What each costs depends on the setup: with two members, a fast handler and a local network, eager's stop lasted 39–75 ms, no longer than an ordinary poll cycle, while cooperative stopped the moved partitions for 0.52–0.68 s, because franz-go notices the second round with a heartbeat 500 ms later; the partitions that stayed kept flowing at their baseline. KIP-848 (`kgo.ServerSideBalancer`) hands over on the consumer heartbeat, 5 s by default, and the moved partitions waited 4.9–6.5 s. Choose cooperative or KIP-848 for what they spare — the members that keep their partitions are never interrupted — not for a faster handover. The two clients differ out of the box, so an eager run has to be configured explicitly |
| `session.timeout.ms` / `heartbeat.interval.ms` | 45 s / 3 s in both | How long a dead member goes unnoticed, and with `group.instance.id` that is exactly what its partitions cost: exp-14b measured 12.6 s of idle partitions at a 12 s timeout when a static member died, against 0.6 s when a dynamic one did. The same setting is what makes a restart free — a static member back inside the timeout keeps its assignment and the group never rebalances (2.05 s of gap, the restart itself, against three assignment changes for a dynamic one). Size it to cover a restart, not an outage |
| `max.poll.interval.ms` vs `kgo.RebalanceTimeout` | Java 5 min; **franz-go has no poll watchdog**, its rebalance timeout is 60 s | franz-go's default does not wait for the handler at all: without `BlockRebalanceOnPoll` the revoke runs beside it, as that option's documentation says; exp-14's instant runs are this default, not a test of it. With `kgo.BlockRebalanceOnPoll`, the Java-like setting, a member busy in its handler holds the rebalance, and after `RebalanceTimeout` the coordinator removes it without telling it: in exp-16 the newcomer waited exactly the 8 s configured, the removed member learnt of it from `UNKNOWN_MEMBER_ID` on its next commit, and a later commit of its rewound a partition 1 726–1 732 records. Budget the handler against the timeout, or stop holding the batch while waiting (section 6) |
| `isolation.level` (`kgo.FetchIsolationLevel`) | `read_uncommitted` | Set `read_committed` for any consumer reading a transactional topic, or it sees aborted records. Cost: reads stop at the LSO, so an open transaction blocks progress until it commits or times out — and blocks *everyone* on the partition, including producers with no transaction: exp-10d held 100 unrelated records back for 23.2 s behind one stuck transaction with a 20 s timeout, while `read_uncommitted` saw them at once |

## 6. Backpressure: slowing the flow on purpose

Named verbatim in KR2, and the thing most people reach for a `sleep` to get. Backpressure
is choosing the rate at which work arrives, not blocking inside the handler.

| Lever | What it does | When |
|---|---|---|
| `kgo.PauseFetchPartitions` / `ResumeFetchPartitions` | stops delivery for chosen partitions while the client keeps heartbeating in the background, so the member stays in the group | the deliberate way to hold back: a downstream is down, a retry tier is waiting out its delay. In exp-16 it was paired with `SetOffsets` back to the first unhandled record and a release of the rebalance: through a 20 s outage, no member was removed, no commit was refused or rewound, and nothing was handled twice |
| `FetchMaxBytes` / `FetchMaxPartitionBytes` | sizes one poll's worth of work — franz-go's replacement for `max.poll.records` | the handler cannot finish a full poll inside the rebalance timeout |
| bounded handler concurrency | caps the work in flight behind one poll | the handler fans out to a slow dependency |
| circuit breaker around the dependency | stops sending doomed calls, and pauses the topic while open | a systemic outage — the retry chain is for single failures, not for an outage ([07](static/07-retry-dlq.md)) |
| `sleep` or an inline retry in the handler | **not backpressure** | never: it holds the batch, and with a single poll loop every partition the member owns waits with it (argued, not run). Under `BlockRebalanceOnPoll` it also holds the rebalance: exp-16 measured the member removed after `RebalanceTimeout`, one commit refused, and its next accepted commit rewinding a partition 1 726–1 732 records — 1 739 handled twice in one run of three |

**What exp-16 measured about lag itself:** through a 20 s outage both handlers built the same
backlog, 7 940–8 501 records at 400 a second, and drained it in 10–13 s. Lag says the
consumer is behind; it does not say whether the group underneath is healthy — the blocking
handler's was not, at the same lag. Alert on the lag's trend, and on commit refusals and
member removals separately.

## 7. Defaults that differ between the two clients

The compact version of everything above, because this is what actually bites when a Java
config is ported to Go.

| Setting | Java | franz-go |
|---|---|---|
| `acks` | `all` (since 3.0) | `AllISRAcks` |
| `enable.idempotence` | on | on |
| `max.in.flight.requests.per.connection` | 5 | **1** |
| `linger.ms` | **5 ms** since Kafka 4.0, 0 before | **10 ms** |
| `compression.type` | **none** | **snappy** |
| `auto.offset.reset` | `latest` | **two options**: start `AtStart`, reset `RewindOffset(1m)` |
| `partition.assignment.strategy` | `[range, cooperative-sticky]` → eager in practice | `CooperativeStickyBalancer` |
| `max.poll.records` | 500 | **does not exist** — size the poll in bytes |
| `max.poll.interval.ms` | 5 min | **no poll watchdog**; `RebalanceTimeout` 60 s |
| record retry bound | `delivery.timeout.ms` 120 s | **unbounded** unless `RecordDeliveryTimeout` is set |
| default partitioner for null keys | sticky batching | `UniformBytesPartitioner`, switches after **64 KiB** |

## Not measured here

| What | Why |
|---|---|
| `acks=all` at `min.insync.replicas=1` losing like `acks=1` | needs the in-sync set shrunk to one, which on three combined broker and controller nodes takes the KRaft quorum down with it — exp-08 |
| eager rebalancing in a large group or with slow handlers | exp-14 had two members on a local network, where eager's round took tens of milliseconds |
| one slow partition stalling a member's other partitions | argued from the single poll loop; exp-16's outage stops every partition at once |
| broker CPU and disk per codec | exp-17b measured the producer only |
| a rolling deploy of more than one member, and static membership under KIP-848 | exp-14b covers one member leaving, dynamic and static, on the classic protocol |
| consumer fetch sizing — `fetch.min.bytes`, `fetch.max.wait.ms`, `FetchMaxBytes`, `FetchMaxPartitionBytes` | section 4 is read off the clients' defaults; no run here varies them |
| the other backpressure levers — bounded handler concurrency, a circuit breaker, fetch sizing | exp-16 measured pause-and-rewind against an inline retry, nothing else in section 6 |
| `AutoCommitMarks` / `MarkCommitRecords` as a commit strategy | used as a tool in exp-14, never compared against the other strategies |
