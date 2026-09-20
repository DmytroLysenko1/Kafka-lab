# 01 — Write path

**The question:** what has to happen before `Produce` returns success, and what has
*not* happened yet at that moment?

KR1 · interactive version: [`docs/dynamic/write-path/`](../dynamic/write-path/)

```mermaid
sequenceDiagram
    participant App as payments-api
    participant P as franz-go producer
    participant B as Any broker
    participant L as Leader p3
    participant F as Followers in ISR

    App->>P: 1. Produce key=payment_id value=PaymentCaptured
    P->>B: 2. Metadata topic=payments.main
    B-->>P: 3. partition leaders and epochs
    Note over P: 4. partition = murmur2(key) mod 6, computed client-side
    Note over P: 5. the batch grows until linger.ms or batch.size

    P->>L: 6. ProduceRequest acks=all, producer id and epoch, seq=N

    alt ISR smaller than min.insync.replicas
        L-->>P: 7. NOT_ENOUGH_REPLICAS, nothing was appended
    else ISR large enough
        rect rgba(67, 160, 71, 0.16)
            Note over L,F: 8. the whole guarantee lives inside this frame,<br/>and there is no disk in it: append to the active segment,<br/>LEO advances, the write sits in page cache, no fsync
            F->>L: 9. FetchRequest from their own LEO
            L-->>F: 10. records
            Note over L,F: 11. high watermark equals the lowest LEO in the ISR<br/>it moves only when every ISR member holds the record
            L-->>P: 12. ProduceResponse ok, base offset 1043
        end
    end

    P-->>App: 13. ack
```

*Fig. 1 — the frame is the entire meaning of `acks=all`: the record is in the page cache
of every replica **currently in the ISR**, no `fsync` has happened on any of them, and
how much that is worth depends on how wide the ISR was at step 8, not on how the topic
was configured (exp-08).*

## What each step really is

The numbers are written into the diagram by hand, and they cover the pauses as well as
the arrows — steps 4, 5, 8 and 11 are where the interesting things happen, and Mermaid's
`autonumber` would have skipped every one of them. The frame around 8–12 is the answer to
the second half of the question: it contains everything the acknowledgement promises, and
a `fsync` is not in it.

| # | What actually happens | Config that moves it | What breaks if you get it wrong |
|---|---|---|---|
| 1 | An in-memory call. Nothing has touched the network. | — | Treating the return of `Produce` as durability. In franz-go the promise callback, not the call, is the event that matters. |
| 2–3 | The client asks **any** broker where the partition leaders are, and caches the answer. Placement is never decided by a broker. | `metadata.max.age.ms` | After leadership moves, the broker answers `NOT_LEADER_OR_FOLLOWER`. A client that does not refresh metadata retries into a void. |
| 4 | `murmur2(key) mod partitions`, computed client-side. With no key franz-go's `UniformBytesPartitioner` stays on one partition until 64 KiB have gone to it, then moves — so a small run looks ordered and a real one is not: between 6 448 and 7 964 of 10 000 events arrived out of order across three runs (exp-01). franz-go hashes keys the way the Java client does, so a Go producer and a Java producer put the same key on the same partition. | `partitioner` → `kgo.RecordPartitioner` | `key=nil` means no ordering for that entity. franz-go's default is `UniformBytesPartitioner(64 KiB, adaptive, keys)` — the KIP-794 uniform sticky partitioner, not the older sticky-until-the-batch-closes one — so a keyless stream stays on one partition until **64 KiB** have gone to it and only then re-picks. At low volume nothing looks broken and you conclude keys are optional; exp-01 has to push well past that threshold to break it. |
| 5 | The batch, not the record, is the unit of transfer. Compression happens here, in the producer. | `linger.ms` → `kgo.ProducerLinger` (**10 ms**, Java: 0) · `batch.size` → `kgo.ProducerBatchMaxBytes` (**≈1 MB**, Java: 16 KB) · `compression.type` → `kgo.ProducerBatchCompression` (**snappy**, Java: none) | This is the throughput-versus-latency dial, and the three defaults above all differ from Java's — a franz-go producer already lingers and already compresses, so "the Go service is slower/cheaper than the Java one" starts here rather than in the code (exp-17). |
| 6 | The request carries a producer id, an epoch and a per-partition sequence number, and the broker rejects a sequence it has already accepted. | `enable.idempotence` | Retries are deduplicated **within one producer session**. Across a restart the id changes and the guarantee ends — which is why the service still needs an outbox and an inbox. |
| 7 | The ISR size is checked **before** the append. Too few in-sync replicas and the record is rejected outright, with nothing written. | `min.insync.replicas`, `acks` | With `min.insync.replicas=1` the cluster degrades silently: `acks=all` keeps returning success while one surviving replica holds the data. |
| 8 | The append goes into the page cache. Kafka does not `fsync` per record. | `flush.messages`, `flush.ms` — leave them alone | Believing "acked" means "on disk on three machines". It means "in the page cache of three machines". |
| 9–10 | Replication is a **fetch**. Followers pull on their own schedule, the leader never pushes. | `replica.lag.time.max.ms`, `broker.session.timeout.ms` | Expecting the ISR to react instantly, and expecting one timer to cover both cases. A follower that merely slows down leaves the ISR after `replica.lag.time.max.ms`, 30 s by default; a broker that *dies* is removed when the controller fences its session, 9 s by default — exp-04 measured 10.1 s for the crash case, consistent with the second timer and not the first — see [04](04-isr-leader-election.md). |
| 11 | The high watermark is the lowest LEO across the ISR. Consumers never see above it. | — | This is precisely why a consumer cannot read a record that a leader failure could still erase. |
| 12 | The response is released only once the record is replicated across the whole ISR — the same condition that moves the high watermark. | `acks` | With `acks=1` the leader answers after its own append. Kill it before the followers fetch and the acknowledged record is gone (exp-08). |
| 13 | Only now does the payment exist for the rest of the system. | `delivery.timeout.ms` | A timeout here is an **unknown outcome**, not a failure. The write may well have been accepted. Recording it as failed and moving on is how money is lost. |

## Three claims this diagram exists to kill

**"`acks=all` means all replicas."** It means all replicas *currently in the ISR*. A
partition with `replication.factor=3` whose ISR has shrunk to one still returns success
for `acks=all` — it is just returning success from a single machine.
`min.insync.replicas=2` converts that silent degradation into a visible
`NOT_ENOUGH_REPLICAS` rejection. Failing loudly is the entire point.

**"Acknowledged means written to disk."** There is no `fsync` on this path. The record
is in the leader's page cache and in the page cache of every in-sync follower. That is
stronger than one disk and weaker than three disks, and the difference shows up exactly
once: a power event that takes the rack down together.

**"It retried, so there may be duplicates."** With `enable.idempotence=true` every batch
carries a producer id, an epoch and a per-partition sequence number, and the broker
rejects a sequence it has already accepted. Retries within one producer session are
deduplicated by the broker. Across a producer **restart** the id changes and the
guarantee ends — that boundary is why the service still needs an outbox and an inbox
([05](05-delivery-semantics.md), [06](06-transactional-outbox.md)).

## To be measured

| Run | What it shows | Status |
|---|---|---|
| exp-01 | `key=nil` vs `key=payment_id`: ordering violations per 10 000 events | **keyless: 6 448 – 7 964 violations over three runs, all 100 payments split across partitions every time. Keyed: 0 violations, 0 split.** [runs](../../experiments/exp-01-partition-keys/results/) |
| exp-08 | `acks=1` plus leader kill vs `acks=all` with `min.insync.replicas=2` (exp-08 runs both halves: the loss under `acks=1`, and the refusal on a topic with `min.insync.replicas=3` when one broker dies) | TBD |
| exp-09 | idempotence off, five in-flight requests, injected network failure: reordering | TBD |
| exp-17 | `linger` × batch size × codec sweep: throughput against p99 produce latency, and what the franz-go defaults already cost | TBD |
