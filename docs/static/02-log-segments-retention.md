# 02 — The log on disk: segments, offsets, retention, compaction

**The question:** what is actually stored in a partition, and which of the four
"current offsets" does a given tool mean?

KR1 · no interactive twin: this one needs exact geometry, and Mermaid lies about
proportions. A monospace block is the honest format here.

```
partition directory: /var/lib/kafka/data/payments.main-3/

  00000000000000000000.log        closed segment  - retention and compaction may touch it
  00000000000000000000.index                      - offset -> byte position, sparse
  00000000000000000000.timeindex                  - timestamp -> offset
  00000000000000001024.log        ACTIVE segment  - never compacted, never deleted
  00000000000000001024.index
  00000000000000001024.timeindex
  leader-epoch-checkpoint                         - epoch -> start offset, used to truncate
  partition.metadata

offsets inside the partition:

    1024          1043              1088     1092        1097
      |             |                 |        |           |
      v             v                 v        v           v
      [=============|=================|========|===========]
                    ^                 ^        ^           ^
                    |                 |        |           LEO  - the leader's next offset
                    |                 |        HW   - every ISR member holds this; consumers stop here
                    |                 LSO  - read_committed stops here: a transaction is still open
                    committed offset of group "payments-consumer"

  consumer lag of the group = HW - committed = 1092 - 1043 = 49 records
```

*Fig. 2 — four different "current positions" exist at the same time, and consumer lag is
the distance between two specific ones; confusing LEO with HW is how a lag dashboard
ends up showing a number nobody can explain (exp-03).*

## The four positions

| Position | Meaning | Who moves it |
|---|---|---|
| **LEO** (log end offset) | the next offset the leader will write | the producer |
| **HW** (high watermark) | the lowest LEO across the ISR — the visibility ceiling for `read_uncommitted` | replication |
| **LSO** (last stable offset) | first offset of the oldest open transaction, capped by HW — the ceiling for `read_committed` | transactional producers |
| **committed offset** | how far one consumer group has promised it is done | the consumer, via `OffsetCommit` |

`LSO <= HW <= LEO` always. A single long-running transaction pins the LSO and stalls
every `read_committed` consumer on that partition while `read_uncommitted` consumers see
no problem at all — a failure mode worth recognising before it is diagnosed live. What
moves the LSO, and why it moves only when a marker is written, is
[10](10-transactions-eos.md).

## Segments

A partition is a directory of segment files named after the offset of their first
record. Only the last one is open for writing. It rolls when `segment.bytes` (1 GB by
default) or `segment.ms` (7 days) is reached.

The `.index` file is **sparse**: one entry per `index.interval.bytes` (4 KB by default),
so a lookup binary-searches the index and then scans forward inside the segment. This is
why "seek to an arbitrary offset" is cheap without keeping an entry per record in memory.

## Retention deletes whole segments

There is no mechanism to delete part of a segment. A record survives until the segment
containing it is **closed** and then fully aged out, so effective retention is always
longer than `retention.ms` suggests. On a low-traffic topic with the default 1 GB
segment the difference is weeks, not minutes — this is a capacity-planning surprise, not
a trivia item.

Deleting by size (`retention.bytes`) works the same way and is per partition, not per
topic: a topic with 6 partitions and `retention.bytes=1GB` holds up to 6 GB.

## Compaction keeps the last value per key

`cleanup.policy=compact` turns the topic into a snapshot of current state instead of a
history. The traps below are listed in the order in which they will waste an afternoon,
and all four have to be defeated at once for a demo to show anything (exp-03):

| Trap | Default | Why nothing happens |
|---|---|---|
| the active segment is never compacted | `segment.bytes=1GB` | with default sizes a demo writes into one open segment and sees no compaction ever |
| `min.cleanable.dirty.ratio` | `0.5` | cleaning does not start until half the log is superseded |
| `min.compaction.lag.ms` | `0` | but combined with segment roll timing it still delays the first pass |
| `delete.retention.ms` | 24 h | tombstones (`value=nil`) stay visible for a day, so "the key is still there" looks like a bug |

Compaction is also what makes `__consumer_offsets` bounded: the latest offset per
group/topic/partition is kept, everything older is cleaned. Consumer group progress is
data inside Kafka, not state inside the process — which is why restarting a consumer
changes nothing and deleting a group changes everything.

**When a compacted topic is the right answer:** the consumer needs current state and can
rebuild it from scratch by replaying the topic (event-carried state transfer). **When it
is wrong:** the consumer needs the sequence of what happened. Compaction destroys
intermediate values by design — after it runs, "the payment was authorised then
captured" is no longer in the log.

## To be measured

| Run | What it shows | Status |
|---|---|---|
| exp-03 | log dump before and after compaction, tombstone disappearance, real vs configured retention | TBD |
