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

the same partition as offsets - the file names above are the tick marks below:

                                           LSO    HW        LEO
                                           1088   1092      1097
                                             |      |         |
                                             v      v         v
  [===========================|=============|======|=========]
  ^                           ^
  |                           1024  first offset of the ACTIVE segment: never
  |                                 compacted and never deleted, which is also
  |                                 the floor on what retention is able to remove
  0  oldest offset still on disk - retention drops whole segments, never records

                                      x  1043  committed offset of the group
                                      |        "payments-consumer", drawn below the
                                      |        log because it is not a position of
                                      |        the partition at all: it is one
                                      |        group's promise, stored in
                                      |        __consumer_offsets, and a second
                                      |        group on this same partition sits
                                      |        at a completely different number

  consumer lag of that group = HW - committed = 1092 - 1043 = 49 records
```

*Fig. 2 — three of these positions belong to the partition and are the same for every
reader; the fourth belongs to one consumer group and is drawn on its own line for that
reason. Consumer lag is the distance between one of each, which is why "the lag" is
meaningless without naming the group — and why confusing LEO with HW produces a
dashboard number nobody can explain (exp-03).*

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
a trivia item. It is also the one claim on this page that exp-03 does **not** back: that
run rolls segments every second, so nothing of its data ever sat in an open segment past
its retention, and everything was duly dropped (exp-03d below).

Deleting by size (`retention.bytes`) works the same way and is per partition, not per
topic: a topic with 6 partitions and `retention.bytes=1GB` holds up to 6 GB.

## Compaction keeps the last value per key

`cleanup.policy=compact` turns the topic into a snapshot of current state instead of a
history. The traps below are listed in the order in which they will waste an afternoon,
and all four have to be defeated at once for a demo to show anything (exp-03):

| Trap | Default | Why nothing happens |
|---|---|---|
| the active segment is never compacted | `segment.bytes=1GB` | with default sizes a demo writes into one open segment and sees no compaction ever. **And on Kafka 4.x you cannot shrink your way out: `segment.bytes` is refused below 1 MiB, so a demo has to roll segments by `segment.ms` instead (exp-03)** |
| `min.cleanable.dirty.ratio` | `0.5` | cleaning does not start until half the log is superseded |
| `min.compaction.lag.ms` | `0` | but combined with segment roll timing it still delays the first pass |
| `delete.retention.ms` | 24 h | tombstones (`value=nil`) stay visible for a day, so "the key is still there" looks like a bug. They are not a leftover: the window exists so every consumer gets a chance to see the delete, and compaction keeps them until it passes (exp-03 saw all 10 survive the first pass) |

Compaction is also what makes `__consumer_offsets` bounded: the latest offset per
group/topic/partition is kept, everything older is cleaned. Consumer group progress is
data inside Kafka, not state inside the process — which is why restarting a consumer
changes nothing and deleting a group changes everything.

**When a compacted topic is the right answer:** the consumer needs current state and can
rebuild it from scratch by replaying the topic (event-carried state transfer). **When it
is wrong:** the consumer needs the sequence of what happened. Compaction destroys
intermediate values by design — after it runs, "the payment was authorised then
captured" is no longer in the log.

## Measured

`make exp-03` · [journal](../00-journal.md#exp-03--what-the-cleaner-keeps) ·
[run log](../../experiments/exp-03-segments-retention/results/run-2026-09-20-154030.log)

| Topic | Before cleaning | After cleaning |
|---|---|---|
| compacted, 50 keys × 40 updates, 10 deleted | 2 010 records, 40 live keys, 10 tombstones | **50 records** — the last value of each surviving key plus the 10 tombstones compaction keeps |
| kept 5 s, 2 000 records | 2 000 records, log starts at 0 | **0 readable**; the log now starts past the last of them |

**`segment.bytes` below 1 MiB is refused on Kafka 4.x** —
[captured](../../experiments/exp-03-segments-retention/results/segment-bytes-rejected.log):
`Invalid value 4096 for configuration segment.bytes: Value must be at least 1048576`. The
standard advice to shrink it for a demo no longer works; roll by `segment.ms` instead.

**The tombstones survived, which is the correct answer.** They are kept for
`delete.retention.ms` so every consumer still reading gets a chance to learn the key is
gone. A tombstone is a record saying "deleted", not an absence.

## Still to be measured

The exp-03 row here originally promised three things this run does not deliver. They are
listed rather than quietly dropped:

| Run | What it shows | Status |
|---|---|---|
| exp-03b | **segment geometry from a real log dump** (`kafka-dump-log.sh`): base offsets, segment sizes, what the index files hold | TBD — exp-03 reads the log as a consumer, so it sees records, never segment boundaries |
| exp-03c | **tombstone removal** after `delete.retention.ms` elapses — the second cleaner pass, not the one that compacts values | TBD — `exp03.compact.yaml` sets `delete.retention.ms: 1000` expecting them to vanish inside the run, and in every run so far all 10 survived |
| exp-03d | **data outliving `retention.ms`** because it is trapped in an open segment | TBD — this is the "a topic holds more than its setting says" claim in the section above, and exp-03 showed the opposite: it rolls segments continuously, so aged data always lands in a closed segment and is always dropped |

exp-03d is the one that matters for the retention section: until it runs, "effective
retention is always longer than `retention.ms` suggests" is reasoning, not a measurement.
