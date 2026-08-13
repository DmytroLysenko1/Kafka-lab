# Kafka internals — cheat sheet and case files

This package is the **KR1 deliverable**: a one-screen summary of the internals, followed
by nine cases — one file, one diagram, one question each. Mermaid in Markdown, in git,
rendered by GitHub without running anything. Six of the nine also exist as interactive
walkthroughs in [`docs/dynamic/`](../dynamic/), for learning the mechanics and for the
tech talk; a reviewer reads this package, an audience watches that one.

## The vocabulary, one line each

| Term | What it actually is |
|---|---|
| topic | a name. Everything real happens at partition level |
| partition | an append-only log on one leader broker, replicated to followers. The unit of ordering, of parallelism and of retention |
| offset | the position of a record inside one partition. Never global, never comparable across partitions |
| segment | one file on disk. A partition is a directory of them; only the last is open for writing ([02](02-log-segments-retention.md)) |
| record key | decides the partition — `murmur2(key) mod partitions`, computed **client-side** ([01](01-write-path.md)) |
| consumer group | a set of members sharing a subscription; each partition is owned by exactly one member ([03](03-read-path.md)) |
| group coordinator | the broker leading the `__consumer_offsets` partition the group id hashes to. Owns membership and committed offsets — nothing else |
| controller (KRaft) | the active node of the Raft quorum. Elects partition leaders and writes cluster metadata as a replicated log ([04](04-isr-leader-election.md)) |
| ISR | the replicas currently caught up with the leader. Shrinks silently, and `acks=all` shrinks with it |

## Ordering and parallelism — the two rules everything else follows from

1. **Order exists inside one partition and nowhere else.** Same key → same partition →
   ordered. `key=nil` → no ordering for that entity, and the failure is invisible at low
   volume ([01](01-write-path.md), exp-01).
2. **One partition is served by one member of a group.** The partition count is a hard
   ceiling on consumers; extras idle ([03](03-read-path.md), exp-02). Adding partitions
   later breaks the existing key→partition mapping.

## The four "current offsets" — [02](02-log-segments-retention.md)

`committed <= LSO <= HW <= LEO`. LEO is where the leader will write next; HW is the
lowest LEO in the ISR and the ceiling for `read_uncommitted`; LSO is the first offset of
the oldest open transaction and the ceiling for `read_committed`; committed is one
group's promise. **Consumer lag = HW − committed.**

## The durability chain — [01](01-write-path.md), [04](04-isr-leader-election.md)

`acks=all` means *every replica currently in the ISR*, and the acknowledgement lands
before any `fsync`. So the guarantee is only as strong as the ISR is wide, and
`min.insync.replicas=2` is what turns a collapsed ISR into a visible
`NOT_ENOUGH_REPLICAS` rejection instead of silent success from one machine.
`unclean.leader.election.enable=false` keeps a partition offline rather than promoting a
replica that is missing acknowledged records.

## Retention vs compaction — [02](02-log-segments-retention.md)

Retention deletes **whole segments**, so effective retention is always longer than
configured. Compaction keeps the last value per key and destroys the intermediate
history — right for "current state" topics (event-carried state transfer), wrong when
the sequence of what happened is the point.

## Defaults that decide the outcome

| Setting | Default | Why it matters here |
|---|---|---|
| `acks` | `all` — Java since 3.0, and `AllISRAcks` in franz-go | with `1` a leader crash loses acknowledged records (exp-08) |
| `min.insync.replicas` | `1` | a **topic** setting, not a producer one. At `1`, `acks=all` protects nothing |
| `replica.lag.time.max.ms` | 30 s | how long a dead follower stays in the ISR |
| `enable.auto.commit` | `true` — franz-go autocommits every 5 s when group consuming | the at-most-once default nobody chose (exp-05) |
| `auto.offset.reset` | `latest` in Java, **`AtStart` (earliest) in franz-go** | the two clients default in opposite directions: a new group either replays all history or silently skips it. First entry for the "Java parameter → franz-go equivalent" column of the tuning checklist |
| `segment.bytes` / `segment.ms` | 1 GB / 7 days | the active segment is never compacted or deleted |
| `min.cleanable.dirty.ratio` | `0.5` | compaction does not start until half the log is superseded |
| `delete.retention.ms` | 24 h | how long tombstones stay visible |
| `unclean.leader.election.enable` | `false` | availability traded for not deleting acknowledged writes |

## The cases

| # | Case | The one question it answers | KR | Interactive |
|---|---|---|---|---|
| [01](01-write-path.md) | Write path | What has to happen before `Produce` returns success? | KR1 | [yes](../dynamic/write-path/) |
| [02](02-log-segments-retention.md) | The log on disk | What is actually stored, and what does "current offset" mean? | KR1 | — (needs exact geometry) |
| [03](03-read-path.md) | Read path | How does a consumer get a partition and a starting offset? | KR1 | [yes](../dynamic/read-path/) |
| [04](04-isr-leader-election.md) | ISR and leader election | What makes an acknowledged record disappear? | KR1 | — |
| [05](05-delivery-semantics.md) | Delivery semantics | Where exactly is a message lost or duplicated? | KR2 | [yes](../dynamic/lost-message/) |
| [06](06-transactional-outbox.md) | Transactional outbox | Where does atomicity end between Postgres and Kafka? | KR3, KR5 | [yes](../dynamic/outbox/) |
| [07](07-retry-dlq.md) | Retry chain and DLQ | Where does a failing message wait, and who is blocked? | KR3 | [yes](../dynamic/retry-dlq/) |
| [08](08-rebalance.md) | Eager vs cooperative rebalance | How long does the group stop processing? | KR2, KR4 | [yes](../dynamic/rebalance/) |
| [09](09-schema-evolution.md) | Schema evolution | Who gets upgraded first, producers or consumers? | KR3 | — |

The service architecture diagram required by KR3 lives in the [repository
README](../../README.md), because that is where the reviewer looks for it.

## Conventions these files follow

**Numbers are measured or marked.** Every figure is either taken from a run under
`experiments/` and tagged with its run id, or written as `TBD (exp-NN)` because the
experiment has not been run yet. No number in this package comes from memory or from an
article. Documented configuration defaults are cited as defaults, not as findings.

**A caption states a conclusion, not a subject.** Not "Fig. 1. The producer", but
"Fig. 1. `acks=all` acknowledges after replication and before `fsync`". A diagram that
cannot be summed up in one sentence is drawing two things and gets split into two files.

**One file answers one question.** The question is in the heading of every file and in
the table above. Anything that does not serve it belongs in another file.

**A referenced step is numbered by hand.** Mermaid's `autonumber` counts arrows and skips
notes, so the pauses — batching, page cache, the high watermark — fall out of the
numbering exactly where the explanation needs them. Wherever a table or a sentence points
at a step ([01](01-write-path.md), [03](03-read-path.md)), the numbers are therefore
written into the diagram by hand and verified against the rendered output. `autonumber`
survives only in files where nothing references a number.

**Boundaries are drawn, not implied.** The most valuable line in these diagrams is where
atomicity ends: the transaction frame, the process edge, the network call.

**Failure points are marked.** At least half of these cases exist because something goes
wrong, so the failure window is on the diagram, not only in the prose.

**Names match the code.** `outbox-relay`, `payments.retry.5s`, `payments-consumer` —
the same identifiers that appear in `cmd/` and in the topic list, never a generic
"Publisher".

**Colour never carries meaning alone.** Every highlighted block is also labelled, so the
diagrams survive dark mode and a black-and-white printout.
