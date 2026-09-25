# Kafka internals — cheat sheet and case files

This package is the **KR1 deliverable**: a one-screen summary of the internals, followed
by ten cases — one file and one question each, drawn in as many figures as the answer
has cases: one where the mechanism is a sequence, two or three where the answer is
"compare these". Mermaid in Markdown, in git,
rendered by GitHub without running anything. Six of the ten also exist as interactive
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

`LSO <= HW <= LEO`, always. LEO is where the leader will write next; HW is the lowest LEO
in the ISR and the ceiling for `read_uncommitted`; LSO is the first offset of the oldest
open transaction and the ceiling for `read_committed` ([10](10-transactions-eos.md) is
what moves it). The committed offset is **not** part of that chain: it is one group's
promise, it sits wherever that group left it, and a `read_uncommitted` consumer routinely
commits above the LSO. **Consumer lag = HW − committed**, because the "latest offset" a
client gets back from `ListOffsets` is the high watermark, not the LEO — which is why the
column the tool labels LOG-END-OFFSET is not the LEO. The label is a misnomer, and the HW
being what is returned is what makes it a misleading one.

## The durability chain — [01](01-write-path.md), [04](04-isr-leader-election.md)

`acks=all` means *every replica currently in the ISR*, and the acknowledgement lands
before any `fsync`. So the guarantee is only as strong as the ISR is wide, and
`min.insync.replicas=2` is what turns a collapsed ISR into a visible
`NOT_ENOUGH_REPLICAS` rejection instead of silent success from one machine.
`unclean.leader.election.enable=false` keeps a partition offline rather than promoting a
replica that is missing acknowledged records.

## Retention vs compaction — [02](02-log-segments-retention.md)

Retention deletes **whole segments**, so effective retention is always longer than
configured — reasoning, not yet measured here (exp-03d). Compaction keeps the last value
per key and destroys the intermediate history — right for "current state" topics
(event-carried state transfer), wrong when the sequence of what happened is the point.

## Defaults that decide the outcome

The full list, with what each setting buys and what it charges, is the tuning checklist:
[`docs/tuning-checklist.md`](../tuning-checklist.md). The table below is the short version —
the defaults that decide an outcome on their own.

| Setting | Default | Why it matters here |
|---|---|---|
| `acks` | `all` — Java since 3.0, and `AllISRAcks` in franz-go | with `1` a leader crash inside the replication window loses acknowledged records — exp-08 held the window open and lost 2 000 of 2 000 |
| `min.insync.replicas` | `1` | a **topic** setting, not a producer one. At `1`, `acks=all` protects nothing |
| `replica.lag.time.max.ms` | 30 s | how long a **live but lagging** follower stays in the ISR — a GC pause or a slow disk, not a crash |
| `broker.session.timeout.ms` | 9 s, with heartbeats every 2 s | what actually removes a **dead** broker from every ISR: the KRaft controller fences it when its session expires. The two timers get conflated constantly, and they answer different questions. exp-04 measured 10.1 s and 10.6 s for a crash, and exp-04c raised this timer to 20 s and measured 20.3 s while the lag timer stayed at 30 s — the reaction tracks this one |
| `enable.auto.commit` | `true` — franz-go autocommits every 5 s when group consuming | at-least-once, not at-most-once: both clients commit only what the *previous* poll returned. It loses records only when handling is asynchronous or franz-go's `GreedyAutoCommit` is on. Killed at the same moment, greedy lost the unwritten rest of a batch and the default lost nothing and replayed 12 (exp-05b, exp-06b) |
| `auto.offset.reset`, part 1: where a **new group** starts | `latest` in Java, **`AtStart` (earliest) in franz-go** (`ConsumeStartOffset`) | the two clients default in opposite directions: a new group either replays all history or silently skips it |
| `auto.offset.reset`, part 2: what happens to a **committed offset that fell out of retention** | `latest` or `earliest` in Java, **the log start** in franz-go (`ConsumeResetOffset`) | franz-go splits the one Java setting in two, and the second half splits again by cause. For this row's case — a commit that fell below the log start — it resumes at the log start, i.e. Java's `earliest`: *"every record that still exists is one it never consumed"*. Its `RewindOffset(1m)` default is what it does after **mid-stream data loss** — a broker that lost records, an epoch it cannot place — and *that* is the behaviour Kafka has no equivalent for. Reading the minute onto the retention case is the misread (`kgo/config.go`, doc on `ConsumeResetOffset`) |
| `compression.type` | `none` in Java, **snappy in franz-go** | the Go producer compresses before you ask it to — 2.05× to 3.28× on payment events depending on how full the batches are (exp-17) |
| `partition.assignment.strategy` | `[range, cooperative-sticky]` in Java — which negotiates down to eager `range` — **`CooperativeStickyBalancer` in franz-go** | the two clients rebalance differently out of the box, so eager has to be configured explicitly. exp-14: eager revoked all six partitions and stopped them 39–75 ms; cooperative stopped only the three that moved, for 0.52–0.68 s; KIP-848 for 4.9–6.5 s, around its heartbeat interval ([08](08-rebalance.md)) |
| rebalance timeout | `max.poll.interval.ms` 5 min in Java, **`RebalanceTimeout` 60 s in franz-go** | the budget a slow handler gets before the group gives up on it differs by 5× ([03](03-read-path.md)) |
| `linger.ms` / `batch.size` | 5 ms (0 before Kafka 4.0) / 16 KB in Java, **10 ms / ≈1 MB in franz-go** | franz-go has already traded latency for batching before any tuning happens: 34–37 records per batch and 8.5–10.2 ms median at 20 000 records/s, against 21–24 and 6.5–7.3 ms with franz-go set to Java's 5 ms (exp-17; no Java client was run) |
| `segment.bytes` / `segment.ms` | 1 GB / 7 days | the active segment is never compacted or deleted |
| `min.cleanable.dirty.ratio` | `0.5` | compaction does not start until half the log is superseded |
| `delete.retention.ms` | 24 h | how long tombstones stay visible |
| `unclean.leader.election.enable` | `false` | availability traded for not deleting acknowledged writes |

The franz-go column is read out of `pkg/kgo/config.go` on master, not out of memory: these
defaults have moved between releases, and the partitioner in [01](01-write-path.md) is the
clearest example of a fact that used to be true. Re-check every one of them against the
revision pinned in `go.mod` once the service exists.

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
| [10](10-transactions-eos.md) | Transactions and EOS | What does a Kafka transaction actually cover, and where does it stop? | KR2 | — |

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
at a step ([01](01-write-path.md), [03](03-read-path.md), [10](10-transactions-eos.md)),
the numbers are therefore written into the diagram by hand and verified against the
rendered output. `autonumber`
survives only in files where nothing references a number.

**Boundaries are drawn, not implied.** The most valuable line in these diagrams is where
atomicity ends: the transaction frame, the process edge, the network call.

**Failure points are marked.** At least half of these cases exist because something goes
wrong, so the failure window is on the diagram, not only in the prose.

**Every question about a boundary is answered with a frame.** A `rect` marks one of two
things and nothing else: the extent of a guarantee (green — [01](01-write-path.md),
[05](05-delivery-semantics.md), [06](06-transactional-outbox.md),
[10](10-transactions-eos.md)) or the window in which it fails (red for loss, amber for
duplication or stalled work). A boundary that appears only in the caption has not been
drawn.

**Where the answer is a comparison, both cases are drawn, not one case and a paragraph.**
Same participants, same record, same failure — and exactly one thing different between
the figures: the position of the commit ([05](05-delivery-semantics.md)), the assignor
([08](08-rebalance.md)), the place the waiting happens ([07](07-retry-dlq.md)), the deploy
order ([09](09-schema-evolution.md)), the ordering of two writes
([06](06-transactional-outbox.md)). The reader compares two pictures instead of trusting
one picture and a claim.

**Names match the code.** `outbox-relay`, `payments-consumer.retry.5s`, `payments-consumer` —
the same identifiers that appear in `deploy/topics/` and, from the service stage on, in `cmd/`, never a generic
"Publisher".

**Colour never carries meaning alone, and it is applied as a tint.** Every highlighted
block is also labelled, so the diagrams survive a black-and-white printout. The `rect`
fills are `rgba` washes rather than solid pastels, because GitHub renders Mermaid in the
reader's own theme: a solid light fill puts near-white text on a pale background for
everyone in dark mode, and pinning the whole palette with `%%{init}%%` only moves the
problem outside the frame. Both variants were rendered in both themes before this one was
chosen.
