# exp-14 — what a consumer joining the group costs, by assignment strategy

**Hypothesis.** When a member joins, eager rebalancing stops every partition in the group
until the new assignment is in place, and cooperative rebalancing stops only the partitions
that move. KIP-848, where the broker assigns, should behave like cooperative.

```
make exp-14
```

A steady stream spread evenly over six partitions, one consumer, and a second joining after
15 s. Every record's handling time is recorded, and for each partition the run reports the
longest stretch nothing of it was handled — over 10 s before the join, as the baseline, and
from 2 s before to 15 s after it. Three strategies — eager `RangeBalancer`,
`CooperativeStickyBalancer`, and KIP-848 with the broker's range assignor — each with two
handlers:

- **instant** — franz-go's default, where a rebalance does not wait for the handler;
- **blocking** — `BlockRebalanceOnPoll`, 2 ms a record at 400 records/s, so a rebalance waits
  for the batch in hand, as it does in the Java consumer.

Eager has to be asked for: franz-go defaults to cooperative-sticky, and a run left at its
defaults would measure cooperative twice.

| Knob | Default | Why |
|---|---|---|
| `-rate` | 1 200 records/s (400 in the blocking runs) | every partition flows steadily, so a gap is the consumer's |
| `-join-after` | 15 s | a clean baseline before the join |

## Result — 2026-09-22 and 2026-09-26

Four runs of all six, two on each date:

| | eager | cooperative | KIP-848 |
|---|---|---|---|
| instant | [1](results/eager-instant-2026-09-22-020510.log) · [2](results/eager-instant-2026-09-22-021756.log) · [3](results/eager-instant-2026-09-26-031909.log) · [4](results/eager-instant-2026-09-26-032315.log) | [1](results/cooperative-instant-2026-09-22-020510.log) · [2](results/cooperative-instant-2026-09-22-021756.log) · [3](results/cooperative-instant-2026-09-26-031909.log) · [4](results/cooperative-instant-2026-09-26-032315.log) | [1](results/server-instant-2026-09-22-020510.log) · [2](results/server-instant-2026-09-22-021756.log) · [3](results/server-instant-2026-09-26-031909.log) · [4](results/server-instant-2026-09-26-032315.log) |
| blocking | [1](results/eager-blocking-2026-09-22-020510.log) · [2](results/eager-blocking-2026-09-22-021756.log) · [3](results/eager-blocking-2026-09-26-031909.log) · [4](results/eager-blocking-2026-09-26-032315.log) | [1](results/cooperative-blocking-2026-09-22-020510.log) · [2](results/cooperative-blocking-2026-09-22-021756.log) · [3](results/cooperative-blocking-2026-09-26-031909.log) · [4](results/cooperative-blocking-2026-09-26-032315.log) | [1](results/server-blocking-2026-09-22-020510.log) · [2](results/server-blocking-2026-09-22-021756.log) · [3](results/server-blocking-2026-09-26-031909.log) · [4](results/server-blocking-2026-09-26-032315.log) |

The broker's group timers are in [broker-group-timers.log](results/broker-group-timers.log).
Longest gap per partition around the join, over all four runs, next to the same measure over
the 10 s before it:

| Strategy | Handler | Partitions that moved | Partitions that stayed | Baseline | Handled twice | Missed |
|---|---|---|---|---|---|---|
| eager | instant | all six revoked; gaps 39–59 ms | — | 29–53 ms | 0 | 0 |
| eager | blocking | all six revoked; gaps 37–75 ms | — | 33–73 ms | 0 | 0 |
| cooperative | instant | 3: **517–537 ms** | 3: 28–51 ms | 34–49 ms | 0 | 0 |
| cooperative | blocking | 3: **512–679 ms** | 3: 41–141 ms | 37–70 ms | 0 | 0 |
| KIP-848 | instant | 3: **4.90–5.02 s** | 3: 39–50 ms | 33–53 ms | 0 | 0 |
| KIP-848 | blocking | 3: **4.98–6.53 s** | 3: 41–66 ms | 35–77 ms | 0 | 0 |

The eager rows are not a cost: every gap around the join sits inside the range the same
partitions showed with nobody joining. The revocation is in the log; its price is below
what this instrument can tell from an ordinary poll cycle.

"Missed" is the run's loss check: a record nobody handled although the group handled records
on both sides of it on that partition.

**Eager did revoke everything, and it cost nothing measurable here.** Every eager run shows
the whole assignment revoked and handed out again in a single round that took 10–20 ms, and
no partition paused for longer than it did in the baseline. When the round *starts* is a
different question: six of the eight runs began within 40 ms of the join and two began at
18.03 s, three seconds after it, because a member learns of a rebalance on its next
heartbeat — 3 s by default. Eager's stop lasts as long as the slowest
member takes to rejoin; with two members, a fast handler, a local network and batches that
never grew large even when blocking, that was tens of milliseconds. The cost the textbook
warns about scales with what this run does not have — many members, slow handlers holding a
rebalance, network round trips — and is not shown here.

**Cooperative stopped the moved partitions for longer than eager stopped any.** It
rebalances in two rounds: the first revokes what must move, the second assigns it. franz-go
notices the second round with a heartbeat it sends 500 ms after the first
(`cooperativeFastCheck` in its `consumer_group.go`), which is the ≈0.5 s every moved
partition waited. The three that stayed kept flowing at their baseline, with one exception: in the fourth
blocking run they paused 125–141 ms, twice their baseline, and the whole group once handled
nothing for 120 ms — still a quarter of what the moved partitions waited. On this stand the
two strategies trade a short stop everywhere against a half-second stop on what moves.

**KIP-848 hands over on the heartbeat.** The broker revokes from the old owner on one
heartbeat and assigns to the new one on a later one, and the consumer heartbeat interval is
`group.consumer.heartbeat.interval.ms` = 5 000 ms on this broker. In the instant runs the
moved partitions waited that interval to within 100 ms; with the blocking handler some
waited up to 1.5 s longer, which the heartbeat does not explain — the revoke-to-assign gap
was the same 5 s there, and the extra is on the handling side, not the protocol's, though
this run does not isolate where. The partitions that stayed kept flowing at their
baseline. Faster rebalancing is not what the new protocol
buys: it buys that the members that keep their partitions are never interrupted, and that
the assignment is computed on the broker.

**What the instrument got wrong, twice.** The first version reported hundreds of records
handled twice under cooperative and a few dozen under eager — its log was not kept, so the
numbers stay out of this file. The callback this experiment registers to log revocations
had replaced franz-go's default one, which is what commits the handled offsets before
partitions go; without it the new owner restarted from the last autocommit. The second
version committed everything the last poll returned, which is *more* than the default
commits — franz-go deliberately commits the previous poll, so that a revoke landing while
the handler is still working cannot mark unhandled records as done. The handler now marks
each record it has handled (`AutoCommitMarks`, `MarkCommitRecords`) and the callback commits
only those. The run also counts what nobody handled, not just what was handled twice: a gap
between the first and last record handled on a partition is exactly what committing ahead
of the handler would leave, and counting duplicates alone would have called that run
clean.

**Not shown:** eager's cost with many members or slow in-flight work, static membership
(`group.instance.id`), and a rebalance caused by a member leaving rather than joining. A
member stuck in its handler during a rebalance is [exp-16](../exp-16-lag-backpressure/).

## Cleaning up

```
make reset-topic TOPIC=exp14.payments
```
