# Lab journal

One entry per run: what was expected, what the cluster did, and what was surprising. The
entries that matter most are the ones where the hypothesis did not survive contact — those
cannot be written without doing the run.

Numbers here are the source; the case files under [`static/`](static/) quote them.

## exp-01 — the key decides the order a payment is handled in

Date: 2026-09-20 · [run logs](../experiments/exp-01-partition-keys/results/) ·
`make exp-01`

**Hypothesis.** Without a key, the events of one payment scatter across partitions and lose
their business order. With `key=payment_id` they stay in one partition and the order holds.

**Setup.** Two topics of 6 partitions, RF 3, `min.insync.replicas=2`; 10 000 events over
100 payments (100 events each, ~100 bytes), produced twice — once with no key, once keyed
by `payment_id`. A single consumer reads from the start and records every event into
Postgres in handling order; violations are counted in Go over that order, not over offsets.

**Result.** Three runs:

| Run | Payments split across partitions | Order violations |
|---|---|---|
| keyless | 100 of 100, every time | **6 448 · 7 838 · 7 964** of 10 000 |
| keyed | 0 | **0 · 0 · 0** |

**What was surprising, twice.**

*The magnitude.* The plan predicted roughly 4 000 violations for the keyless run and the
cluster produced between six and eight thousand — up to four in five events. The estimate
had assumed a payment straddles two partitions; in fact every one of the 100 payments had
its events spread over all six, because the producer switches partition once 64 KiB have
gone to the current one and each payment contributes only ~10 KB. The consumer then drains
partition by partition, so almost every event of a payment arrives after a later one.

*The spread.* The keyless number is not a constant — 6 448, 7 838 and 7 964 across three
runs of identical input. Partition switching and the order in which a fetch returns
partitions both vary, so this is a shape, not a figure: "most events, unpredictably how
many". The keyed column, by contrast, is exactly zero every time, which is what a guarantee
looks like next to a tendency.

**What the instrument got wrong first.** The first version of the experiment would have
reported a rerun as if it were a fresh run: it read the topic from offset 0 and stopped
after N records, so a second `make exp-01` would have counted the *previous* run's events
while the new ones sat unread past the cursor — a plausible, self-consistent, wrong number.
Every event now carries a run id, the consumer counts only its own and refuses to report
unless the count matches exactly. The fix is visible in the numbers above: two runs
executed back to back returned 6 448 and 7 964, where the broken version would have
returned the same figure twice.

**Conclusion.** The key is not a detail of throughput, it is the ordering unit. Ordering
survives only inside one partition, so "ordered" means "keyed by the entity whose order you
care about". The keyed run producing zero is also the control on the instrument: a non-zero
number there would have meant the counter, not Kafka, was broken.

**Carried into:** [`static/01-write-path.md`](static/01-write-path.md) step 4 and its
measurement table.

## exp-02 — a hot key, and what adding consumers buys

Date: 2026-09-20 · [run logs](../experiments/exp-02-hot-partition/results/) ·
`make exp-02`

**Hypothesis.** Key skew turns one partition into the bottleneck, and adding consumers does
not fix it.

**Setup.** One topic of 6 partitions, RF 3; 20 000 events over 20 merchants with 80% of
them keyed to a single merchant. Each record costs the handler a fixed 200 µs, so the drain
time measures handling capacity rather than the network. The same data is then drained by
consumer groups of 1, 2, 3, 6 and 7 members, each group reading the topic from the start.

**Result.**

| Consumers | Time to drain | Idle members | How the work fell |
|---|---|---|---|
| 1 | 6.03 s | 0 | one member took all 20 000 |
| 2 | 5.49 s | 0 | 18 105 / 1 895 |
| 3 | 5.47 s | 0 | 17 894 / 1 053 / 1 053 |
| 6 | 5.19 s | 0 | 17 052 on one member, 211–842 on the rest |
| 7 | 5.06 s | 1 | same, and the seventh handled nothing |

The keys put 17 052 of 20 000 records — 85% — on partition 3.

**What this says.** Seven times the consumers bought 16%. Perfect spreading would have
drained in about a sixth of the single-consumer time; the floor here is the hot partition,
because a partition is handled by exactly one member of a group however many members there
are. The seventh consumer is the other half of the same rule: with six partitions there was
nothing left to give it, so it sat idle while one of its peers worked through 85% of the
topic.

**What was surprising.** The skew came out sharper than the keys suggest: 80% of the events
were addressed to one merchant, but 85% of the records landed on one partition. The extra
1 052 records are five of the nineteen cold merchants that `murmur2` happened to place on
partition 3 as well — and the cold keys spread 5/4/4/3/2/1 across the six partitions, not
evenly, because hashing nineteen keys onto six slots has no reason to be fair. Key skew and
partition skew are different numbers, and it is the second one that decides the drain.

**What the instrument got wrong first.** The first version of this experiment never
committed an offset. A consumer group with no committed offsets resets to the start of the
partition on *every* assignment, so a partition that moved between members during the join
would have been handled twice — the strict "handled exactly what was produced" check would
then have failed the whole run, discarding the group sizes already measured. It now commits
as it works, so a reassignment resumes, and a failed drain still prints the measurements
made before it. The commits cost about half a second per drain: the earlier, unsafe
instrument read 5.67 s down to 4.18 s, where the trustworthy one reads 6.03 s down to
5.06 s. The ratio is what the experiment is about, and it did not improve — it got worse,
from 26% to 16%.

**Conclusion.** Parallelism is capped by partitions, and *useful* parallelism is capped by
the busiest partition. Two remedies, both with a price: change the key so the weight
spreads (`merchant_id` plus a bucket), or add partitions — which moves existing keys to
different partitions and breaks the ordering those keys used to have (exp-01).

**Carried into:** [`static/03-read-path.md`](static/03-read-path.md), step 7 and its
measurement table.

## exp-03 — what the cleaner keeps

Date: 2026-09-20 · [run logs](../experiments/exp-03-segments-retention/results/) ·
`make exp-03`

**Hypothesis.** A compacted topic keeps the last value of each key rather than the history,
and it only shows that once a segment is closed. Retention deletes whole segments, so a
topic always holds more than its setting says.

**Setup.** Two single-partition topics, RF 3. The compacted one takes 50 keys × 40 updates
and then a tombstone for 10 of those keys; the retention one takes 2 000 records and is
kept for 5 seconds. Both roll segments on `segment.ms=1000`, and the run waits for the log
to change rather than sleeping for a guessed while.

**Result.**

| Topic | Before cleaning | After cleaning |
|---|---|---|
| compacted | 2 010 records, 40 live keys, 10 tombstones | **50 records**, 40 live keys, 10 tombstones |
| kept 5 s | 2 000 records, log starts at 0 | **0 records readable**, log starts at 2 000, open segment survives |

**What was surprising.** The plan said to squeeze `segment.bytes` down to kilobytes so the
active segment rolls quickly. Kafka 4.x refuses: the minimum is 1 MiB, confirmed by the
broker rejecting 4 096 with "Value must be at least 1048576". Rolling has to be driven by
`segment.ms` instead. A piece of advice that was true for years is now simply invalid, and
the only way to find that out was to run it.

**The tombstones did not disappear**, and that is correct rather than a failure: compaction
keeps them for `delete.retention.ms` so that every consumer still reading the log gets a
chance to learn that the key is gone. A tombstone is a record that says "deleted", not an
absence — and it is removed by a later pass, not by the one that compacts the values.

**Retention removed everything and left something.** After cleaning, the log starts at
offset 2 000: every record produced had aged past 5 seconds and its segments were dropped
whole. What is still readable is the open segment, which is never deleted however old it
is. "Retention 5 s" therefore means "closed segments older than 5 s are dropped", and the
amount of data actually kept depends on how quickly segments roll — not on the setting
alone.

**What the instrument got wrong first.** The second run of the experiment reported 41 live
keys instead of 40: the segment-roll markers the experiment writes were counted as data,
and records from the previous run were still in the log. The markers are now counted apart,
and the run resets its own topics before measuring — the same lesson as exp-01, in a
different shape: an experiment that reads a log has to own the log it reads.

**Conclusion.** Compaction is a snapshot of state, not a history, and it is worth taking
only when the reader wants current values — which is the shape event-carried state transfer
needs. Neither policy is instant: both act on closed segments, and the open one is always
exempt.

**Carried into:** [`static/02-log-segments-retention.md`](static/02-log-segments-retention.md),
its compaction traps table and measurement row.
