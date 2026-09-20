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

## exp-04 — what a dead broker costs, and what recovery does not do

Date: 2026-09-20 · [run logs](../experiments/exp-04-isr-leader-election/results/) ·
`make exp-04`

**Hypothesis.** Killing the broker that leads a partition costs a pause, not data: a
replica from the ISR takes over, `acks=all` keeps being honoured because two replicas
remain in sync, and once the broker is back everything returns to how it was.

**Setup.** One topic, 3 partitions, RF 3, `min.insync.replicas=2`,
`unclean.leader.election.enable=false`; 300 records per phase with `acks=all`. Four
phases, each a separate process, so the shell can `docker kill` the broker leading
partition 0 between them and time the cluster's reaction from the kill itself rather than
from when the next process happened to start. `kill`, not `stop`: SIGTERM gives Kafka a
controlled shutdown, which hands leadership over politely and measures the good case.

**Result.**

| Phase | Leaders | Smallest ISR | Writes accepted |
|---|---|---|---|
| baseline | `[3 1 2]` | 3 of 3 | 300 of 300 |
| degraded, kafka3 killed | `[1 1 2]` | 2, and `min.insync.replicas` is 2 | 300 of 300 |
| recovered, kafka3 back | `[1 1 2]` | 3 of 3 | — |
| after preferred election | `[3 1 2]` | 3 of 3 | — |

| What | How long |
|---|---|
| kill → ISR shrinks and partition 0 has a new leader | **10.9 s** |
| restart → ISR whole again | **5.3 s** |
| preferred election → leadership back on the preferred replica | **0.2 s** |

**What was surprising.** The reaction took 10.9 s, and the number everyone quotes for
"a replica leaves the ISR" — `replica.lag.time.max.ms`, 30 s by default — is not the one
that applies. A crashed broker is not a slow follower. The controller stops receiving its
heartbeats and fences it after `broker.session.timeout.ms`, which is 9 s with a heartbeat
every 2 s, and fencing rewrites the ISR of every partition that broker belonged to at once.
Both defaults were read back off the running broker (`kafka-configs --describe --all`)
rather than recalled: `broker.session.timeout.ms=9000`, `broker.heartbeat.interval.ms=2000`,
`replica.lag.time.max.ms=30000`. The lag timer governs a *live* follower that has fallen
behind; the session timeout governs one that is gone. They are two different failure
detectors, and only the second one was exercised here.

**One dead broker degraded every partition — 3 of 3.** With RF 3 on three brokers, every
broker holds a replica of every partition, so there is no partition that a failure can
miss. That is the shape of a small cluster rather than a fault: the ratio only improves
once there are more brokers than replicas. It is also why the under-replicated metric on a
three-node stand is binary in practice — it reads 0 or everything.

**Writes never stopped, and there was no margin left.** The smallest ISR was 2 and
`min.insync.replicas` is 2 — exactly at the threshold, not above it. The cluster answered
every one of the 300 records, which is the good news, and one more failure would have
turned the same `acks=all` call into `NOT_ENOUGH_REPLICAS`, which is exp-08. A dashboard
showing "writes fine" during this phase is telling the truth and hiding the important part.

**Recovery did only half of what recovery sounds like.** The replica rejoined the ISR 5.3 s
after the restart, and partition 0 was still led by the broker that replaced it, and stayed
that way. `auto.leader.rebalance.enable` is off on this stand deliberately — its 300 s timer
would move leadership in the middle of a measurement — so the preferred election is a step
someone has to run. It took 0.2 s. Left undone after each restart, leadership drifts onto
whichever brokers happened to survive, and a cluster that looks healthy is quietly serving
its partitions from two machines instead of three.

**What this run does not show.** Unclean leader election needs the ISR to collapse onto a
replica that is behind, and on three combined broker/controller nodes that means killing
two — which destroys the KRaft quorum, so the controller cannot rewrite an ISR at all. The
produce then hangs instead of failing, which demonstrates nothing about the flag. The same
constraint already forced the redesign of exp-08.

**Conclusion.** RF 3 with `min.insync.replicas=2` survives exactly one broker, loudly for
about eleven seconds and silently after that. The failure is detected by the heartbeat
session, not by the lag timer; recovery restores replication by itself and leadership never;
and the margin during the degraded window is zero, which is the number worth alerting on —
not the writes, which keep succeeding right up until they do not.

**Carried into:** [`static/04-isr-leader-election.md`](static/04-isr-leader-election.md),
its measurement table.
