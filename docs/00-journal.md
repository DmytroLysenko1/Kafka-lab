# Lab journal

One entry per run: what was expected, what the cluster did, and what was surprising. The
entries that matter most are the ones where the hypothesis did not survive contact — those
cannot be written without doing the run.

Numbers here are the source; the case files under [`static/`](static/) quote them.

## exp-01 — the key decides the order a payment is handled in

Date: 2026-09-20 · [run logs](../experiments/kafka_internals/exp-01-partition-keys/results/) ·
`make exp-01` — three sequential runs, each on freshly reset topics

**Hypothesis.** Without a key, the events of one payment scatter across partitions and lose
their business order. With `key=payment_id` they stay in one partition and the order holds.

**Setup.** Two topics of 6 partitions, RF 3, `min.insync.replicas=2`; 10 000 events over
100 payments (100 events each, ~100 bytes), produced twice — once with no key, once keyed
by `payment_id`. A single consumer reads from the start and records every event into
Postgres in handling order; violations are counted in Go over that order, not over offsets.

**Result.** Three runs:

| Run | Payments split across partitions | Order violations |
|---|---|---|
| keyless | 100 of 100, every time | **7 964 · 7 964 · 8 721** of 10 000 |
| keyed | 0 | **0 · 0 · 0** |

**What was surprising, twice.**

*The magnitude.* The plan predicted roughly 4 000 violations for the keyless run and the
cluster produced between six and eight thousand — up to four in five events. The estimate
had assumed a payment straddles two partitions; in fact all 100 were spread across several
of the six, because the producer switches partition once 64 KiB have gone to the current one
and each payment contributes only ~10 KB. (The instrument counts how many payments span more
than one partition, not how many partitions each spans, so "several" is as far as the
evidence goes — the exact spread is not measured.) The consumer then drains
partition by partition, so almost every event of a payment arrives after a later one.

*The spread.* The keyless number is not a constant — 7 964, 7 964 and 8 721 across three
sequential runs of identical input on freshly reset topics. Partition switching and the
order in which a fetch returns partitions both vary, so this is a shape, not a figure:
"most events, unpredictably how many". Two of the three landing on the same number is part
of the same picture — the variation is real but not large. The keyed column, by contrast, is
exactly zero every time, which is what a guarantee looks like next to a tendency.

**What the instrument got wrong, twice over.** The first version would have reported a
rerun as if it were a fresh run: it read the topic from offset 0 and stopped after N
records, so a second `make exp-01` would have counted the *previous* run's events while the
new ones sat unread past the cursor — a plausible, self-consistent, wrong number. Every
event now carries a run id, the consumer counts only its own, and the run refuses to report
unless the count matches exactly.

The run id was not enough. It keeps a rerun from *counting* the previous run's events; it
does not keep those events out of the partitions whose handling order is the thing being
measured. The three runs published before this one were taken on topics that were never
reset, and two of them overlapped in time on the same two topics — their numbers were
measured while another producer was writing into the same partitions. They are kept in
[`results/superseded/`](../experiments/kafka_internals/exp-01-partition-keys/results/superseded/) with what
is wrong with each. `run.sh` now resets both topics first, which is the rule the later
experiments were built on: an experiment that reads a log has to own the log it reads.

The count is also no longer computed from a slice in memory. The consumer writes each
handled event to Postgres with a `handled_seq`, and the report is computed from
`SELECT ... ORDER BY handled_seq` — so the number rests on the table the design has always
claimed it rests on.

**Conclusion.** The key is not a detail of throughput, it is the ordering unit. Ordering
survives only inside one partition, so "ordered" means "keyed by the entity whose order you
care about". The keyed run producing zero is also the control on the instrument: a non-zero
number there would have meant the counter, not Kafka, was broken.

**Carried into:** [`static/01-write-path.md`](static/01-write-path.md) step 4 and its
measurement table.

## exp-02 — a hot key, and what adding consumers buys

Date: 2026-09-20 · [run log](../experiments/kafka_internals/exp-02-hot-partition/results/run-2026-09-20-200615.log) ·
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
| 1 | 5.63 s | 0 | one member took all 20 000 |
| 2 | 5.12 s | 0 | 18 105 / 1 895 |
| 3 | 4.95 s | 0 | 17 894 / 1 053 / 1 053 |
| 6 | 4.81 s | 0 | 17 052 on one member, 211–842 on the rest |
| 7 | 4.76 s | 1 | same, and the seventh handled nothing |

The keys put 17 052 of 20 000 records — 85% — on partition 3.

**What this says.** Seven times the consumers bought 15%. Perfect spreading would have
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

**What the instrument got wrong, twice over.** The first version never committed an offset.
A consumer group with no committed offsets resets to the start of the partition on *every*
assignment, so a partition that moved between members during the join would have been
handled twice — the strict "handled exactly what was produced" check would then have failed
the whole run, discarding the group sizes already measured. It now commits as it works, so a
reassignment resumes, and a failed drain still prints the measurements made before it.

The second fault was the topic. Every drain reads from the start, so a topic that is never
reset makes each run fetch and decode every earlier run's records before reaching its own:
the drain times grow run by run, and the previously published seconds (6.03 down to 5.06)
were measured on a topic carrying twice the data of the run before it. The comparison drawn
from that — "the commits cost about half a second per drain" — was measuring commit cost
and extra scanning together, and is withdrawn. The run now resets its topic, and the clean
figures are 5.63 down to 4.76.

What did not change is the thing the experiment is about. The ratio was 16% on the dirty
topic and 15% on the clean one, and the partition distribution is identical to the record —
17 052 on p3 both times — because `murmur2` over the same keys is deterministic.

**Conclusion.** Parallelism is capped by partitions, and *useful* parallelism is capped by
the busiest partition. Two remedies, both with a price: change the key so the weight
spreads (`merchant_id` plus a bucket), or add partitions — which moves existing keys to
different partitions and breaks the ordering those keys used to have (exp-01).

**Carried into:** [`static/03-read-path.md`](static/03-read-path.md), step 7 and its
measurement table.

## exp-03 — what the cleaner keeps

Date: 2026-09-20 · [run log](../experiments/kafka_internals/exp-03-segments-retention/results/run-2026-09-20-200653.log) ·
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
| kept 5 s | 2 000 records, log starts at 0 | **0 records readable**, log starts past the last of them; the open segment holds only the instrument's own roll markers |

**What was surprising.** The plan said to squeeze `segment.bytes` down to kilobytes so the
active segment rolls quickly. Kafka 4.x refuses: the minimum is 1 MiB, confirmed by the
broker rejecting 4 096 with "Value must be at least 1048576". Rolling has to be driven by
`segment.ms` instead. A piece of advice that was true for years is now simply invalid, and
the only way to find that out was to run it.

**The tombstones did not disappear**, and that is correct rather than a failure: compaction
keeps them for `delete.retention.ms` so that every consumer still reading the log gets a
chance to learn that the key is gone. A tombstone is a record that says "deleted", not an
absence — and it is removed by a later pass, not by the one that compacts the values.

**Retention removed everything, and what was left was not what the hypothesis predicted.**
The hypothesis said a topic holds *more* than its setting suggests, because the open segment
escapes deletion. The run showed the opposite: all 2 000 records were dropped, the log now
starts past the last of them, and the only thing still readable is the handful of
segment-roll markers the instrument itself wrote seconds earlier. So what was demonstrated
is the clean half — every closed segment aged out and went whole — and not the interesting
half, because the experiment never produced a record that was older than `retention.ms` and
still readable. To show that, the open segment has to be closed *late*, trapping aged data
inside it; this run closes segments continuously, so aged data always lands in a closed
segment and always goes. "Retention 5 s" means "closed segments older than 5 s are dropped";
how much a topic actually keeps beyond that is still unmeasured here, and is now listed as
outstanding in the case file.

**What the instrument got wrong first.** Two faults, one per run. The first run counted the
segment-roll markers the experiment writes as data, and reported 41 live keys plus 10
deleted — 51 keys out of a possible 50, which is how it was noticed. The second inherited
the previous run's log, its retention topic starting exactly where the first one ended. The
markers are now counted apart, and the run resets its own topics before measuring — the same lesson as exp-01, in a
different shape: an experiment that reads a log has to own the log it reads.

**Conclusion.** Compaction is a snapshot of state, not a history, and it is worth taking
only when the reader wants current values — which is the shape event-carried state transfer
needs. Neither policy is instant: both act on closed segments, and the open one is always
exempt.

**Carried into:** [`static/02-log-segments-retention.md`](static/02-log-segments-retention.md),
its compaction traps table and measurement row.

## exp-04 — what a dead broker costs, and what recovery does not do

Date: 2026-09-20 · [run log](../experiments/kafka_internals/exp-04-isr-leader-election/results/run-2026-09-20-200722.log) ·
`make exp-04`

**Hypothesis.** Killing the broker that leads a partition costs a pause, not data: a
replica from the ISR takes over, `acks=all` keeps being honoured because two replicas
remain in sync, and once the broker is back everything returns to how it was.

**Setup.** One topic, 3 partitions, RF 3, `min.insync.replicas=2`,
`unclean.leader.election.enable=false`; 300 records per phase with `acks=all`. Four
phases, each a separate process, so the shell can `docker kill` the broker leading
partition 0 between them and time the cluster's reaction from the kill itself. The binary
is built once before the first phase: `go run` compiles on first call, and on a cold build
cache that compile would land inside the interval being timed. `kill`, not `stop`: SIGTERM
gives Kafka a controlled shutdown, which hands leadership over politely and measures the
good case. Broker IDs below are whatever the assignment produced on this run — the result
is the shape, not the identities.

**Result.**

| Phase | Leaders | Smallest ISR | Writes accepted |
|---|---|---|---|
| baseline | `[2 3 1]` | 3 of 3 | 300 of 300 |
| degraded, kafka2 killed | `[3 3 1]` | 2, with `min.insync.replicas` 2 as the broker reports it | 300 of 300 |
| recovered, kafka2 back | `[3 3 1]` | 3 of 3 | — |
| after preferred election | `[2 3 1]` | 3 of 3 | — |

| What | Within | Of that, spent polling |
|---|---|---|
| kill → ISR shrinks and no partition is leaderless | **10.6 s** | 10.6 s |
| restart → ISR whole again | **5.1 s** | 5.1 s |
| preferred election → leadership back on the preferred replica | **0 s** | 0 s |

Every interval is an upper bound measured from the event the shell timed, and the second
column is how much of it the measuring process spent actually watching. The first two rows
are honest intervals: the cluster changed while we were looking. The third is degenerate —
leadership was already back at the first poll — and that is the whole finding for it. An
earlier version of this table read 0.2 s there and 10.9 s above; both figures included the
next process starting up, and the 0.2 s was *nothing but* startup. The instrument now
prints the polling time next to the bound so a degenerate row cannot be mistaken for a
measurement.

**What was surprising.** The reaction took 10.6 s — 10.1 s on the run before it — and the
number everyone quotes for "a replica leaves the ISR", `replica.lag.time.max.ms` at 30 s,
cannot be the one that applies. A crashed broker is not a slow follower: the controller
stops receiving its heartbeats and fences it after `broker.session.timeout.ms`, and fencing
rewrites the ISR of every partition that broker belonged to at once. The three defaults are
committed as evidence —
[`results/broker-timers.log`](../experiments/kafka_internals/exp-04-isr-leader-election/results/broker-timers.log):
`broker.session.timeout.ms=9000`, `broker.heartbeat.interval.ms=2000`,
`replica.lag.time.max.ms=30000`.

For a while this entry could only argue that. Nothing in the run varied either timer, so
"ten-odd seconds fits 9 s and cannot fit 30 s" was circumstantial, however strong. exp-04c
varies it, and the reaction moves with it — the entry below.

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

**Recovery did only half of what recovery sounds like.** The replica rejoined the ISR 5.1 s
after the restart, and partition 0 was still led by the broker that replaced it, and stayed
that way. `auto.leader.rebalance.enable` is off on this stand deliberately — its 300 s timer
would move leadership in the middle of a measurement — so the preferred election is a step
someone has to run. It was already done at the first poll — see the table above. Left undone
after each restart, leadership drifts onto
whichever brokers happened to survive, and a cluster that looks healthy is quietly serving
its partitions from two machines instead of three.

**What this run does not show.** Unclean leader election needs the ISR to collapse onto a
replica that is behind, and on three combined broker/controller nodes that means killing
two — which destroys the KRaft quorum, so the controller cannot rewrite an ISR at all. The
produce then hangs instead of failing, which demonstrates nothing about the flag. The same
constraint already forced the redesign of exp-08.

**Conclusion.** RF 3 with `min.insync.replicas=2` survives exactly one broker, loudly for
about ten seconds and silently after that. The reaction time fits the heartbeat session and
cannot fit the lag timer — the argument of this entry, not a result of it, until exp-04c
runs; recovery restores replication by itself and leadership never;
and the margin during the degraded window is zero, which is the number worth alerting on —
not the writes, which keep succeeding right up until they do not.

**Carried into:** [`static/04-isr-leader-election.md`](static/04-isr-leader-election.md),
its measurement table.

## exp-04c — the same kill, with the session timeout moved

Date: 2026-09-21 · [run log](../experiments/kafka_internals/exp-04-isr-leader-election/results/exp-04c-2026-09-21-172333.log) ·
`make exp-04c`

**Hypothesis.** exp-04 explained its ten-second reaction by `broker.session.timeout.ms`
rather than `replica.lag.time.max.ms`. If that is right, raising the session timeout and
changing nothing else moves the reaction by the same amount. If the lag timer were
governing it, the reaction would not move at all.

**Setup.** The same experiment, the same topic and the same kill. The brokers are recreated
with `broker.session.timeout.ms=20000` instead of 9 000 — the named volumes survive, so the
topic and its data are the ones exp-04 used. `replica.lag.time.max.ms` is left at its
default. The run prints the timers the broker is enforcing before it measures anything, and
puts the default back on every exit path.

**Result.**

| `broker.session.timeout.ms` | `replica.lag.time.max.ms` | kill → ISR shrinks |
|---|---|---|
| 9 000 (default) | 30 000 | 10.1 s · 10.6 s |
| **20 000** | 30 000 | **20.3 s** |

The broker's own config listing is in the log above the measurement:
`broker.session.timeout.ms=20000 synonyms={STATIC_BROKER_CONFIG:...=20000,
DEFAULT_CONFIG:...=9000}`.

**What this settles.** The reaction tracked the session timeout across an 11-second change
while the lag timer sat at 30 s throughout. exp-04's explanation was an argument from
config values — the kind that is usually right and occasionally, expensively, wrong — and
it is now a measurement. Both figures land about 0.3–1.6 s above their timeout, which is
the heartbeat interval (2 s) plus the controller's own work plus this experiment's 250 ms
polling: a broker is fenced when its session expires, and a session expires some time after
the last heartbeat that would have renewed it, not the instant the process dies.

**What did not move.** ISR recovery after the restart stayed at 5.1 s, unchanged from
exp-04. That is the returning replica catching up, which has nothing to do with fencing —
a useful control: had recovery moved too, the change would have been something broader than
the timer under test.

**Why this matters beyond the number.** "Kafka takes 30 seconds to notice a dead broker" is
the received wisdom, and it is wrong for the case people mean by it. Tuning
`replica.lag.time.max.ms` down to make failover faster would do nothing for a crash and
would make the ISR twitchy under ordinary GC pauses — the setting it would actually change
is the one for a slow follower. The alert threshold and the failover budget both belong on
`broker.session.timeout.ms`.

**Carried into:** [`static/04-isr-leader-election.md`](static/04-isr-leader-election.md),
its measurement table, and the timers table in [`static/README.md`](static/README.md).

## exp-05/06/07 — the three semantics as numbers

Date: 2026-09-21 ·
[run log](../experiments/transaction_guarantee/exp-05-delivery-semantics/results/run-2026-09-21-175334.log) ·
`make exp-05`

**Hypothesis.** At-most-once, at-least-once and effectively-once are not three libraries or
three settings. They are one ordering decision — where the offset is committed relative to
the write — and a crash in the wrong place turns each into a different kind of incident.

**Setup.** One topic of 3 partitions, RF 3. 1 000 payments produced with `acks=all`, then a
consumer with `DisableAutoCommit` reads them in batches of 50 and writes each to Postgres.
Partway through, the consumer sends itself `SIGKILL` — a real crash, so no deferred close
runs and nothing is flushed — and a second process resumes from whatever the first managed
to commit. The counts are read back out of Postgres, never out of the dead process's
memory. Three runs, identical but for the commit order.

**Result.**

| Mode | Commit order | Rows | Distinct | Outcome |
|---|---|---|---|---|
| at-most-once | commit, then write | 951 | 951 | **49 lost** |
| at-least-once | write, then commit | 1 050 | 1 000 | **50 duplicated** |
| inbox | claim and write in one transaction, then commit | 1 000 | 1 000 | **exactly once** |

**The two failures are the same window from both sides.** One batch, 50 records. Committed
before writing, the 49 that had not been written when the process died are gone — the
offsets say they were handled, so no restart will ever fetch them. Written before
committing, the whole batch is fetched again and charged twice. Same crash, same instant,
opposite failure, and the only difference in the code is which of two statements comes
first.

**The inbox run was delivered the duplicates too.** That is the sentence worth keeping:
exp-07 did not receive its records once. It received 50 of them twice, exactly as exp-06
did, and still holds 1 000 rows, because the claim and the write are a single transaction
and the second delivery loses the race for the primary key. *Exactly-once effect,
at-least-once delivery* — Kafka never promised the first and always gave the second.

**What the instrument got wrong first, twice.**

*The resumed consumer hung.* The first live run timed out with "332 left". Progress was
being tracked as "have I read up to the last offset of every partition", and a consumer
replacing a killed one is never sent the records its predecessor already committed — so a
partition the predecessor finished held the run open forever. It now seeds progress from
the group's committed offsets, so a partition someone else finished is finished. The
failure mode is worth naming: a hang reported as a timeout, in the middle of an experiment
about losing records.

*The crash landed where it cost nothing.* With the kill set at 500 handled records and
batches of 50, the at-most-once run died exactly on a batch boundary — the offsets that had
been committed covered precisely what had been written, nothing was lost, and the report
read "every payment exactly once" for the semantics whose entire purpose is to lose
payments. It was caught because the run states what each mode must demonstrate and compares
the outcome to it, rather than printing three numbers and leaving the reader to notice. The
kill now refuses to land on a batch boundary under at-most-once. A crash that costs nothing
proves nothing.

**Conclusion.** There is no configuration flag for exactly-once between Kafka and a
database. At-most-once and at-least-once are both one line away from each other, and the
gap between them is where payments live or die. What closes it is not a stronger delivery
guarantee but an idempotent write: the consumer stops caring how many times a record
arrives. That is the same shape as the outbox on the way out ([06](static/06-transactional-outbox.md)),
and it is why exp-10 will matter — Kafka transactions do cover read-process-write, and they
stop at the database boundary.

**Carried into:** [`static/05-delivery-semantics.md`](static/05-delivery-semantics.md),
its measurement table.
