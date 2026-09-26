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

Date: 2026-09-21 · `make exp-05`, `make exp-06`, `make exp-07` · run logs:
[at-most-once](../experiments/transaction_guarantee/exp-05-at-most-once/results/run-2026-09-21-180311.log) ·
[at-least-once](../experiments/transaction_guarantee/exp-06-at-least-once/results/run-2026-09-21-180404.log) ·
[inbox](../experiments/transaction_guarantee/exp-07-inbox/results/run-2026-09-21-180500.log)

**Hypothesis.** At-most-once, at-least-once and effectively-once are not three libraries or
three settings. They are one ordering decision — where the offset is committed relative to
the write — and a crash in the wrong place turns each into a different kind of incident.

**Setup.** Three experiments, one harness. Each owns a topic of 3 partitions, RF 3 — they
are not allowed to share one, because a consumer group resumes from committed offsets and a
topic carrying a sibling's records would replay them into this one's count. 1 000 payments
produced with `acks=all`, then a
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


## exp-08 — what `acks` promises, and what it only implies

Date: 2026-09-21 · `make exp-08` · run logs:
[acks=1](../experiments/transaction_guarantee/exp-08-acks/results/acks-one-2026-09-21-183122.log) ·
[acks=all](../experiments/transaction_guarantee/exp-08-acks/results/acks-all-2026-09-21-183122.log)

**Hypothesis.** `acks=1` acknowledges a record the moment the leader has it, so killing the
leader afterwards loses a record the caller was told was safe. `acks=all` on a topic whose
`min.insync.replicas` is above the number of live replicas refuses the write instead —
failing loudly rather than lying quietly.

**Setup.** Two single-partition topics, RF 3. `exp08.acks1` takes 2 000 records of 4 KB
with `acks=1`; its leader is then killed. `exp08.isr3` declares `min.insync.replicas=3`,
takes the same 2 000 records with `acks=all` while every broker is up, and then the same
again with one broker killed — one, because killing two of three combined broker and
controller nodes takes the KRaft quorum with it and the produce hangs instead of being
refused.

**Result — the second half, which is the one that matters operationally.**

| Cluster | `acks=all`, `min.insync.replicas=3` |
|---|---|
| every broker up | **2 000 of 2 000 accepted** |
| one broker down | **0 of 2 000 accepted — `NOT_ENOUGH_REPLICAS`** |

Not one record was written and not one was lost, because not one was accepted. The producer
was told no. That is the whole value of the setting: `acks=all` without
`min.insync.replicas` above one means "everyone currently in the in-sync set", and an
in-sync set of one is one machine — so the guarantee that reads as the strongest available
is exactly as strong as the number you did not set.

**Result — the first half, which did not happen.**

| | |
|---|---|
| acknowledged with `acks=1` | 2 000 of 2 000 |
| readable after the leader was killed | **2 000** |
| lost | **0** |

The window is real and it did not open. The followers had fetched all 8 MB before the kill
landed, so the new leader had everything. This is the honest shape of `acks=1`: it does not
lose records often, it loses them rarely — and a failure mode that fires rarely and reports
nothing is worse to operate than one that fires predictably. The plan anticipated exactly
this ("at the first run there were no losses, it had time to replicate") and it is still
the right call to record the negative rather than tune the run until it produces the
expected answer.

**What was tried to open the window, and why it cannot work.** A race is a lottery, not an
experiment, so the first version of this run held the followers back with Kafka's own
replication throttle — `leader.replication.throttled.replicas` and
`follower.replication.throttled.replicas` on the topic, with the broker rates set to one
byte per second. The configs applied and were read back from the cluster. Replication was
untouched: an 8.3 MB log reached all three replicas instantly. Replication quotas exist for
reassignment traffic, where the destination replica is not in the in-sync set; they do not
restrain replicas that already are. That is a mechanism worth knowing before planning any
test around throttling, and it is stated here as the explanation for an observation, not as
something this run verified in Kafka's source.

**Something the throttle attempt exposed on the way.** The first padded run wrote 2 000
records of 4 KB and produced a 616 KB log. franz-go compresses with snappy by default,
unlike the Java client, and 4 KB of repeated characters compresses to nearly nothing — so
the whole log fitted in a single fetch, which would have made any throttle irrelevant even
if throttles applied. The producer here now disables compression explicitly. It is the
same default the tuning checklist already flags as a franz-go/Java divergence, met in the
wild rather than read off a page.

**What the instrument got wrong first.** The count phase asked for the log's end offset and
ignored the per-partition error in the reply. A leader mid-election answers with
`OFFSET_NOT_AVAILABLE` and an offset of −1, the read loop then ran zero times, and the
report printed "0 records readable" — which is precisely this experiment's claimed finding,
manufactured by the instrument rather than by Kafka. It now refuses to report until the
partition gives usable offsets. The first run of half one printed that false zero, and it
would have been published as a loss.

**Conclusion.** `acks=all` is not a guarantee on its own; it is a guarantee about a set
whose size you choose with `min.insync.replicas`. Set it above one and the cluster refuses
writes it cannot make durable — an outage instead of silent loss, which for payments is the
trade you want. Set it to one, or use `acks=1`, and you are relying on replication winning
a race nobody measures and nobody alerts on.

**Carried into:** [`static/01-write-path.md`](static/01-write-path.md) and
[`static/04-isr-leader-election.md`](static/04-isr-leader-election.md).


## exp-09 — the failure the advice warns about is not the one this client has

Date: 2026-09-21 · `make exp-09` · run logs:
[idempotence off](../experiments/transaction_guarantee/exp-09-reordering/results/plain-2026-09-21-184103.log) ·
[idempotent](../experiments/transaction_guarantee/exp-09-reordering/results/idempotent-2026-09-21-184103.log)

**Hypothesis.** With idempotence off and more than one request in flight, a retried batch
can land after a later one, so a refund is recorded before the capture it refunds. With
idempotence on, the broker's sequence numbers make that impossible. This is the standard
teaching, and it is written with the Java client in mind.

**Setup.** One partition, RF 3, `min.insync.replicas=3`, so freezing a single follower
makes every `acks=all` request fail with `NOT_ENOUGH_REPLICAS` — a retriable error, which
is what makes the producer retry. A steady stream for two minutes, each payment a capture
followed by its refund, keyed by payment, with small batches and no linger so several
requests are in flight at once. During the stream a follower is frozen with `docker pause`
and thawed, three times; every wait is on the cluster's own state, not a sleep. Then the
whole partition is read back and checked for events out of order, refunds before captures,
duplicates and gaps. Run twice: idempotence off with five requests in flight, then
idempotent.

**Result.**

| Producer | Produced | Read back | Out of order | Refund before capture | Duplicated | Missing |
|---|---|---|---|---|---|---|
| idempotence off, 5 in flight | 217 840 | 218 131 | **0** | 0 | **291** | 0 |
| idempotent | 266 980 | 266 980 | 0 | 0 | 0 | 0 |

**What was surprising: franz-go did not reorder. It duplicated.** Not one event arrived out
of order in either run, and the idempotence-off run wrote 291 events twice. That is not
luck. It follows from how the client handles a retriable failure, read from its source
before the run and confirmed by it: on a failure franz-go rewinds the partition to its
*oldest* pending batch (`resetBatchDrainIdx` sets the drain index to zero) and refuses to
pipeline that partition again until a request succeeds (`okOnSink` goes false). Everything
after the failed batch is resent after it, in order — including batches the broker had in
fact already written but whose acknowledgement the client never saw. Order survives;
exactly-once does not.

**Why that matters more than it sounds.** The advice in the tuning checklist said, with
this experiment's number on it, that turning idempotence off with five in flight makes
`Refunded` overtake `Captured`. For this client it does not, and the checklist now says
what does happen. The conclusion is unchanged — leave idempotence on — but the reason is
different, and so is the symptom anyone debugging it would be looking for. A team hunting
for reordered events would find none and conclude the configuration was fine, while
charging a few hundred customers twice.

**The control held exactly.** 266 980 events, 266 980 read back, zero of anything, through
the same three refusal windows. Idempotence is not a performance setting; it is what turns
the producer's retries from a source of duplicates into a no-op.

**What this run does not show.** Reordering in franz-go is not proven impossible: its own
documentation says more than one request in flight without idempotence "may result in out
of order records", and three refusal windows are three chances, not a proof. What was shown
is that under this failure — a retriable refusal from the leader — the client's rewind
turns the textbook reordering into duplication. A failure that loses one request on the
wire while a later one lands, rather than refusing both, is the case left open.

**What the instrument got wrong first.** Nothing numerical, but the run script had the
same flaw as exp-08's: the frozen broker's id was set inside a piped block, which is a
subshell, so the clean-up trap outside could not see it. Had a wait failed while a follower
was frozen, the broker would have been left paused — answering nothing and hanging every
later experiment. The clean-up now thaws all three brokers unconditionally.

**Carried into:** [`static/01-write-path.md`](static/01-write-path.md) and the idempotence
and in-flight rows of [`tuning-checklist.md`](tuning-checklist.md).


## exp-10 — what a Kafka transaction covers, and where it stops

Date: 2026-09-21 · `make exp-10a` … `make exp-10d` · run logs:
[10a](../experiments/transaction_guarantee/exp-10a-rollback/results/run-2026-09-21-185537.log) · [10b](../experiments/transaction_guarantee/exp-10b-database-boundary/results/run-2026-09-21-185649.log) · [10c](../experiments/transaction_guarantee/exp-10c-read-uncommitted/results/run-2026-09-21-185751.log) · [10d](../experiments/transaction_guarantee/exp-10d-hanging-transaction/results/run-2026-09-21-190032.log)

**Hypothesis.** A Kafka transaction makes a read-process-write loop exactly-once: the
output records and the consumed offsets commit together or not at all. It does not reach
anything outside Kafka — a database write in the same loop survives the abort — and it has
a cost that lands on readers who never asked for transactions.

**Setup.** Three experiments share one loop: 1 000 payments on an input topic, consumed in
batches of 50, each batch one transaction that writes the payments to an output topic and
commits the consumed offsets with it. After producing the eleventh batch and before ending
its transaction, the processor sends itself `SIGKILL`. A replacement starts with the same
transactional id — which is how the broker learns the old transaction is dead and aborts it
— resumes from the last committed offsets, and finishes. exp-10b also writes each payment
to Postgres inside the loop. The output is read back twice, `read_committed` and
`read_uncommitted`. exp-10d is separate: one producer opens a transaction and never ends it,
another writes after it with no transaction, and both isolation levels are timed.

**Result.**

| Run | `read_committed` | `read_uncommitted` | Postgres |
|---|---|---|---|
| 10a — rollback | **1 000, 1 000 distinct** | 1 050 | — |
| 10b — database in the loop | **1 000, 1 000 distinct** | 1 050 | **1 050, 50 duplicated** |
| 10c — `read_uncommitted` | 1 000, 1 000 distinct | **1 050** | — |

| 10d — a transaction left open | |
|---|---|
| high watermark / last stable offset | 101 / 0 |
| `read_uncommitted` saw the unrelated records after | 0 s |
| `read_committed` saw them after | **23.2 s**, with a 20 s transaction timeout |

**The transaction held exactly where it claims to.** The killed batch's 50 output records
and its consumed offsets rolled back as one: the replacement reprocessed the batch, and a
`read_committed` reader saw every payment once. That is the whole of what Kafka's
exactly-once promises, and it kept the promise under a real crash.

**And not one step further.** exp-10b is the same run with a Postgres write in the loop.
The Kafka output is still exactly 1 000. The database holds 1 050 rows: the 50 written for
the killed batch survived the abort, because nothing ever told Postgres a transaction
existed, and the replacement wrote them again. This is the misreading case 10 exists to
prevent — reaching for Kafka transactions to protect a database write — and it looks
correct right up until the process dies in the wrong millisecond. What makes the database
side safe is the inbox (exp-07); what makes the Kafka side safe is the transaction. Neither
covers the other.

**The aborted records were written; they were only withheld.** exp-10c read the same output
`read_uncommitted` and got all 50 aborted records. A consumer left on the default isolation
level — which is `read_uncommitted` in both clients — processes work that never happened.

**A stuck transaction blocks everyone.** In exp-10d the producer that wrote after the stuck
transaction used no transaction at all, and its 100 records were still invisible to
`read_committed` readers for 23.2 seconds: they sat behind the last stable offset, which
cannot pass an open transaction. The high watermark had moved, so every dashboard showed
101 records of lag, and nothing could be read — the "lag without messages" symptom, made on
purpose.

**What was surprising: the stall is longer than the timeout.** 20 s configured, 23.2 s
measured. The coordinator does not abort a transaction the instant it expires; it scans for
expired ones every `transaction.abort.timed.out.transaction.cleanup.interval.ms`, 10 s by
default — [read off the broker](../experiments/transaction_guarantee/exp-10d-hanging-transaction/results/broker-transaction-timers.log),
not recalled. So the real window is timeout plus up to ten seconds. franz-go defaults the
timeout to 40 s (Java to 60 s), which makes a hung producer's reach 40–50 s by default, and
`transaction.max.timeout.ms` lets a producer ask for up to 15 minutes. The clock starts at
the transaction's first record, not at `Begin`.

**What the instrument got right first, because it was fixed before it could bite.** A
transactional topic ends in a commit or abort marker, and a `read_committed` reader is never
handed that marker. The shared reader waited to see a partition's last offset, so on these
topics it would have waited forever and reported a short read. It now keeps control records
for its offset arithmetic and drops them from what it returns. And `RequireStableFetchOffsets`,
which the design planned to set, is a no-op in franz-go 1.22 — stable offset fetch is always
on — which the linter caught before it could sit in the code implying otherwise.

**Conclusion.** Kafka transactions are the right tool for exactly one shape: Kafka in,
Kafka out. They make that stage exactly-once under a real crash. They do nothing for a
database write, they leave aborted records readable to anyone on the default isolation
level, and one hung producer stalls every `read_committed` reader of its partitions for the
timeout plus the coordinator's scan — including readers of producers who never used a
transaction.

**Carried into:** [`static/10-transactions-eos.md`](static/10-transactions-eos.md),
[`static/06-transactional-outbox.md`](static/06-transactional-outbox.md),
[`static/05-delivery-semantics.md`](static/05-delivery-semantics.md) and the isolation row
of [`tuning-checklist.md`](tuning-checklist.md).


## exp-17 — linger, batch size and the codec, under one fixed load

Date: 2026-09-21 · `make exp-17` · run logs: [run 1](../experiments/transaction_guarantee/exp-17-batching-sweep/results/superseded/run-2026-09-21-191906.log) · [run 2](../experiments/transaction_guarantee/exp-17-batching-sweep/results/superseded/run-2026-09-21-192434.log) ·
[the Java client's own defaults](../experiments/transaction_guarantee/exp-17-batching-sweep/results/java-client-defaults.log)

**Hypothesis.** Linger trades latency for throughput: waiting fills batches, and full
batches compress better and cost fewer requests. The three franz-go defaults — linger
10 ms, a ≈1 MB batch ceiling, snappy — have already made that trade before anyone tunes
anything, and the difference from Java's defaults should be visible and priced.

**Setup.** 32 cells: linger 0, 5, 10 and 50 ms × batch ceiling 16 KiB and the default ×
codec none, snappy, lz4, zstd. Each cell is offered the same 20 000 records a second for ten
seconds into a six-partition topic with `acks=all`, RF 3. The payload is generated payment
JSON — ids, amounts, merchants, timestamps varying, field names repeating — seeded, so every
codec compresses identical bytes. Latency is from handing a record to the client to its
acknowledgement; bytes and batch sizes come from the client's own per-batch metrics, not an
estimate. The whole sweep ran twice.

**Result — robust across both runs.**

| Linger | Records per batch | Median latency | p99 (range over cells and runs) | zstd | snappy |
|---|---|---|---|---|---|
| 0 | ≈5 | 2.1–3.4 ms | 11–33 ms, unstable | 2.9–3.1× | 2.0–2.1× |
| 5 ms — Java since 4.0 | ≈22.5 | 6.6–6.9 ms | 11–19 ms | 4.7× | 2.8× |
| 10 ms — franz-go | ≈34 | 8.1–9.1 ms | 16–24 ms | 5.1× | 2.95× |
| 50 ms, 16 KiB batch | ≈40 | 11.7–12.2 ms | 23–32 ms | 5.2× | 3.0× |
| 50 ms, default batch | ≈140 | 29.8–33.1 ms | 57–65 ms | 5.6× | 3.3× |

Every cell kept up: acknowledged within a percent of offered, in both runs.

**What was surprising, three times.**

*The Java default was not what the documents said.* Every tuning guide, and this
repository until this run, gives Java's `linger.ms` as 0. Kafka 4.0 changed it to 5 ms
(KIP-1030). That was not recalled — it was printed by the 4.3.1 Java client's own config
class inside the broker image, along with `batch.size` = 16 384, which also corrected a row
of the tuning checklist that claimed both clients defaulted to ≈1 MB. The sweep grew a 5 ms
row so that "both clients' defaults are in the table" became true rather than assumed.

*Compression is a property of the batch, not the codec.* zstd went from 3.0× to 5.6× on
identical bytes, and snappy from 2.1× to 3.3×, purely by changing how long the producer
waited. A team comparing codecs at linger 0 is measuring how badly five-record batches
compress, and will pick the wrong one.

*Linger is an upper bound, not a price.* The checklist said "you pay exactly the linger in
added latency". 10 ms added about 6 ms to the median, because a batch ships as soon as it is
full or another partition's request goes out. At 50 ms the batch ceiling decided everything:
a 16 KiB batch filled in about 12 ms and shipped, so the 50 ms linger was never reached;
with the default ≈1 MB ceiling the producer waited it out and the median was 31 ms. Below
16 KiB of batch the two ceilings measured identically — the knob does nothing until batches
reach it.

**What this means for the two clients.** franz-go's 10 ms against franz-go set to Java's 5 ms — no Java client was run, and the two lingers are not the same mechanism — costs 1.5–2 ms
of median latency and about 5 ms of p99, and buys batches half as large again and 4–8% better
compression. That is a defensible default for a service; it is not the free lunch the
"Go is cheaper" comparison sometimes assumes, and it is not the 10 ms penalty the other side
assumes either.

**What the tail says, and what it does not.** Linger 0 has the lowest median and the least
stable tail — 11 to 33 ms at p99 across cells that should behave alike — because five-record
batches mean several thousand requests a second, and more, smaller requests queue unevenly.
Two single cells in the second run spiked to 74 and 125 ms at p99 where the first run had
measured 11 and 19: host noise, visible only because the sweep ran twice. No conclusion here
rests on a single cell's p99.

**What this run does not show.** At 20 000 records a second none of the 32 configurations
was short of capacity, so throughput did not differ and neither did the codecs' CPU cost:
zstd bought the fewest bytes at no measurable latency. Both only appear at saturation, which
is exp-17b, listed as outstanding.

**Carried into:** the `linger.ms`, `batch.size` and `compression.type` rows of
[`tuning-checklist.md`](tuning-checklist.md), [`static/01-write-path.md`](static/01-write-path.md)
and the defaults table in [`static/README.md`](static/README.md).


## Audit of exp-05…17 — six claims that did not survive a second reading

Date: 2026-09-22 · two independent read-throughs of every experiment in
`transaction_guarantee` against its code, franz-go's source and the run logs, then fixes,
reruns and a second read-through. The earlier entries above stay as written; this one
records what they got wrong, because a claim that was published and then withdrawn is
worth more than one quietly edited.

**Autocommit is not at-most-once by default.** The checklist, the defaults table and
case 05 said "at-most-once by default (exp-05)". franz-go's own source says the opposite —
the one-poll lag in `consumer_group.go` "is what makes default autocommit at-least-once" —
and Java behaves the same in a synchronous poll loop. Autocommit loses records only when the
handler hands them to another goroutine and polls on, or with `GreedyAutoCommit`. exp-05
never measured autocommit at all: it commits by hand, before the write. Every mention now
says so.

**exp-07 could not tell deduplication from no redelivery.** "This run was delivered the
duplicates too" rested on nothing in the log: the claim result of each inbox write was
discarded. A redelivery the inbox refuses is now written to `delivery_refused` in the same
transaction, and the verdict fails a clean table with no refusals. The rerun refused
exactly 50 ([run](../experiments/transaction_guarantee/exp-07-inbox/results/run-2026-09-21-234218.log)).

**exp-08's `acks=1` half could not lose anything.** The leader was killed after all 2 000
acknowledgements had returned, by which time local followers had everything; "lost 0,
the window did not open" was not luck but construction. Both followers are now paused
before the write, the leader killed, the followers thawed: 2 000 acknowledged, 0 readable
([run](../experiments/transaction_guarantee/exp-08-acks/results/acks-one-2026-09-22-000605.log)).
The first audit pass then asked whether that loss leaned on the stand — pausing two combined
nodes also freezes the KRaft quorum, which is why the followers stay in the in-sync set. The
pause is now timed: 1 s against a 9 s `broker.session.timeout.ms`, so a live quorum would
not have fenced them either. The counter also reads distinct keys now, since a
non-idempotent retry could otherwise hide a loss behind a duplicate.

**exp-09's duplicates were a timeout, not franz-go.** The entry above explains 291
duplicates by franz-go rewinding past batches that had already landed. That was inferred
from the source, not counted. Counting it needed franz-go's debug-level per-response summary,
since the client retries silently at every other level: on the first try an info-level
counter found "none" while the run had 80 duplicates. Counted, every duplicate is a record
the leader appended and then answered `REQUEST_TIMED_OUT` for — the frozen follower was
still in the in-sync set, so the leader waited out the 10 s produce timeout — and the
rewind resent nothing. In the final run the plain producer duplicated 80 records and had retried exactly 80 appended-then-timed-out ones ([run](../experiments/transaction_guarantee/exp-09-reordering/results/plain-2026-09-22-000803.log)); the idempotent producer retried 100 such records and read none back twice ([run](../experiments/transaction_guarantee/exp-09-reordering/results/idempotent-2026-09-22-000803.log)). Any client without idempotence duplicates
those records, so the finding is not a franz-go trait. The absence of reordering probably
is, through its one-in-flight-after-an-error gate, but that is read from the source.

**exp-10a showed an aborted output, not rolled-back offsets.** franz-go's
`GroupTransactSession` puts the offsets into the transaction only inside `End`, and the
process died before `End`, so there were no offsets to roll back — they never moved. The
wording now says so, and the verdict requires the aborted batch to be present, since a run
whose crash missed the transaction would be exact too. The rerun holds to it: `read_committed` 1 000 of 1 000 with the aborted 50 present under `read_uncommitted` ([run](../experiments/transaction_guarantee/exp-10a-rollback/results/run-2026-09-22-001224.log)).

**exp-17's `acked/s` could never show a backlog,** because it divided by the same window
as `offered/s`. It is replaced by `drain`, the time from the last record offered to the last
acknowledgement: 1.3–11.9 ms in all 32 cells, so "no cell fell behind" is now measured
(the log of that run is superseded; see the entry below).
Two readings were also corrected: the "Java 5 ms" row is franz-go set to Java's value, and
at 50 ms the 16 KiB batches did not fill — they averaged about 10 KB, because franz-go sends
every partition bound for a broker once any one is ready.

**Instruments that read a client's hooks or logs close the client first.** The
concurrency review found that `Flush` orders only the record promises; franz-go runs the
batch-written hook and the debug summary from a defer after them. exp-09 and exp-17 now
read those counts after `Close`.

**Still not measured:** rebalancing strategies (exp-14), consumer lag and backpressure
(exp-16), throughput and codec CPU at saturation (exp-17b), autocommit itself, `acks=all`
at `min.insync.replicas=1`, and exp-09 with the Java client.


## exp-05b, 06b, 14, 16, 17b — the rest of KR2

Date: 2026-09-22 · run logs under each experiment's `results/`

**Autocommit, measured rather than quoted (exp-05b, exp-06b).** The audit above corrected
the docs from franz-go's source; these two runs put numbers on it. Both kill the consumer at
the same moment — past 500 handled, right after an autocommit has landed inside the batch
being written — and differ only in the flavour. Greedy lost the unwritten rest of the batch,
38 and 41 payments; the default lost nothing and wrote 12 twice, both runs. *What the
instrument got wrong first:* the kill waited for two autocommits after the poll, on the
theory that the first might have been sent before it. It never fired, because franz-go
does not send an autocommit when nothing has changed, so a batch sees one. The kill now
reads what the client has committed and, for greedy, waits until that covers the batch.
100 ms is also the shortest autocommit interval franz-go accepts; the first attempt asked
for 20 and was refused at start-up.

**Rebalancing (exp-14): the textbook difference, and not the textbook costs.** Eager
revoked all six partitions when a second member joined — and stopped them for 45–61 ms, no
longer than an ordinary poll cycle, because with two members, a fast handler and a local
network the round is that short. Cooperative stopped only the three that moved, but for
0.51–0.68 s: franz-go notices its second round with a heartbeat 500 ms after the first
(`cooperativeFastCheck` in `consumer_group.go`). KIP-848 stopped the moved three for
5.0–6.5 s, its consumer heartbeat interval, read off the broker as 5 000 ms. None of the
strategies stopped partitions that did not move except eager, which moved everything.
Holding rebalances off while handling 2 ms records changed none of this at a load the
member kept up with. *What the instrument got wrong first:* 312 records handled twice under
cooperative. The callback registered to log revocations had replaced franz-go's default
one, which is what commits before partitions go; with the commit restored every run reports
zero. The number was published nowhere, but it would have read as a property of
cooperative rebalancing.

**Lag and backpressure (exp-16): lag is the outage, the group is the handler.** Through a
20 s dependency outage with a member joining in the middle, both handlers built the same
backlog and drained it in the same 10–13 s. What differed is everything lag does not show.
Retrying inline under `BlockRebalanceOnPoll` held the rebalance: the newcomer waited
exactly the 8 s rebalance timeout, the coordinator removed the blocked member without
telling it, and it learnt of it at 35.4 s, when the dependency answered and its commit was
refused with `UNKNOWN_MEMBER_ID`. Then the
surprise: that member handled a few more records it had fetched before the outage, rejoined,
and its next commit was *accepted* — and moved a partition's committed offset back
1 726–1 732 records, in every run. When the rewound partition went to it next, it handled
1 739 records a second time; when it stayed with the member already past it, the next commit
covered the rewind. One run in three. Pausing fetches and rewinding to the first unhandled
record showed none of it. *What the instrument got wrong first,* three times over: the
producer truncated 300 records a second to 200, because 1.5 records per 5 ms tick rounds
down — both lag experiments now refuse a rate that is not a whole number per tick; a
"partitions lost" count read zero while a member had plainly been removed, because franz-go
did not call `OnPartitionsLost` for it; and the first rewind detector compared every commit
with the highest offset ever committed, so the removed member's catch-up commits all read as
rewinds. The rewind is now judged against the last accepted commit only.

**Throughput and codec CPU (exp-17b): compression bought throughput.** Flat out, with no
codec the producer stalled at 70–79 MB/s on the wire; every codec put less on the wire and
delivered more records — zstd 2.4–2.8× as many, for 52–67% more producer CPU per megabyte.
On this stand the brokers share the laptop and write every byte three times, so bytes were
the first ceiling. Two derived numbers in the first draft of the write-up were ratios taken
across different runs; every ratio is now taken within one run and one linger.

**What still cannot be shown here:** `acks=all` at `min.insync.replicas=1` losing like
`acks=1` needs the in-sync set shrunk to one, and on three combined broker and controller
nodes that takes the KRaft quorum down with it. `min.insync.replicas=2` is measured in
exp-04 and 3 in exp-08.

**Carried into:** the autocommit, assignment, rebalance-timeout, backpressure, compression
and "not measured here" parts of [`tuning-checklist.md`](tuning-checklist.md), cases
[03](static/03-read-path.md), [05](static/05-delivery-semantics.md),
[07](static/07-retry-dlq.md) and [08](static/08-rebalance.md), and the defaults table in
[`static/README.md`](static/README.md).


## The hook that Close does not wait for — and what it moved

Date: 2026-09-22 · a concurrency review of the new experiments, then a second independent
audit of the whole group, then reruns

**The instrument was reading a counter nobody had finished writing.** exp-17 and exp-17b
take their byte columns from franz-go's batch-written hook. The entry above says those are
read after `Close`, on the reasoning that `Close` orders what `Flush` does not. It does not:
franz-go dispatches that hook in a goroutine of its own (`go func()` in `produceMetrics.hook`,
its `sink.go`) which neither `Flush` nor `Close` joins. Both sweeps now wait until the hook
has accounted for every acknowledged record, and fail the cell if it never does.

**That was not a rounding error.** Rerun with the wait, the same grid measures 4.2–8.5
records per batch at linger 0 against the ≈5 published before, and zstd 2.9–3.7× against
2.87–3.22×. The cells that lose the most are exactly the ones with the smallest, most
numerous batches — the tail of a cell is a larger share of it. Every exp-17 and exp-17b
figure in this repository now comes from a run that waits; the superseded logs were deleted
rather than kept as a second opinion, since they measure the same thing worse.

**exp-14 was committing more than it had handled.** Its revoke callback replaced franz-go's
default one and called `CommitUncommittedOffsets`, which stores everything the last poll
returned. franz-go's own callback deliberately stores the *previous* poll instead, so that a
revoke arriving while the handler is still working cannot mark unhandled records as done.
Without `BlockRebalanceOnPoll` — half this experiment's runs — that is a loss window. The
handler now marks each record it has handled and the callback commits only those, and the
run counts what nobody handled as well as what was handled twice: two runs of all six
configurations report zero of each. Its published ranges are those two runs: eager 39–75 ms,
cooperative 0.52–0.68 s on what moved, KIP-848 4.9–6.5 s — the same shape as the entry above,
measured again after the fix.

**Two deadlines that were missing.** The same revoke callback held the rebalance while
committing with no timeout of its own, and a member polling with `BlockRebalanceOnPoll`
waits for that callback on a condition variable no context can interrupt — a slow broker
would have hung the run past its deadline. And exp-14's handler slept per record without
looking at the context, so a batch of 200 could outlast a cancellation by seconds. Both are
bounded now.

**What the audit found in the writing, not the code.** The root README claimed `TBD` rows
that no longer exist and listed exp-14 and exp-16 as outstanding; case 01 cited the two
exp-17 runs that, by this repository's own admission, could not have shown a backlog;
exp-08's replication-throttle paragraph and exp-14's first instrument failure quoted numbers
whose logs were not kept, and now say so; "the three that stayed never stopped" was one
240 ms reading away from being true, and is now stated as the baseline it actually was.
Four gaps that were neither measured nor admitted — static membership and a rebalance from a
member leaving, consumer-side fetch sizing, the other backpressure levers, and
`AutoCommitMarks` as a commit strategy — are now listed in the checklist's "Not measured
here".

**Not everything the audit reported was real:** it read exp-09's test package as missing
`goleak`, which it has had since the package was written.


## exp-01 again — three runs were not enough to call a spread

Date: 2026-09-24 · `make exp-01` · four more runs on top of the three from 2026-09-20

The entry at the top of this journal calls the keyless number "a shape, not a figure" and
then quotes three runs: 7 964, 7 964 and 8 721. A fourth run, left uncommitted on
2026-09-21, read 6 827 — below everything the docs claimed. Rather than drop it, the run
was repeated three more times: 8 343, 7 206, 7 962.

| Runs | Keyless violations per 10 000 | Keyed |
|---|---|---|
| seven | **6 827 – 8 721**, median 7 964 | 0, every run |

The spread is about a fifth of the lowest reading, and two runs landing on the same number
early on made it look tighter than it is. Nothing about the finding changes — every payment
is split across partitions, most of its events are handled out of order, and the keyed
column is exactly zero every time — but the published range now covers what the instrument
actually produces. Three runs is enough to show a difference between a guarantee and a
tendency; it is not enough to bound the tendency.

**Carried into:** [`static/01-write-path.md`](static/01-write-path.md), the group README and
the root README.


## exp-14b — the other half of a rebalance

Date: 2026-09-24 · `make exp-14b` · two runs of four cells

exp-14 measures a member joining. A deploy does the opposite several times a day, so the
same program now also takes the second member away at 25 s — for good, or for two seconds —
with and without a `group.instance.id`, on cooperative-sticky throughout and a session
timeout cut to 12 s so the run outlives it.

| Membership | The member | Its partitions idle | Assignment changes |
|---|---|---|---|
| dynamic | gone for good | 0.57–0.61 s | one |
| dynamic | back after 2 s | 0.58–0.59 s | three |
| static | gone for good | **12.58–12.61 s** | one, after the session timeout |
| static | back after 2 s | **2.05 s** | none |

**Static membership is the session timeout, seen from both sides.** With an instance ID
franz-go sends no `LeaveGroup` at all when the client closes (the classic path returns early;
under KIP-848 it sends a heartbeat with member epoch −2 instead), so the group learns of a
death only when the session expires — 12.6 s of partitions nobody read. The same silence is
why a restart is free: the member came back inside the timeout, kept its assignment, and the
group never rebalanced. The only gap was the two seconds the process was down.

Dynamic membership is the mirror image: a leave is answered in 0.6 s, and a restart costs a
second rebalance, moving the partitions to the survivor and back again — the same waste
eager rebalancing spends on partitions that were never going anywhere.

Nothing was handled twice and nothing was missed in any cell, which is the check that the
commit-marking added to exp-14 still holds when the member goes away mid-stream.

**Carried into:** the `session.timeout.ms` row of [`tuning-checklist.md`](tuning-checklist.md)
and the group README.

## The payments service, part 2 — the database half of the outbox

Date: 2026-09-24 · `make test-integration` against the compose Postgres ·
[`internal/infrastructure/postgres/`](../internal/infrastructure/postgres/)

**What was expected.** That the transaction boundary, the idempotent insert and
`FOR UPDATE SKIP LOCKED` would behave the way the case files say they do — and that tests
written against them would catch it if they did not.

**What the database did.** Eight goroutines authorising the same `(merchant, idempotency
key)` at once ended with one row in `payments`, one row in `outbox`, and exactly one caller
told it had created anything; the other seven were handed the same payment id. Two relays
holding open transactions claimed two records each, disjoint, without either waiting for
the other. A second `MarkPublished` on an already-published record changed nothing rather
than pushing its timestamp an hour forward. The `CHECK` refused an amount of 0 and of −1
even though the insert came straight from the test, bypassing the domain entirely.

**The part worth keeping.** A passing test proves nothing until it has been made to fail,
so both claims were re-run against a deliberately broken adapter. With `SKIP LOCKED` taken
out of the claim, the second relay stopped stepping over the first's rows and blocked until
the test's own 15 s deadline killed it — the queue-into-one-lane failure, seen directly.
With `CreateOrGet` changed to report `created` unconditionally, both idempotency tests
failed on the line that matters: *the replay reported a stored payment; the merchant would
be charged twice*.

**Borrowed rather than invented.** The unit of work is the shape already proven in
`utils/storage`: a closure that owns the boundary, a savepoint when one is already open,
the transaction carried in the context under an unexported key so no other package can join
it, and — the detail worth stealing — commit and rollback run on `context.WithoutCancel`
with their own timeout. A caller who walks away mid-commit leaves the outcome genuinely
unknown, and the code says so (`ErrCommitUnknown`) instead of reporting a failure it cannot
prove.

**Two holes found by reading the diff as a stranger.** `Claim` refused to run outside a
transaction, `CreateOrGet` did not — so the call that promises "the payment and its event
land together" would have quietly split into two autocommits if anyone ever forgot the
wrapper. And a replay that reused a key for a *different* amount was handed the earlier
payment, which would tell a caller its 5 000.00 went through when 19.99 was authorised.
Both refuse now, the comparison of "the same authorisation" lives on the payment itself
rather than in the adapter, and with the guard switched off the test says so in words:
`err = <nil>, want payments: the idempotency key was used for a different amount`.

**Carried into:** the KR3 row of the [README](../README.md); the relay and the Kafka
publisher are the next slice.

## The payments service, part 3 — the relay, and the timestamp that is not the event's

Date: 2026-09-25 · `make test-integration` and a hand run of `cmd/outbox-relay` against the
live stand · [`internal/infrastructure/kafka/`](../internal/infrastructure/kafka/)

**Hypothesis.** The outbox rows are already correct, so publishing them is mechanical:
encode as Protobuf, register the schema, key by `payment_id`, publish with `acks=all`, mark
published after the ack.

**What the cluster did instead.** Every publish was refused: `INVALID_TIMESTAMP: The
timestamp of the message is out of acceptable range`. The producer was stamping each record
with the time the payment was authorised, which is what the outbox row carries, and the
broker will not take a record dated further ahead than
`log.message.timestamp.after.max.ms` — 3 600 000 ms on this stand, read out of the broker's
own config, while `before.max.ms` is unbounded.

**Why it matters beyond the test.** The record timestamp belongs to the log: retention and
time-based seeking use it. Business time belongs in the payload, where `occurred_at` already
was. Had the event time stayed in the record, one event dated by a skewed clock — a client
clock, a service an hour ahead — would be refused for good, and because the relay publishes
in outbox order, every record queued behind it would wait there with it. The fix is one
deleted line, and a test now asserts the two times are different things: the payload keeps
`2026-09-25T09:00:00Z`, the record is stamped within a minute of now.

**End to end, by hand.** One row inserted into `outbox`, `cmd/outbox-relay` started on a
throwaway topic: `outbox published records=1`, the row came back `published = t`, and the
console consumer showed the record in partition 2 with headers
`event_id:20102, event_type:payment.authorized`, the key equal to the payment id, and a
value beginning `00 00 00 00 01 00` — magic byte, schema id 1, message index 0, then the
protobuf. That prefix is the whole point of the registry: a consumer reads the id out of
the bytes and fetches the exact schema they were written against.

**One more thing the stand said and the banner did not.** Apicurio's `/system/info` reports
"Apicurio Registry (In Memory)" even when it is not: the log says `Using postgresql SQL
storage` and the tables are in the `apicurio` database. Worth knowing before someone
concludes their schemas are about to be lost.

**What the fresh-context review caught.** The producer put the whole registry URL into its
failure message, and that message is logged by the relay. Here it is `localhost:8080` with
nothing to hide, but a registry url is exactly where basic-auth or an `access_token=` query
lives — and the Postgres adapter two files away already refuses to print its connection
string for that reason. The rule was applied in one place and broken in the other. Errors
now name only scheme, host and path, and a unit test feeds the function a password and a
token and checks neither survives. The same review also flagged the database url as unsafe;
that one was wrong, and the code says so — `postgres.New` parses the url before connecting
and never passes the driver error out, so a failure names `host:port/database` and nothing
else.

**Carried into:** the KR3 row of the [README](../README.md). Next are the consumer side
with its inbox, exp-11 (poison pill to the dead letter topic) and exp-12 (schema evolution),
which is why the registry keeps its history in Postgres rather than in memory.

## The payments service, part 4 — the consumer, and a ghost that was stealing its rows

Date: 2026-09-25 · `make test-integration` against the live stand ·
[`internal/application/merchants/`](../internal/application/merchants/),
[`internal/infrastructure/kafka/consumer.go`](../internal/infrastructure/kafka/consumer.go)

**What was built.** The other end of the outbox: a consumer group that reads
`payments.main` with autocommit off, decodes the Confluent header and the protobuf behind
it, claims the event in an inbox keyed by the `event_id` the relay set, adds the amount to
a per-merchant total, and only then commits the offset. The claim and the total are one
transaction, so a crash between them takes both — the retry counts the payment rather than
finding an event already marked handled and skipping it forever.

**What the stand confirmed.** The same event published twice moves the total once and
leaves one inbox row. A record that was never protobuf lands in the dead letter topic with
its bytes unchanged, a `dlq_reason` and where it came from, and the good record behind it
is handled — the partition keeps moving. An event the consumer can decode but must refuse
— an amount of zero — takes the same route: `ErrUnprocessable` is the line between "try
again later" and "this will never work", and only the second one leaves the partition.

**The part that cost the afternoon.** `TestClaimHandsEachRecordToExactlyOneRelay` started
failing about one run in six — but only in a full suite run, never alone, and never twenty
times in a row on its own. The first suspicion was the obvious one: two packages sharing a
database, `go test` running them in parallel. `-p 1` did not fix it, which was the first
sign the diagnosis was wrong, and the fix was reverted rather than left in as a charm.

Making the failure explain itself is what found it. The assertion now prints what each
relay claimed, how many rows were unpublished and how many locks were held:

```
relays claimed 2 and 1 of 4; 4 rows were unpublished and 1 locks were held on outbox
```

Four rows, three claimed, one held by somebody else. That somebody was a relay from an
earlier hand run, still alive 43 minutes later: `go run ./cmd/outbox-relay` had been stopped
with a signal to the `go run` process, which does not pass it on to the program it built.
The child kept sweeping the outbox twice a second against the same database, taking rows
the test expected to find free — and rolling back, because the topic it published to had
been deleted, which is why the rows were still unpublished when the test looked.

Two things worth keeping from that. Stopping a `go run` is not stopping the service: build
the binary and run that, or the process outlives the terminal it was started from. And a
test that fails one time in six is not necessarily flaky — this one was reporting a fact
about the environment, and the fastest way to that fact was making the failure message
carry the evidence instead of just the mismatch.

**What the fresh-context reviews caught.** Two things, both of the same shape: a rule I had
already applied elsewhere and then missed here. The offset commit ran on a context that
cannot be cancelled — right, so a shutdown does not throw away work already done — but with
no timeout, which is what `storage.commit` and `deadletters.Send` both have: an unreachable
broker would have hung the poll loop until the orchestrator killed the process. And the two
identifiers that become primary keys, `event_id` from a record header and `merchant_id`
from the payload, were stored unbounded; the code's own comment says the consumer does not
trust what arrives on a topic, and then it trusted a header's length. Both are bounded at
64 characters now — what the producing domain allows a merchant id to be — in the use case
and again in the schema, and an event that breaks the bound goes to the dead letter topic
rather than blocking the partition.

**Carried into:** the KR3 row of the [README](../README.md). The dead letter path is the
groundwork exp-11 will measure; the retry topics in the catalog are still unused.

## The payments service, part 5 — the front door, and the loop closing

Date: 2026-09-25 · hand run of all three services against the live stand ·
[`internal/interfaces/http/`](../internal/interfaces/http/), [`cmd/payments-api`](../cmd/payments-api/)

**What was built.** `POST /payments` and `GET /merchants/{id}/total`, behind a required
`X-API-Key` compared in constant time. The body is bounded and decoded with unknown fields
refused — a stray `"amount": 19.99` beside `amount_minor` is a typo that must not be
ignored silently. Every domain refusal becomes a status in one table, not in each handler:
missing idempotency key 400, key reused for a different amount 409, storage unreachable
503, anything unclassified 500 with a fixed phrase and nothing from the error itself.

**The whole path, measured rather than asserted.** Three services running, one topic:

| Request | Answer |
|---|---|
| `POST /payments` with `Idempotency-Key: e2e-1` | **201 Created** |
| the same request again, as a retrying client sends it | **200 OK**, same payment id |
| the same key with `amount_minor: 500000` | **409 Conflict** |
| the same request with no `X-API-Key` | **401 Unauthorized** |
| `GET /merchants/m-e2e/total`, five seconds later | `{"authorized_minor":1999}` |

and in the database afterwards: one payment, one outbox row marked published, one inbox
row. The relay logged `outbox published records=1` in between. That is the design's claim
— state and event together, published once, counted once — running as one thing rather
than as five test suites that each believe it separately.

**Two small decisions worth naming.** The API refuses to start without a key rather than
offering an anonymous mode: a flag that disables authentication is a flag somebody sets in
production. And the amount is minor units in the wire format as well as in the domain, so
nothing in the service ever has to decide what 19.99 rounds to — the test that sends
`"amount_minor": 19.99` expects a 400, and gets one.

**Carried into:** the KR3 row of the [README](../README.md). KR3 is now complete end to
end; what remains is exp-11 and exp-12 on top of the dead letter topic and the registry,
and the observability of KR4.

## exp-11 — what one unreadable record costs, and what the first run measured instead

Date: 2026-09-25 · [run logs](../experiments/transaction_guarantee/exp-11-poison-pill/results/) ·
`make exp-11` — two runs

**Hypothesis.** A record nobody can decode blocks everything behind it unless it has
somewhere to go. With a dead letter topic the partition keeps moving; without one the
consumer crash-loops on the same offset forever.

**Setup.** 100 good records with one undecodable record after the tenth, on a single
partition — "behind it" has to be unambiguous. The consumer is the service's own; the only
difference between the cells is the sink it hands an unreadable record to: the real dead
letter topic in one, a refusing one in the other, which is what a service that never
configured the route amounts to. Separate topics, groups, merchants and event id ranges,
because the inbox deduplicates by event id and overlapping ranges would make one cell's
records look like the other's duplicates.

**Result.**

| | no dead letter route | dead letter topic |
|---|---|---|
| payments counted | **10** of 100 | **100** of 100 |
| records archived | 0 | 1 |
| committed offset | 10, with **91 records never read** | 101 |
| consumer restarts | **125 · 124** in 30 s | 0 |
| outcome | never drained | drained in **322 ms · 207 ms** |

**What the first run measured, and why it was thrown away.** It reported 11 payments
counted and the offset at 101 — everything committed, almost nothing handled. Those two
numbers cannot both be true of a stuck partition, so the run was not published. The inbox
said what had happened: rows for events 110000–110009 **and 110099** — the first ten and
the very last. The supervisor was calling `Run` again on the same franz-go client, and the
client keeps its own fetch position: the "restart" resumed after the record that had killed
it, and then a later batch committed an offset that swept everything below it. The harness,
not the service, was walking past the poison.

A restart is now a new client, which starts from the last committed offset — what a
restarted pod actually does. The numbers changed from incoherent to flat: ten handled, ten
committed, ninety-one never read.

**What it shows.** The cost of a poison pill is not the record; it is the queue behind it.
Ninety-one payments were produced, acknowledged and never read, on a partition the broker
considers perfectly healthy — every replica in sync, nothing lost. The failure is in the
consumer and it hides well: it comes back, reads, dies, and the lag graph is a flat line
rather than a spike. The route out is the whole difference, and the classification is what
makes it safe: only "this will never work" leaves the partition, while a database that is
down still holds the offset, because giving up on those is how payments go missing quietly.

**And one more instrument fix, from the review rather than from the numbers.** The backoff
between restarts was a plain `time.Sleep`, which no cancellation can interrupt: the cell
ran 30.162 s against a 30 s budget, and the overshoot was exactly that sleep. It is a
`select` on the budget now, the cell gives up at 30.003 s, and the runs above are the ones
taken after the change — the earlier pair was deleted rather than kept beside numbers
measured with a different tool.

**Carried into:** the exp-11 row of [case 07](static/07-retry-dlq.md) and the retry section
of [patterns.md](patterns.md), which until now said this was designed but not measured.

## exp-12 — the registry is off by default, and protobuf will not tell you

Date: 2026-09-25 · [run logs](../experiments/transaction_guarantee/exp-12-schema-evolution/results/) ·
`make exp-12` — two runs, Apicurio 3.0.9

**Hypothesis.** Adding a field is safe, removing one or retyping it is not, and the registry
is what stops the unsafe ones from being published.

**What the registry did.** Each change asked on a subject of its own, seeded with the
schema the service really publishes under, at three settings:

| change | NONE | BACKWARD | FULL |
|---|---|---|---|
| add an optional field | accepted | accepted | accepted |
| remove a field | accepted | **refused** | accepted |
| retype a field, keeping its number | accepted | **refused** | **refused** |

Two of those cells are not what the design notes expected. **A new subject starts at
`NONE`** — the global level reads `{"compatibilityLevel":"NONE"}` and every change goes
through, so running a schema registry buys nothing until somebody sets the level; the
server is not the protection, the setting is. And **`FULL` is not `BACKWARD` plus more**:
this version accepts a field deletion under `FULL` while refusing it under `BACKWARD`,
which is the opposite of what the names suggest. Case 09 expected the deletion to be
accepted under `BACKWARD` and recorded the refusal instead — the trap it describes is real,
but it is one setting over.

**What the bytes did.** The same three changes written as records and read by a consumer
built against the baseline:

| record | outcome |
|---|---|
| v1 | counted |
| a field added at the end | **counted**, unknown field skipped |
| the amount removed | dead letter: *an authorised amount is positive* |
| the amount retyped to a string, same number | dead letter: *an authorised amount is positive* |

**The part worth keeping.** Neither destructive change produced a decoding error. Protobuf
skipped what it could not match and handed over a message whose `amount_minor` was zero —
no error at the transport, none at the parser, none at the schema id, which was valid and
pointed at the right schema. The only layer that noticed was the domain rule that an
authorised payment is for a positive amount. Without that line the merchant's projection
would have taken two payments of zero and every component would have reported success.

**Two mistakes this experiment made first, both caught by numbers that could not be true.**
The first compatibility matrix said adding a field was incompatible — because `BACKWARD`
had been set *after* an incompatible version was already registered under that subject, so
each candidate was compared against that rather than the baseline. Fresh subject per cell
now, with the level read back before asking. Then the wire half reported a decoding error
for the added field: it had been written at number 5, which is `occurred_at` in the real
schema, so the cell measured a reused number instead of an added field. Both halves derive
from the service's own schema text now, so the numbering cannot drift apart again.

**A defect the experiment found, and the wrong fix it tempted me into.** The consume phase
ended with `context deadline exceeded` where it should simply have finished, because the
consumer reports a deadline as a failure while treating a cancellation as an orderly stop.
The quick fix was to call a deadline a shutdown too — and a fresh-context review showed why
that is wrong: the deadlines that reach that line are the consumer's *own*, from the offset
commit and from the handling of a record, so accepting them as clean exits would report a
failed commit as a normal stop. The consumer is unchanged; what changed is the harness,
which now ends the run by cancelling rather than by deadline. A deadline is a limit on
work, a cancellation is an instruction to stop, and only the second one is a shutdown.

The same review also flagged the commit's `context.WithTimeout(context.WithoutCancel(ctx),
5s)` as leaking the parent's deadline into the commit. It does not: `WithoutCancel` returns
a context with no deadline at all — `Deadline()` reports `ok=false` — so the commit gets
its five seconds whatever the caller's budget was. Checked in the standard library rather
than argued.

**Carried into:** the measured matrix in [case 09](static/09-schema-evolution.md), which
until now was a table of expectations.

## exp-13 — the front door held, and the cluster lied about its own health

Date: 2026-09-25 · [run logs](../experiments/transaction_guarantee/exp-13-broker-outage/results/) ·
`make exp-13` — two runs

**Hypothesis.** With the outbox in place, killing brokers under load should not cost a
single payment: the API writes to Postgres, the events wait there, and the relay catches up
afterwards. The backlog should grow while the cluster is unwell and fall when it is back.

**Setup.** Ten payments a second through the real API for ninety seconds, with the real
relay publishing to a topic of three partitions, RF 3, `min.insync.replicas=2`. At twenty
seconds the run kills brokers — one in cell A, two in cell B — and brings them back thirty
seconds later. The kill happens from inside the run so that it lands on the same clock as
the samples, which are taken twice a second: outbox backlog from Postgres, partition state
from the cluster's metadata.

**Result.**

| | one broker down | two brokers down |
|---|---|---|
| payments accepted during the outage | 302 · 302 | 303 · 303 |
| payments refused | **0** | **0** |
| peak backlog | 88 · 91 | 469 · 523 |
| cluster's own account of itself | 3 under-replicated | **0 under-replicated, 0 leaderless** |
| back to the pre-outage backlog | already caught up on return | 18.3 s · 24.2 s |

**What was expected and happened.** Not one payment was refused in either cell. One broker
down is a leader election rather than an outage: about a hundred records pile up while new
leaders are chosen, and the backlog drains before the broker is even back. Two brokers down
is the case the outbox is for — nothing publishes for thirty seconds, four to five hundred
records wait in Postgres, and the relay works them off in 18.3 s and 24.2 s — the two
figures in the table above — while payments keep arriving.

**What was not expected.** In cell B the cluster reported zero under-replicated partitions
and zero partitions without a leader, for the entire outage. That is not a measurement
error — it survived three separate fixes to the instrument. All three nodes here are
controllers, so killing two leaves no quorum; with no controller there is nothing to update
the metadata, and the surviving broker goes on serving the picture it had before the kill.
`kafka-topics.sh --describe` does not even manage that: it times out on
`listPartitionReassignments`, which needs the controller.

So the panel labelled *under-replicated partitions* is not a health check for a cluster
that has lost its quorum — it is a report from a cluster that can no longer tell you
anything. The number that did move, within a second, in both cells, was the outbox backlog,
read from the service's own database: the one component the outage could not reach.

**Four lies the instrument told first, all the same lie.** Absence of data rendered as
zero. It counted under-replicated partitions across the whole stand and reported 107 of
them as this run's. Scoped to the topic, it counted the lines of `kafka-topics.sh` output —
which is empty, with warnings on stderr, when brokers are missing — and called that a
healthy cluster. Reading metadata through `kadm` instead, it ranged over a response that
did not contain the topic and produced zeros a third time. And after all three were fixed,
the backlog read still came back as `-1` on failure, which the drain check compared with
`<=`: a database outage would have been reported as a drained queue. That fourth one was
found by a fresh-context review, after the author had written the rule three times and not
applied it to the fourth number on the same line — which is the whole argument for reading
your own diff with someone else's eyes. A reading nobody could take prints as `unknown`
now, and the zeros in cell B are the ones that survived all four fixes.

**Carried into:** the exp-13 row of [case 06](static/06-transactional-outbox.md), and the
runbook that comes next — where "watch the backlog, not the cluster's opinion of itself" is
the first line worth writing.

## exp-15 — the throttle works, and on this stand it bought nothing

Date: 2026-09-25 · [run logs](../experiments/kafka_internals/exp-15-partition-reassignment/results/) ·
`make exp-15` — two runs

**Hypothesis.** Moving replicas on a live topic costs the producers something, and the
throttle is how you decide whether to pay it all at once or spread it out.

**Setup.** Three partitions at RF 2 holding 60 MiB, raised to RF 3 while a producer writes
one 1 KiB record every 10 ms and times every acknowledgement. Twice: unthrottled, and at
1 MiB/s.

**Result.**

| | no throttle | 1 MiB/s |
|---|---|---|
| move | **2.2 s · 2.5 s** | **24.1 s · 24.8 s** |
| copied | 66.3 · 66.5 MiB | 69.2 · 71.1 MiB |
| producer median, before → during | 2.6 → 1.7 ms · 2.9 → 2.1 ms | 2.9 → 1.7 ms · 2.8 → 1.6 ms |
| producer p95, before → during | 6.1 → 5.3 ms · 5.8 → 8.2 ms | 6.8 → 3.4 ms · 6.7 → 3.9 ms |

**What holds.** The throttle does what it claims, and the arithmetic is per broker, not per
cluster: 22 MiB arriving at each of three brokers at 1 MiB/s is 22 seconds, and the runs
took 24. That is the number to divide by when someone asks how long a move will take.

**What does not.** The unthrottled move did not make the producers wait. Median and p95
during the copy were no worse than before it, in both cells and both runs. This stand
cannot show the thing throttles exist for: three brokers on one laptop share an SSD and a
loopback network, and 66 MiB at 30 MiB/s saturates neither. The honest statement is that
the throttle cost ten times the duration and bought nothing measurable *here* — on a
cluster where replication competes with client traffic for one NIC, the trade is real, and
this run is not evidence for it either way.

**Two things the run found by accident.** topicctl refuses to change a replication factor
at all — *"Replication in topic config (2) is not equal to observed max ISR (3); this cannot
be resolved by topicctl"* — which is why the second cell has to drop and recreate the topic
rather than re-apply its YAML. And `--verify` is not a status check: it is what removes the
throttle the `--execute` set. A run that stops before verify leaves every later replication
on that cluster crawling at a megabyte a second, with nothing in the topic config to say so.

**The run that measured nothing.** The first version preloaded 60 MiB of a repeating byte
pattern. The brokers held 2.2 MiB each — the batches compressed away — so the throttled
move had six megabytes to copy and finished in 2.4 s, which reads exactly like a throttle
that does not work. The log directories settled it: 2.2 MiB where 40 was expected. The
payload is incompressible now, and the run reports what the brokers actually hold
(121.2 MiB for 60 MiB produced at RF 2) instead of what it believed it produced.

**Carried into:** the replication-factor line in
[`deploy/topics/README.md`](../deploy/topics/README.md), which said a reassignment is a
migration and can now say what one costs.

## 2026-09-25 — an audit of the whole package, and the same mistake in four new places

**What was checked.** Every experiment against the thesis in its own README, every Kafka
claim in the prose against a file rather than against memory, every published number
against a committed log, and the dashboard against the failures this stand has actually
produced. Nothing was re-measured; this is a reading of what is already here.

**The finding that matters is a repeat.** Four of the runs corrected earlier in this
journal were wrong in one shape — absence of data rendered as zero. That shape was still
live in four more places, none of them touched since. `exp-03` and `exp-10d` threw away the
`ok` from a kadm `Lookup`, so a failed offset read became offset 0 — and offset 0 is
literally what each of them publishes as its result: a log emptied by retention, and an LSO
pinned by an open transaction. `exp-11` and `exp-16` summed listings with `Each` without
checking `Err`, so a partition that failed to answer contributed 0 — a topic nobody
produced to, and zero lag in the one experiment whose subject is lag. `exp-15` summed log
dirs the same way, and answered 0 bytes copied if no broker answered at all.

All five now go through one guard. `labkit.OffsetAt` refuses each of the three ways a
listing declines — partition absent, load error, the `-1` kadm fills in for an unknown
topic — and `labkit.spansFrom` refuses a topic missing from the response entirely, because
an empty partition map makes a reader that is finished before it polls once. The guard has
tests that go red when it is removed, including the control that matters: a log the broker
*positively* reported as empty is still a valid span, since that is what exp-03 measures.

**The second finding is the other half of the same habit.** Five statements about franz-go
were written from memory and are wrong. The one that had spread furthest: `ConsumeResetOffset`
resolving a committed offset that fell below the log start. The docs said it rewinds a
minute; franz-go's own doc says it resumes at the log start, and the one-minute rewind is
for mid-stream data loss. The minute was quoted in the cheat sheet's flagship defaults
table. Separately, `max.poll.records` was called non-existent in four places; it is
`PollRecords(ctx, n)`, an argument rather than a setting. This is the third time a franz-go
claim written from memory has had to be withdrawn, after `AtStart` and the forwarded-record
timestamp. The rule from here: no sentence about the client ships without the
`config.go`/`consumer.go` line beside it.

**And one figure contradicted its own run.** Fig. 9a drew a field deletion being accepted
under `BACKWARD` and cited exp-12c as support — while exp-12c is the run that measured it
*refused*, 409. The figure now runs under `FULL`, which is where the change actually gets
through on Apicurio 3.0.9, which makes the story stronger: the deploy passes the registry's
strictest check and the old consumer still gets a zero.

**Withdrawn here.** exp-02's drain table was printing three rows from a superseded run
beside two from the current one, and the "15% faster with seven consumers" built on it is
noise — the seventh member handles nothing and its cell is still faster than the sixth. The
honest result is stronger: adding members bought nothing measurable, because one member
serves the hot partition. exp-15's table printed two values per cell from one committed run;
the unbacked halves are gone and a repeat is owed. The end-to-end HTTP walk in the README
was labelled "measured" and is a hand run — it now says so.

**Still open.** exp-04 never reads its topic back, so "a pause, not data" is not something
its code can settle, and it writes only after the controller has finished re-electing;
exp-14's producer passes a nil promise, so a partition it failed to write to looks like a
partition the consumer stalled on; `stand` and `eos` compute an honest verdict and return
nil regardless, so a failed run can exit 0. The dashboard has never been in front of four
of the five failures, and its under-replicated panel is the repo's own documented lie
rendered in Grafana.

## The payments service, part 6 — the retry chain, and three things a review found in it

Date: 2026-09-25 · [exp-18](../experiments/transaction_guarantee/exp-18-retry-chain/) ·
`make exp-18`, `make test-integration`

**What was missing.** KR3 asks for retries with backoff, a retry topic and a dead letter
queue. The dead letter topic existed; the three retry topics were in the catalog and nothing
read them, and the README said so. The obvious way to close the gap — send every transient
error into the chain — would have been wrong for this service: its only dependency is
Postgres, and "Postgres is down" sent through a 5 s / 1 min / 10 min chain is the whole topic
in the dead letter topic eleven minutes into an outage. So the chain carries exactly one
failure, the only one that belongs to a record rather than to the service: a merchant row
another transaction holds. `TotalsStore.Add` bounds its lock wait at 2 s, SQLSTATE `55P03`
becomes `merchants.ErrContended` in the repository, and only that goes into the tiers.
Everything else holds the offset, as before.

**Before any of it: the integration suite did not compile.** `NewConsumer` had gained a
metrics argument in `714f593` and `consumer_integration_test.go` still called it with six,
so `make test-integration` had been failing to build the kafka package since — and nothing
said so, because `make verify` does not build the `integration` tag. Fixed with the rest.

**Result.** exp-18, one merchant locked for 30 s under 600 payments, six of them its own:

| | no chain | chain |
|---|---|---|
| healthy payments counted while the row was locked | 268–305 of 594 | 594 of 594 |
| the last healthy payment | +30.5 s, just after the lock | +17.0–17.1 s |
| consumer restarts | 13 in every run | 0 |
| inbox rows = the merchants' totals | 600 = 600 | 600 = 600 |

With the lock held past every tier, all six ended as `exhausted` dead letters, were replayed
once the row was free, and were counted 3.0–3.5 s later — once.

**The lock wait is not one `lock_timeout`.** Six contended payments should have cost the
partition 12 s; the gaps between healthy payments added up to 16. Two of them were 4 s, and
both were payments whose main-stage attempt queued behind a tier's attempt already waiting
on the same row: Postgres applies `lock_timeout` per lock acquisition — the tuple lock
first, then the row — not per statement. The bound holds, but "2 s per contended payment"
turned out to be the floor.

**Three fresh-context reviews, and what they found in code I had already called finished.**
The concurrency review found that a tier paused its whole topic and slept until its head
record was due. My reasoning had been that every record arriving later is due later, so
pausing loses nothing — true for new arrivals, false for a backlog: after an outage a poll
returns part of a long-due backlog, and one fresh record fetched from another partition put
the rest of it to sleep for a full tier delay. Reproduced before it was fixed: 600 due
records on one partition, one fresh on another, 11 s with the topic paused. Now only the
waiting partition is paused, each with its own resume time, and the poll is bounded by the
earliest of them. Fixing that surfaced the next one, found by a run rather than a reviewer:
a resumed partition joined only the *next* fetch, and franz-go's default fetch wait is 5 s,
so exp-18 counted replayed payments 5–8 s after the replay into a 3 s tier. Tiers now fetch
with a 500 ms wait; the delay test measures 40–70 ms of lateness and fails at the default.
The security review found that the attempt count and the origin of a retried record were
read from headers a producer controls, and that dead-letter headers were appended beside
any a producer had already set, so the forged one was read first. The attempt count now
comes from the stage's position in the chain, and every header is written, not appended.
And the lock wait itself sits inside a batch that holds the rebalance, so a partition full
of one locked merchant's payments could have held it past its 60 s timeout — exp-16's
failure — until a 20 s batch budget went in.

**Left open, on purpose.** The inbox deduplicates by an `event_id` header, and a producer
that can write to the topic can pre-empt a real payment's id. On a stand with no broker
authentication, such a producer could as easily publish a payment outright; the fix is
producer ACLs on every topic of the chain, and it is written down in case 07 rather than
patched in the consumer.

## 2026-09-26 — the audit's reruns, and what they took back

**What was done.** An audit of every run against its own hypothesis found cells without
controls, claims resting on one pass, and numbers quoted from logs that said something
else. Every flagged experiment was rerun or given the cell it lacked. Most results held;
what follows is what did not, and what the instruments got wrong on the way.

**Withdrawn or corrected.**
- *exp-02.* "Seven members bought 15%" was one pass; the 2026-09-25 entry above then called
  it noise. Both were half right. Three runs with a control cell show the skewed topic has
  a ceiling of 1.17× — all records over the hottest partition — and six members reached
  1.12–1.16×, while the same six drained evenly keyed events 5.2–5.8× faster than one. The
  gain is real and the key caps it.
- *exp-14.* "Eager stopped every partition for 39–75 ms" quoted gaps that sit inside the
  baseline poll cycle (29–73 ms). Over four runs the revocation is in the log and its cost
  is not measurable here.
- *exp-17/17b.* "Batching matters more than the codec" and "bytes are the ceiling" went
  beyond the runs: batching doubles every codec's ratio, zstd still compresses ≈1.7× better
  than snappy at equal batching, and the byte ceiling is inferred, not isolated.
- *exp-09.* The reordering it was built to show never happened on franz-go in five runs;
  the duplicates were exactly the records appended before a `REQUEST_TIMED_OUT`.
- *exp-04.* "Leadership never comes back" is this stand's `auto.leader.rebalance.enable=false`,
  not Kafka's; the default moves it back on a 300 s check. And one kill in three lands on
  the KRaft controller: that run took 15.3 s, not 10, because the new controller starts the
  dead broker's session over. The run now prints the controller before every kill.

**The instruments, again.** Four of the reruns first measured the instrument:
- exp-14's producer used the run's context, so franz-go failed the last tick's buffered
  records at shutdown and the run reported a gap in the stream where there was none.
- exp-08's new control cell hung for twelve minutes past every deadline: an idempotent
  franz-go producer cannot give up on a batch the leader answered `REQUEST_TIMED_OUT`,
  since the leader may have appended it. Its `count` then reported "2 000 of 2 000 lost"
  for a producer that had been acknowledged nothing — loss was measured against what was
  sent, not against what was promised.
- exp-18's reordering metric compared payments across partitions and found 954 overtakes
  in the cell without a chain, where Kafka had never ordered them. Within a partition it is
  0 without the chain and 280–316 with it — the price of the time the chain saves.
- And the laptop slept. A chain of reruns left running with the lid closed produced an
  exp-13 whose 90 s cells took 18 minutes of wall clock, and an exp-08 frozen mid-cell with
  a broker killed. Both exited 0 or would have. Every rerun since checks `pmset` for a sleep
  inside its window.

**The finding that matters is ours.** exp-13 now dumps the dashboard's panels from
Prometheus for each cell's window, and in the two-broker outage every panel an operator
would look at said *healthy*: under-replicated 0 and `kafka_brokers` 3 — the frozen
metadata, as documented — and the outbox backlog **1**, flat, while Postgres held 308. The
relay sets that gauge when a sweep ends, and during a full outage no sweep ends. The
backlog was the one number this repo told operators to trust because the outage cannot
reach it; the database cannot be reached, but the gauge could. The relay now counts it in
a goroutine of its own beside the sweeps — a test holds a sweep on a publisher that never
answers and requires the backlog to keep climbing, and fails on the old code — and the
exp-13 rerun shows the panel going 5, 10, 60 … 460 through the same outage.

**Checked and held.** exp-03, 10d, 11, 12, 15 and 16 reproduced on rerun; exp-08's
`acks=1` loss reproduced exactly, and its new control cell answered the hypothesis the
first two cells could not: under the identical failure `acks=all` acknowledged none of the
2 000 records and so lost none of what it had promised.
