# exp-04 — ISR, leader election and recovery

**Hypothesis.** Killing the broker that leads a partition costs a pause, not data: a
replica from the ISR takes over, `acks=all` keeps being honoured because two replicas are
still in sync, and once the broker is back everything returns to how it was.

**What this run can and cannot settle.** It measures how long the cluster takes to notice
and where leadership lands, and it shows `acks=all` still being honoured afterwards. It
does **not** settle the "not data" half: the run never reads the topic back, so a leader
that kept every acknowledged record and one that truncated some would print the same table.
Nor does it price the pause — the degraded phase waits for the controller to finish
re-electing before it writes, so its 300 of 300 describes a settled two-of-three cluster,
not the moment of the kill. Both need a phase this experiment does not yet have: a
continuous writer across the kill, and a `labkit.ReadAll` of the topic at the end.

```
make exp-04
```

One topic, 3 partitions, RF 3, `min.insync.replicas=2`,
`unclean.leader.election.enable=false`. Four phases, each its own process so the shell can
kill a broker between them and time the cluster's reaction from the failure itself:

| Phase | What it does |
|---|---|
| `baseline` | writes with `acks=all`, records who leads what, names the leader of partition 0 |
| `degraded` | waits for the cluster to notice the kill, then writes again |
| `recovered` | waits for the ISR to be whole again after the restart |
| `elected` | what an explicit preferred election changes |

The script uses `docker kill`, not `stop`: SIGTERM gives Kafka a controlled shutdown, which
hands leadership over politely and measures the good case. A crash is the case worth
measuring.

| Knob | Default | Why |
|---|---|---|
| `RECORDS` | 300 | enough to prove writes are served, small enough that the phase is about availability, not throughput |

## Result — 2026-09-20

[run log](results/run-2026-09-20-200722.log) · broker IDs are whatever the assignment
produced on this run — the previous run killed kafka3, this one kafka2. The result is the
shape, `[a b c]` → `[b b c]` → `[a b c]`.

| Phase | Leaders | Smallest ISR | Writes accepted |
|---|---|---|---|
| baseline | `[2 3 1]` | 3 of 3 | 300 of 300 |
| degraded, kafka2 killed | `[3 3 1]` | **2**, with `min.insync.replicas` 2 | **300 of 300** |
| recovered, kafka2 back | `[3 3 1]` | 3 of 3 | — |
| after preferred election | `[2 3 1]` | 3 of 3 | — |

| What | Within | Of that, spent polling |
|---|---|---|
| kill → ISR shrinks and no partition is leaderless | **10.6 s** | 10.6 s |
| restart → ISR whole again | **5.1 s** | 5.1 s |
| preferred election → leadership back | **0 s** | 0 s |

Every interval is an upper bound measured from the event the shell timed, so the second
column matters: it is how long the measuring process actually spent watching. On the first
two rows it is the whole interval, so the cluster changed while we were looking. On the
third it is zero — leadership was already back at the first poll, which is all this
instrument can say about a preferred election: it finishes faster than a process can start
and ask. Fast enough not to measure is still the answer to "should I wait for it".

**The reaction time is set by the session timeout, not by the lag timer — and exp-04c
measures that rather than arguing it.** `replica.lag.time.max.ms` at 30 s is the figure
usually quoted for a replica leaving the ISR, and ten-odd seconds cannot be it. A crashed
broker is not a slow follower: the controller stops receiving heartbeats and fences it after
`broker.session.timeout.ms` — 9 s, heartbeats every 2 s, both [read off the running
broker](results/broker-timers.log) — and fencing rewrites the ISR of every partition it was
in.

```
make exp-04c
```

recreates the brokers with `broker.session.timeout.ms=20000`, changes nothing else, kills
the same broker, and puts the default back on every exit path:

| `broker.session.timeout.ms` | `replica.lag.time.max.ms` | kill → ISR shrinks |
|---|---|---|
| 9 000 | 30 000 | 10.1 s · 10.6 s |
| **20 000** | 30 000 | **20.3 s** |

[log](results/exp-04c-2026-09-21-172333.log), which prints the timers the broker is
enforcing above the measurement. ISR recovery stayed at 5.1 s in both — that is the
returning replica catching up, not fencing, and it is the control: had it moved too, the
change would have been something broader than the timer under test.

Each figure lands a little above its timeout — heartbeat interval, controller work, and this
experiment's 250 ms polling. A session expires some time after the last heartbeat that would
have renewed it, not the instant the process dies.

**One dead broker degraded every partition.** With RF 3 on three brokers each broker holds
a replica of every partition, so `under-replicated 3 of 3`. That is the shape of a small
cluster, not a bug: the ratio only improves when there are more brokers than replicas.

**Writes never stopped, and there was no margin left.** The smallest ISR was 2 and the
broker reports `min.insync.replicas` 2 — exactly the threshold. The next failure is the one
that turns `acks=all` into `NOT_ENOUGH_REPLICAS`; that is exp-08.

**Leadership did not come back on its own.** The replica rejoined in 5.1 s, but partition 0
was still led by its replacement, and stayed that way until asked. `auto.leader.rebalance.enable`
is off on this stand, so the preferred election is a step someone has to run. Left undone,
every restart leaves the cluster a little more lopsided.

**What is not demonstrated here.** Unclean leader election needs the ISR to collapse to a
replica that is behind, which on three combined broker/controller nodes means killing two of
them — and that destroys the KRaft quorum, so the controller cannot rewrite an ISR at all.
The same constraint is written up in
[04](../../../docs/static/04-isr-leader-election.md#the-three-ways-an-acknowledged-record-dies).

The reasoning is in the [journal](../../../docs/00-journal.md).

## exp-04c — varying the timer

`make exp-04c` recreates the brokers with a raised `broker.session.timeout.ms`, runs the
same kill, and restores the default. The named volumes survive a recreate, so the topic and
its data are the ones exp-04 measured on.

```
SESSION_TIMEOUT_MS=30000 make exp-04c    # any value; 20000 is the default for this run
```

It leaves the stand as it found it: default timeout, every broker running, leadership back
on the preferred replicas.

## Cleaning up

```
make reset-topic TOPIC=exp04.isr
make elect-preferred
```

The run arms a `trap` that restarts the killed broker on every exit path, so an aborted run
should not leave the stand short one broker. If one ever does — check `docker ps` — start it
by hand, and note that `make` execs into `kafka-lab-kafka1` by default, so with that broker
down every target needs `KAFKA_CONTAINER=kafka-lab-kafka2`.
