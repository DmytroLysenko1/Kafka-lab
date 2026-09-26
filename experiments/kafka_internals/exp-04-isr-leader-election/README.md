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

## Result — three runs

Runs: [2026-09-20 15:41](results/run-2026-09-20-154106.log),
[2026-09-20 20:07](results/run-2026-09-20-200722.log) (killed kafka2) and
[2026-09-26](results/run-2026-09-26-034607.log) (killed kafka3, which was also the KRaft
controller). Broker IDs are whatever the assignment produced; the result is the shape,
`[a b c]` → `[b b c]` → `[a b c]`. The
phase table is the 2026-09-26 run, the only one that also reads the topic back:

| Phase | Leaders | Smallest ISR | Writes accepted |
|---|---|---|---|
| baseline | `[3 1 2]` | 3 of 3 | 300 of 300 |
| degraded, kafka3 killed | `[1 1 2]` | **2**, with `min.insync.replicas` 2 | **300 of 300** |
| recovered, kafka3 back | `[1 1 2]` | 3 of 3 | — |
| after preferred election | `[3 1 2]` | 3 of 3 | — |
| readback | — | — | **600 of 600 acknowledged records readable, 0 lost** |

| What | Within | Of that, spent polling |
|---|---|---|
| kill → ISR shrinks and no partition is leaderless | **10.1 s · 10.6 s · 15.3 s** (the last with the controller killed) | the whole interval |
| restart → ISR whole again | **5.1 s** in every run | the whole interval |
| preferred election → leadership back | **0–0.1 s** | 0 s |

**The degraded window was a pause, not data.** The readback reads the topic to its end after
recovery and finds every one of the 600 acknowledged writes. It says that for writes made
before the kill and after the re-election; the kill itself happens between the two write
phases, so nothing was in flight when it landed — that case is exp-08.

**Killing the controller costs about five seconds more.** In the 15.3 s run the killed
broker was also the active KRaft controller. The [surviving brokers' logs](results/controller-failover-2026-09-26-034607.log)
show them lose it and elect kafka2 about three seconds after the kill; a new controller has
no record of when the dead broker last heartbeated, so its 9 s session starts over. That is
roughly 12 of the 15.3 s; the rest is not isolated by this run. The two earlier runs did not
record which broker was the controller, so they cannot serve as a clean control; the run now
prints it before every kill. On three combined nodes one kill in three lands on the
controller.

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
| 9 000 | 30 000 | 10.1 s · 10.6 s · 15.3 s (the last with the controller killed) |
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

**Leadership did not come back on its own — on this stand.** The replica rejoined in 5.1 s,
but partition 0 was still led by its replacement, and stayed that way until asked.
`auto.leader.rebalance.enable` is off here; the broker default is on, and then the
controller moves leadership back itself on a check every 300 s
([config](results/leader-rebalance-config.log)) — not measured. With it off, the preferred
election is a step someone has to run, and every restart left undone leaves the cluster a
little more lopsided.

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
