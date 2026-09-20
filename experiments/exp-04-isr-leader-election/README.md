# exp-04 — ISR, leader election and recovery

**Hypothesis.** Killing the broker that leads a partition costs a pause, not data: a
replica from the ISR takes over, `acks=all` keeps being honoured because two replicas are
still in sync, and once the broker is back everything returns to how it was.

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

[run log](results/run-2026-09-20-154106.log) · broker IDs are whatever the assignment
produced on this run; the result is the shape, `[a b c]` → `[b b c]` → `[a b c]`.

| Phase | Leaders | Smallest ISR | Writes accepted |
|---|---|---|---|
| baseline | `[3 1 2]` | 3 of 3 | 300 of 300 |
| degraded, kafka3 killed | `[1 1 2]` | **2**, with `min.insync.replicas` 2 | **300 of 300** |
| recovered, kafka3 back | `[1 1 2]` | 3 of 3 | — |
| after preferred election | `[3 1 2]` | 3 of 3 | — |

| What | Within | Of that, spent polling |
|---|---|---|
| kill → ISR shrinks, partition 0 has a new leader | **10.1 s** | 10.1 s |
| restart → ISR whole again | **5.3 s** | 5.3 s |
| preferred election → leadership back | **0 s** | 0 s |

Every interval is an upper bound measured from the event the shell timed, so the second
column matters: it is how long the measuring process actually spent watching. On the first
two rows it is the whole interval, so the cluster changed while we were looking. On the
third it is zero — leadership was already back at the first poll, which is all this
instrument can say about a preferred election: it finishes faster than a process can start
and ask. Fast enough not to measure is still the answer to "should I wait for it".

**The reaction time is consistent with a session timeout, not with the lag timer.** This is
an inference, not a measurement, and the difference is worth stating plainly: nothing in the
run varies either setting. What was measured is one interval of 10.1 s. What is argued is
that `broker.session.timeout.ms` governs it — 9 s, heartbeats every 2 s — rather than
`replica.lag.time.max.ms`, the 30 s figure usually quoted for a replica leaving the ISR.
10.1 s fits the first and cannot fit the second, and a crashed broker is not a slow
follower: the controller stops receiving heartbeats and fences it, which rewrites the ISR of
every partition it was in. The three defaults are [read off the running
broker](results/broker-timers.log), not recalled. Turning the argument into a measurement
takes one more run with `broker.session.timeout.ms` raised, showing the reaction move with
it; until that exists this stays an inference.

**One dead broker degraded every partition.** With RF 3 on three brokers each broker holds
a replica of every partition, so `under-replicated 3 of 3`. That is the shape of a small
cluster, not a bug: the ratio only improves when there are more brokers than replicas.

**Writes never stopped, and there was no margin left.** The smallest ISR was 2 and the
broker reports `min.insync.replicas` 2 — exactly the threshold. The next failure is the one
that turns `acks=all` into `NOT_ENOUGH_REPLICAS`; that is exp-08.

**Leadership did not come back on its own.** The replica rejoined in 5.3 s, but partition 0
was still led by its replacement, and stayed that way until asked. `auto.leader.rebalance.enable`
is off on this stand, so the preferred election is a step someone has to run. Left undone,
every restart leaves the cluster a little more lopsided.

**What is not demonstrated here.** Unclean leader election needs the ISR to collapse to a
replica that is behind, which on three combined broker/controller nodes means killing two of
them — and that destroys the KRaft quorum, so the controller cannot rewrite an ISR at all.
The same constraint is written up in
[04](../../docs/static/04-isr-leader-election.md#the-three-ways-an-acknowledged-record-dies).

The reasoning is in the [journal](../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp04.isr
make elect-preferred
```

The run arms a `trap` that restarts the killed broker on every exit path, so an aborted run
should not leave the stand short one broker. If one ever does — check `docker ps` — start it
by hand, and note that `make` execs into `kafka-lab-kafka1` by default, so with that broker
down every target needs `KAFKA_CONTAINER=kafka-lab-kafka2`.
