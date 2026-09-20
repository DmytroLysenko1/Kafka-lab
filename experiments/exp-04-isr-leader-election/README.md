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

| Phase | Leaders | Smallest ISR | Writes accepted |
|---|---|---|---|
| baseline | `[3 1 2]` | 3 of 3 | 300 of 300 |
| degraded, kafka3 killed | `[1 1 2]` | **2**, and `min.insync.replicas` is 2 | **300 of 300** |
| recovered, kafka3 back | `[1 1 2]` | 3 of 3 | — |
| after preferred election | `[3 1 2]` | 3 of 3 | — |

| What | How long |
|---|---|
| kill → ISR shrinks and partition 0 has a new leader | **10.9 s** |
| restart → ISR whole again | **5.3 s** |
| preferred election → leadership back on the preferred replica | **0.2 s** |

**The reaction time is a session timeout, not a lag timer.** The number usually quoted for
"a follower falls out of the ISR" is `replica.lag.time.max.ms`, 30 s by default. This took
10.9 s, because a crashed broker is not a slow follower: the controller stops receiving its
heartbeats and fences it after `broker.session.timeout.ms` — 9 s, sent every 2 s — and
fencing rewrites the ISR of every partition that broker was in. Both defaults were read
back off the running broker, not from memory.

**One dead broker degraded every partition.** With RF 3 on three brokers each broker holds
a replica of every partition, so `under-replicated 3 of 3`. That is the shape of a small
cluster, not a bug: the ratio only improves when there are more brokers than replicas.

**Writes never stopped, and there was no margin left.** The smallest ISR was 2 and
`min.insync.replicas` is 2 — exactly the threshold. The next failure is the one that turns
`acks=all` into `NOT_ENOUGH_REPLICAS`; that is exp-08.

**Leadership did not come back on its own.** The replica rejoined in 5.3 s, but partition 0
was still led by its replacement, and stayed that way. `auto.leader.rebalance.enable` is off
on this stand, so the preferred election is a step someone has to run — it took 0.2 s and
restored `[3 1 2]`. Left undone, every restart leaves the cluster a little more lopsided.

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
