# exp-14b — a member leaving, and what `group.instance.id` changes about it

**Hypothesis.** [exp-14](../exp-14-rebalance-strategies/) measures a member joining. Leaving
is the other half, and the one a deploy does several times a day. Static membership
(`group.instance.id`) is supposed to make a restart invisible to the group — and to make a
real death take the session timeout to notice.

```
make exp-14b
```

exp-14's program, four cells: the second consumer leaves at 25 s and either stays gone or
comes back two seconds later, each with dynamic membership and with a `group.instance.id`.
Cooperative-sticky throughout, so the only thing that differs is membership, and the session
timeout is set to 12 s rather than the 45 s default so the run can outlast it. Everything
else is exp-14's: 1 200 records/s over six partitions, and the longest stretch each
partition went unhandled, measured from 2 s before the departure to 15 s after it.

| Knob | Default | Why |
|---|---|---|
| `-leave-after` | 25 s | 15 s after the join, so the baseline is the group at rest with two members |
| `-rejoin-after` | 2 s | a pod restart, shorter than the session timeout |
| `-session-timeout` | 12 s | the run has to outlive it; Kafka's default is 45 s |

## Result — 2026-09-24

Two runs of each: dynamic [gone](results/dynamic-gone-2026-09-24-170703.log) ·
[restart](results/dynamic-restart-2026-09-24-170703.log), static
[gone](results/static-gone-2026-09-24-170703.log) ·
[restart](results/static-restart-2026-09-24-170703.log); the second pair is the `-171058`
files beside them.

| Membership | The member | Partitions it held | Partitions the other member kept | Assignment changes |
|---|---|---|---|---|
| dynamic | gone for good | **0.57–0.61 s** | 41–62 ms | one: the survivor takes them 0.55–0.58 s after the leave |
| dynamic | back after 2 s | **0.58–0.59 s** | 36–40 ms | three: they move to the survivor, then back again at 29.6 s |
| static | gone for good | **12.58–12.61 s** | 36–50 ms | one, and only after the session timeout expires at 37.6 s |
| static | back after 2 s | **2.05 s** | 40–51 ms | none — the group never learnt it had gone |

Nothing was handled twice and nothing was missed in any cell.

**Dynamic membership answers a leave immediately, and a restart twice.** Closing the client
sends `LeaveGroup`, so the group rebalances at once: the survivor had the partitions back
within 0.6 s. When the member comes back, it costs a second rebalance — the partitions move
to the survivor and then straight back, which is the same pure waste as eager's
revoke-and-regrant, only spread over four seconds.

**Static membership trades that for a session timeout.** With a `group.instance.id`,
franz-go sends no `LeaveGroup` at all on close (`consumer_group.go` returns early when the
instance ID is set), so a member that really died is noticed only when its session expires:
its partitions sat idle 12.6 s, the timeout plus detection. The same property is the point
of the feature: a member that comes back inside the timeout keeps its assignment, the group
never rebalances, and the only gap is the restart itself — 2.05 s, the two seconds the
process was down.

**Which is the right trade depends on which event is common.** A service that deploys
several times a day pays two rebalances per pod without static membership and none with it;
what it risks is that a pod which dies for good leaves its partitions unread for the session
timeout. That is the number to size: long enough to cover a restart, short enough to be an
acceptable stall.

**Not shown:** the same comparison under KIP-848, where the leave path differs — franz-go
sends a leave heartbeat for static members there rather than returning early — and a
rolling deploy of more than one member, which is where the rebalance count multiplies.

## Cleaning up

```
make reset-topic TOPIC=exp14.payments
```
