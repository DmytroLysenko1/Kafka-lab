# exp-08 — what `acks` promises, and what it only implies

**Hypothesis.** `acks=1` acknowledges a record the moment the leader has it, so killing
the leader afterwards loses a record the caller was told was safe. `acks=all` on a topic
whose `min.insync.replicas` is above the number of live replicas refuses the write
instead — failing loudly rather than lying quietly.

```
make exp-08
```

Two single-partition topics, RF 3, and two logs. Each half kills a broker and puts it back
on every exit path, leadership included.

| Knob | Default | Why |
|---|---|---|
| `RECORDS` | 2 000 | enough that a partial loss would be visible as a number |
| `RECORD_BYTES` | 4 096 | uncompressed, so the log on disk is the size it looks |

## Result — 2026-09-21

### `acks=all` with `min.insync.replicas=3` — [run log](results/acks-all-2026-09-21-183122.log)

| Cluster | Accepted |
|---|---|
| every broker up | **2 000 of 2 000** |
| one broker down | **0 of 2 000 — `NOT_ENOUGH_REPLICAS`** |

Not one record was written and not one was lost, because not one was accepted. That is the
value of the setting: `acks=all` means "everyone currently in the in-sync set", and an
in-sync set of one is one machine — so the guarantee that reads as the strongest available
is exactly as strong as the number you did not set.

### `acks=1` — [run log](results/acks-one-2026-09-21-183122.log)

| | |
|---|---|
| acknowledged | 2 000 of 2 000 |
| readable after the leader was killed | **2 000** |
| lost | **0** |

The window is real and it did not open: the followers had fetched all 8 MB before the kill
landed. This is the honest shape of `acks=1` — it does not lose records often, it loses
them rarely, and a failure that fires rarely and reports nothing is worse to operate than
one that fires predictably.

**Holding the followers back with a replication throttle does not work**, which is worth
knowing before planning any test around it. The configs applied and read back from the
cluster; an 8.3 MB log still reached all three replicas instantly. Replication quotas exist
for reassignment traffic, where the destination replica is not in the in-sync set — they do
not restrain replicas that already are. Demonstrating the loss needs a follower paused
rather than throttled, which is exp-08c in
[case 04](../../../docs/static/04-isr-leader-election.md#still-to-be-measured).

The reasoning, and the false zero this instrument printed before it was fixed, are in the
[journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp08.acks1
make reset-topic TOPIC=exp08.isr3
make elect-preferred
```
