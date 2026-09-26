# exp-08 — what `acks` promises, and what it only implies

**Hypothesis.** `acks=1` acknowledges a record the moment the leader has it, so killing
the leader afterwards loses a record the caller was told was safe. `acks=all` on a topic
whose `min.insync.replicas` is above the number of live replicas refuses the write
instead — failing loudly rather than lying quietly.

```
make exp-08
```

Three single-partition topics, RF 3, three cells and a log for each. The `acks=1` cell
pauses both followers, writes, kills the leader and thaws the followers; the
`acks=all`/`min.insync.replicas=3` cell kills one broker. The third cell is the control the
first two lacked: the `acks=1` cell's failure step for step — followers paused, write,
leader killed, followers thawed — with only `acks` changed to `all`, on a topic at
`min.insync.replicas=2`. Without it the first two cells compare two different failures.
Every paused or killed broker is put back on every exit path, leadership included.

| Knob | Default | Why |
|---|---|---|
| `RECORDS` | 2 000 | enough that a partial loss would be visible as a number |
| `RECORD_BYTES` | 4 096 | uncompressed, so the log on disk is the size it looks |

## Result — 2026-09-21, rerun 2026-09-26

### `acks=all` with `min.insync.replicas=3` — [run log](results/acks-all-2026-09-21-183122.log) · [rerun](results/acks-all-2026-09-22-000605.log) · [rerun](results/acks-all-2026-09-26-161734.log)

| Cluster | Accepted |
|---|---|
| every broker up | **2 000 of 2 000** |
| one broker down | **0 of 2 000 — `NOT_ENOUGH_REPLICAS`** |

Not one record was written and not one was lost, because not one was accepted. What this
half shows is the price of the setting, not its protection: with `min.insync.replicas`
equal to the replication factor, one dead broker stops every write. The protection is the
contrast with the half below, and the two halves are not the same failure. `acks=all` under
the same kind of pause is measured in [exp-09](../exp-09-reordering/): with a frozen
follower still in the in-sync set, the leader appended the records and answered
`REQUEST_TIMED_OUT` instead of acknowledging them — it never told the producer they were
safe.

### `acks=1`, followers paused — [run log](results/acks-one-2026-09-22-000605.log) · [rerun](results/acks-one-2026-09-26-161734.log)

| | |
|---|---|
| acknowledged by the leader alone | 2 000 of 2 000 |
| readable after the leader was killed and a follower elected | **0** |
| lost | **2 000 of 2 000** |

Both followers were paused before the write, so the leader acknowledged 8 MB that existed
nowhere else. It was killed, the followers were thawed after 1 s, and the controller fenced
the dead leader after 10.9 s and elected a follower — cleanly, because a paused follower is
still in the in-sync set. The new leader had none of the records, and every one the
producer had been told was written is gone, with no error anywhere. The rerun reproduced it
exactly: 2 000 acknowledged, 0 readable, fenced after 10.3 s.

### `acks=all`, the same failure — [run log](results/acks-all-paused-2026-09-26-161734.log)

| | |
|---|---|
| acknowledged, with both followers paused | **0 of 2 000** — the producer gave up after 3 s: *records have timed out before they were able to be produced* |
| readable after the leader was killed and a follower elected | 0 |
| lost | **0 of the 0 that were acknowledged** |

The same pause, the same kill, the same empty new leader — and nothing the producer was
told had been written went missing, because it was told nothing had been. The 2 000
records are gone in both cells; the difference is entirely in what the caller believes.
With `acks=1` a service would have marked 2 000 payments as published; with `acks=all` it
got an error within its delivery timeout and would still hold them, in an outbox, to send
again. This is the comparison the hypothesis asks for, and the first two cells could not
make it.

The producer here runs with idempotence off, as in the `acks=1` cell, and for a second
reason: franz-go's idempotent producer cannot give up on a batch the leader answered
`REQUEST_TIMED_OUT` (the leader may have appended it, and dropping it would break the
sequence numbers), so it retries until the in-sync set answers. The first attempt at this
cell did exactly that and hung for twelve minutes past every deadline set on it. With
idempotence on, `acks=all` still never acknowledges during the pause — it just never
answers at all, which is what exp-09 shows from the other side.

**This does not lean on the stand's shape.** Pausing two of three combined broker and
controller nodes also freezes the KRaft quorum, and a frozen quorum cannot shrink an
in-sync set — which is one reason the followers stayed in it. On a cluster with dedicated
controllers the quorum would stay up and fence a follower silent for longer than
`broker.session.timeout.ms`, and the partition would then go offline instead of electing an
empty leader. The run therefore times the pause: 1 s against the broker's 9 s, so a live
quorum would not have fenced either follower, and the election would have gone the same
way.

**The pause is the experiment, not a trick.** It holds open the window every `acks=1`
write passes through — from the leader's append to the followers' fetch — which on a
local network lasts milliseconds. The first version of this run killed the leader after
the write had returned, found 2 000 of 2 000 readable
([log](results/acks-one-2026-09-21-183122.log)), and called it luck; it was not luck,
the window had closed by construction before the kill landed. In production the window is
as wide as replication lag: a GC pause, a slow disk, a saturated link.

**Holding the followers back with a replication throttle does not work**, which is worth
knowing before planning any test around it — an observation from the attempt that preceded
this run, whose log was not kept, so the numbers below are from the
[journal](../../../docs/00-journal.md) rather than from `results/`. The configs applied and
read back from the cluster; an 8.3 MB log still reached all three replicas instantly. Replication quotas exist
for reassignment traffic, where the destination replica is not in the in-sync set — they do
not restrain replicas that already are.

**The middle setting is measured elsewhere.** With RF 3 and one broker dead,
`min.insync.replicas=2` still accepted every write, with the in-sync set at exactly 2 —
[exp-04](../../kafka_internals/exp-04-isr-leader-election/). Together with this run's
refusal at 3, that is the whole trade: 2 survives one failure and refuses the second.

**Not shown:** `acks=all` with `min.insync.replicas=1` after the in-sync set has shrunk to
the leader alone — the case where `acks=all` loses like `acks=1`. Shrinking the set to one
needs two nodes down, and on three combined broker and controller nodes that takes the
KRaft quorum with it, so the set cannot shrink at all. It is blocked on this stand for the
same reason as exp-04b.

The reasoning, and the false zero this instrument printed before it was fixed, are in the
[journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp08.acks1
make reset-topic TOPIC=exp08.isr3
make reset-topic TOPIC=exp08.all2
make elect-preferred
```
