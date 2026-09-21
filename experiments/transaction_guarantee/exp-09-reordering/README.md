# exp-09 — reordering, and what franz-go does instead

**Hypothesis.** With idempotence off and more than one request in flight, a retried batch
can land after a later one, so a refund is recorded before the capture it refunds. With
idempotence on, the broker's sequence numbers make that impossible.

```
make exp-09
```

A steady two-minute stream of captures and refunds into one partition of a
`min.insync.replicas=3` topic. A follower is frozen and thawed three times, so every
`acks=all` request is refused with `NOT_ENOUGH_REPLICAS` and retried at each edge. Then
the partition is read back and checked. Run twice, one log each.

| Knob | Default | Why |
|---|---|---|
| `DURATION` | 120 s | long enough to cover three freezes with traffic on both sides of each |
| `FLAPS` | 3 | three refusal windows: three chances, not one |

## Result — 2026-09-21

| Producer | Produced | Read back | Out of order | Duplicated | Log |
|---|---|---|---|---|---|
| idempotence off, 5 in flight | 217 840 | 218 131 | **0** | **291** | [run](results/plain-2026-09-21-184103.log) |
| idempotent | 266 980 | 266 980 | 0 | 0 | [run](results/idempotent-2026-09-21-184103.log) |

**franz-go did not reorder. It duplicated.** On a retriable failure it rewinds the partition
to its oldest pending batch and stops pipelining it until a request succeeds, so everything
after the failed batch is resent after it, in order — including batches the broker had
already written. The order survives; exactly-once does not.

The textbook failure is Java-shaped. Anyone hunting for reordered events with this client
would find none, conclude the configuration was fine, and still be charging customers twice.
The conclusion is the same — leave idempotence on — but the symptom is different.

**Not shown:** that franz-go can never reorder. Its own documentation says it "may", and
three refusal windows are three chances. What was shown is that a retriable refusal from
the leader turns the reordering into duplication. The reasoning is in the
[journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp09.payments
```

The run thaws every broker and restores preferred leadership on any exit.
