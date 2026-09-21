# exp-06 — at-least-once

**Hypothesis.** Moving the commit to after the write removes the loss and buys a different
incident: a crash before the commit means the whole batch is fetched again, and every
payment in it is charged a second time.

```
make exp-06
```

The same stand as [exp-05](../exp-05-at-most-once/), with the two statements swapped. The
consumer writes each payment to Postgres and only then commits the batch's offsets. Halfway
through it sends itself `SIGKILL`, and a second process resumes from the last commit.

| Knob | Default | Why |
|---|---|---|
| `PAYMENTS` | 1 000 | enough that one replayed batch is a number, not noise |
| `DIE_AFTER` | 500 | halfway, so the crash has a before and an after |

## Result — 2026-09-21

[run log](results/run-2026-09-21-180404.log)

| Rows | Distinct | Outcome |
|---|---|---|
| 1 050 | 1 000 | **50 payments charged twice** |

Distinct is exactly 1 000, which is the tell: nothing was lost, and 50 rows are second
copies. The replayed batch is the same window exp-05 loses — one crash, one instant, and
the opposite failure, decided by which of two statements comes first.

**This is what Kafka actually offers.** At-least-once is not a weaker mode you opt into; it
is what a redelivering broker plus a non-idempotent consumer always adds up to. The fix is
not a stronger delivery guarantee, it is a write that does not care how many times the
record arrives — [exp-07](../exp-07-inbox/).

All three share [`../stand/`](../stand/). The reasoning is in the
[journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp06.payments
```
