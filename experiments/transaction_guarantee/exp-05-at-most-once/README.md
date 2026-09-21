# exp-05 — at-most-once

**Hypothesis.** Committing the offset before handling the record is the cheapest possible
consumer, and it buys that cheapness with payments. A crash between the commit and the
write leaves the cluster convinced the record was handled, and no restart will ever fetch
it again.

```
make exp-05
```

1 000 payments are produced onto this experiment's own topic. The consumer commits each
batch's offsets and only then writes its payments to Postgres. Halfway through it sends
itself `SIGKILL` — a real crash, so no deferred close runs and nothing is flushed — and a
second process resumes from whatever the first committed. The counts are read back out of
the database, because the dead process's memory is not evidence of anything.

| Knob | Default | Why |
|---|---|---|
| `PAYMENTS` | 1 000 | enough that one lost batch is a number, not noise |
| `DIE_AFTER` | 500 | halfway, so the crash has a before and an after |

## Result — 2026-09-21

[run log](results/run-2026-09-21-180311.log)

| Rows | Distinct | Outcome |
|---|---|---|
| 951 | 951 | **49 payments lost** |

The missing 49 are one batch minus the record that was being written when the process
died. Their offsets had already been stored, so as far as the group is concerned they were
handled, and the resuming consumer starts past them. Nothing in the cluster reports this:
there is no error, no lag, and no retry — the payments simply never existed for anyone
downstream.

**Where the crash lands decides whether there is anything to see.** Killing the process on
the last record of a batch would commit exactly what had been written and lose nothing,
which is how the first version of this run reported "every payment exactly once" for the
semantics whose entire purpose is to lose payments. The kill now refuses to land there.

Its two siblings run the same stand with the other two orderings — [exp-06](../exp-06-at-least-once/)
and [exp-07](../exp-07-inbox/) — and all three share [`../stand/`](../stand/), because they
differ in nothing else. The reasoning is in the [journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp05.payments
```

`delivery_handled` and `delivery_inbox` are keyed by run id and mode, so runs never read
each other's rows and nothing needs dropping between them.
