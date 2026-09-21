# exp-05/06/07 — delivery semantics under a killed consumer

**Hypothesis.** At-most-once, at-least-once and effectively-once are not three libraries or
three settings. They are one ordering decision — where the offset is committed relative to
the write — and a crash in the wrong place turns each into a different kind of incident.

```
make exp-05
```

One stand, three modes. 1 000 payments are produced, a consumer writes each to Postgres,
and partway through it sends itself `SIGKILL`: a real crash, so no deferred close runs and
nothing is flushed. A second process resumes from whatever the first committed, and the
counts are read back out of the database — the dead process's memory is not evidence.

| Mode | What the consumer does |
|---|---|
| `at-most-once` | commit the batch's offsets, then write each payment |
| `at-least-once` | write each payment, then commit the batch's offsets |
| `inbox` | claim the payment id and write it in one transaction, then commit |

| Knob | Default | Why |
|---|---|---|
| `PAYMENTS` | 1 000 | enough that one lost batch is a number, not noise |
| `DIE_AFTER` | 500 | halfway, so the crash has a before and an after |

## Result — 2026-09-21

[run log](results/run-2026-09-21-175334.log)

| Mode | Rows | Distinct | Outcome |
|---|---|---|---|
| at-most-once | 951 | 951 | **49 payments lost** |
| at-least-once | 1 050 | 1 000 | **50 payments charged twice** |
| inbox | 1 000 | 1 000 | **exactly once** |

Both failures are the same window — one batch of 50 — seen from opposite sides. Committed
before writing, the records not yet written when the process died are gone and no restart
will fetch them. Written before committing, the whole batch arrives again.

**The inbox run was delivered the duplicates too.** It received 50 records twice, exactly as
the at-least-once run did, and still holds 1 000 rows: the claim and the write are one
transaction, so the second delivery loses the race for the primary key and writes nothing.
*Exactly-once effect, at-least-once delivery.*

The reasoning, and the two ways this instrument was wrong before it was right, are in the
[journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp05.payments
```

The tables `exp05_handled` and `exp05_inbox` are keyed by run id, so runs never read each
other's rows and nothing needs dropping between them.
