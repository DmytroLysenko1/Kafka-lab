# exp-10b — where a Kafka transaction stops

**Hypothesis.** Writing to Postgres inside a transactional loop is not covered by the transaction: after a crash the Kafka output is exactly once and the database rows are not.

```
make exp-10b
```

1 000 payments go through a read-process-write loop, one Kafka transaction per batch of
50: each transaction writes the batch to the output topic and commits the consumed offsets
with it. After producing the eleventh batch and before ending its transaction the processor
sends itself `SIGKILL`. A replacement presents the same transactional id — which is how the
broker learns the old transaction is dead and aborts it — and resumes from the last commit.
The loop itself lives in [`../eos/`](../eos/), shared with its two siblings.
This one also writes every payment to Postgres inside the loop.

## Result — 2026-09-21

[run log](results/run-2026-09-21-185649.log)

| `read_committed` | Postgres |
|---|---|
| **1 000 rows, 1 000 distinct** | **1 050 rows, 50 duplicated** |

Same crash, same rollback, and the rows written for the killed batch survived it. Nothing
ever told Postgres a transaction existed, so there was nothing to abort, and the
replacement wrote those 50 payments again. This is the one misreading case 10 exists to
prevent: reaching for Kafka transactions to protect a database write looks correct until
the process dies between the two.

What makes the database write safe is an idempotent write — the inbox of
[exp-07](../exp-07-inbox/). The transaction makes the Kafka side safe. Neither covers the
other.

`eos_handled` deliberately has no unique key on `payment_id`: it stands for any side effect
a Kafka transaction cannot reach, and a constraint there would be the very idempotency this
experiment shows the transaction does not provide.

The reasoning is in the [journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp10b.input
make reset-topic TOPIC=exp10b.output
```
