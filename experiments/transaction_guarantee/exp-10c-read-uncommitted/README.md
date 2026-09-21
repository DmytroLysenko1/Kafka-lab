# exp-10c — the aborted records are still in the log

**Hypothesis.** An aborted transaction does not stop its records being written. A consumer on `read_uncommitted` — the default in both clients — is handed them.

```
make exp-10c
```

1 000 payments go through a read-process-write loop, one Kafka transaction per batch of
50: each transaction writes the batch to the output topic and commits the consumed offsets
with it. After producing the eleventh batch and before ending its transaction the processor
sends itself `SIGKILL`. A replacement presents the same transactional id — which is how the
broker learns the old transaction is dead and aborts it — and resumes from the last commit.
The loop itself lives in [`../eos/`](../eos/), shared with its two siblings.

## Result — 2026-09-21

[run log](results/run-2026-09-22-022300.log)

| `read_committed` | `read_uncommitted` |
|---|---|
| 1 000 rows, 1 000 distinct | **1 050 rows — the 50 aborted records delivered** |

The transaction withheld the killed batch from `read_committed` readers; it did not keep
it out of the log. A consumer left on the default isolation level processes work that was
rolled back — which, for payments, is work that never happened.

The reasoning is in the [journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp10c.input
make reset-topic TOPIC=exp10c.output
```
