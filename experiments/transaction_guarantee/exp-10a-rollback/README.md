# exp-10a — a transaction rolls back output and offsets together

**Hypothesis.** A crash in the middle of a Kafka transaction rolls back both the records it wrote and the offsets it consumed, so a `read_committed` reader of the output sees every payment exactly once.

```
make exp-10a
```

1 000 payments go through a read-process-write loop, one Kafka transaction per batch of
50: each transaction writes the batch to the output topic and commits the consumed offsets
with it. After producing the eleventh batch and before ending its transaction the processor
sends itself `SIGKILL`. A replacement presents the same transactional id — which is how the
broker learns the old transaction is dead and aborts it — and resumes from the last commit.
The loop itself lives in [`../eos/`](../eos/), shared with its two siblings.

## Result — 2026-09-21

[run log](results/run-2026-09-21-185537.log)

| `read_committed` | `read_uncommitted` |
|---|---|
| **1 000 rows, 1 000 distinct** | 1 050 |

The killed batch's 50 records were written and then aborted; its offsets were never
committed, so the replacement processed the batch again, and this time committed it. A
`read_committed` reader saw each payment once. The 50 extra rows a `read_uncommitted`
reader is handed are the aborted copies — [exp-10c](../exp-10c-read-uncommitted/) is about
exactly them.

The reasoning is in the [journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp10a.input
make reset-topic TOPIC=exp10a.output
```
