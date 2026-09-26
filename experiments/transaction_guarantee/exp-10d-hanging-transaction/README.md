# exp-10d — a transaction left open

**Hypothesis.** A `read_committed` reader cannot read past the last stable offset, and an
open transaction pins it. So one producer that begins a transaction and never ends it stops
every `read_committed` reader of that partition — including readers of producers who use no
transaction at all — until the broker aborts it.

```
make exp-10d
```

One producer writes one record inside a transaction and abandons it, as a producer that
hangs or loses its thread after producing would. A second producer, with no transaction,
writes 100 records after it. The run times how long each isolation level takes to be
handed those 100.

| Knob | Default | Why |
|---|---|---|
| `TRANSACTION_TIMEOUT` | 20 s | short enough to run, long enough to be unmistakable next to fetch latency |
| `RECORDS` | 100 | the unrelated producer's writes |

## Result — 2026-09-21, and 2026-09-26

[run 1](results/run-2026-09-21-190032.log) · [run 2](results/run-2026-09-26-025347.log) ·
[broker transaction timers](results/broker-transaction-timers.log)

| | run 1 | run 2 |
|---|---|---|
| high watermark / last stable offset | **101 / 0** | **101 / 0** — 101 records of lag, none deliverable |
| `read_uncommitted` saw the 100 after | **0 s** | **0 s** |
| `read_committed` saw them after | **23.2 s** | **27.3 s** |

Run 2 was made after the offset-read guard went in (`labkit.OffsetAt`): run 1's "last stable
offset 0" was read by the instrument that could turn a failed lookup into 0, and run 2 reads
the same 0 through one that refuses to.

An unrelated producer with no transaction was held back for the whole life of someone
else's. Every dashboard would show 101 records of lag while nothing could be read — "lag
without messages", made on purpose.

**The stall is longer than the timeout.** The coordinator scans for expired transactions
every `transaction.abort.timed.out.transaction.cleanup.interval.ms`, 10 s by default, so a
20 s timeout releases readers between 20 and 30 s — 23.2 s and 27.3 s in the two runs. franz-go defaults the timeout to 40 s
(Java 60 s), and `transaction.max.timeout.ms` lets a producer ask for up to 15 minutes. The
clock starts at the transaction's first record, not at `Begin`.

The reasoning is in the [journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp10d.stream
```
