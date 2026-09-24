# exp-01 — partition keys, ordering and parallelism

**Hypothesis.** Without a key the events of one payment scatter across partitions and lose
their business order. With `key=payment_id` they land in one partition and the order holds,
at the cost of capping parallelism at the partition count.

```
make exp-01
```

It creates the two topics from [`topics/`](topics/), produces the same 10 000 events over
100 payments twice — keyless, then keyed — and writes the report to `results/`.
`make exp-01` runs `make check` first: a measurement taken on a cluster that drifted from
the catalog is a number about the drift.

| Knob | Default | Why |
|---|---|---|
| `EVENTS` | 10 000 | ~100 bytes each, so ~170 KB reach every partition — well past the 64 KiB after which the keyless producer switches partition. A smaller run looks ordered and lies |
| `PAYMENTS` | 100 | 100 events per payment, enough that a payment spans partitions rather than landing in one by luck |

## Why a run id

The topic is never truncated between runs, so a second run appends to what the first one
left. Without a marker the consumer would read from the start, stop after N records and
count the *previous* run — a plausible, self-consistent, wrong report. Every event carries
the id of the run that produced it, the consumer keeps only its own, and it refuses to
report unless the count matches exactly what it produced.

## How it counts

The consumer records every event into Postgres **in handling order** (`handled_seq`), which
is the order the business sees; offsets only order records inside one partition. An event
is a violation when a later event of the same payment was already handled. A repeat of the
newest event is not a violation — at-least-once delivery produces those, and counting them
would inflate every run. Both rules are covered by `violations_test.go`, because the
instrument has to be trustworthy before the measurement means anything.

## Result — seven runs, 2026-09-20 to 2026-09-24

| Run | Payments split across partitions | Order violations |
|---|---|---|
| keyless | 100 of 100, every time | 6 827 · 7 206 · 7 962 · 7 964 · 7 964 · 8 343 · 8 721 of 10 000 |
| keyed | 0 | 0, every run |

The keyless figure is a shape, not a constant: partition switching and fetch order vary
between runs. Three runs made it look tighter than it is — the first three landed on
7 964, 7 964 and 8 721, and the next four widened the spread to 6 827–8 721, about a fifth
of the lowest. What does not vary is that every payment is split and most of its events
arrive out of order. The keyed column is exactly zero every time, which is the difference between
a guarantee and a tendency — and it is also the control on the instrument, since a non-zero
number there would mean the counter was broken rather than Kafka.

The full story, including where the estimate in the plan was wrong, is in the
[journal](../../../docs/00-journal.md).

## Cleaning up

The topics belong to this experiment, not to the catalog:

```
make reset-topic TOPIC=exp01.keyless   # recreate from YAML
make reset-topic TOPIC=exp01.keyed
```
