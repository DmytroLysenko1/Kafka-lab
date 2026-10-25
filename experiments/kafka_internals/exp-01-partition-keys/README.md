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
| `EVENTS` | 10 000 | ~170 bytes each — a JSON event with 60 bytes of filler — so ~280 KB reach every partition, well past the 64 KiB after which the keyless producer switches partition. A smaller run looks ordered and lies (measured below) |
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

## Below and above the switch — 2026-10-25

"A smaller run looks ordered" was the knob table's argument until these runs:

| `EVENTS` | bytes produced, keyless | payments split across partitions | order violations | runs |
|---|---|---|---|---|
| 200 | ~34 KB — under 64 KiB | **0** | **0** | [17:31:15](results/run-2026-10-25-173115.log) · [17:31:27](results/run-2026-10-25-173127.log) |
| 500 | ~85 KB — over 64 KiB once | **100 of 100** | **381** | [17:30:18](results/run-2026-10-25-173018.log) · [17:30:31](results/run-2026-10-25-173031.log) |

Keyed, both sizes: 0 and 0. At 200 events the keyless producer never left its first
partition, so every payment was read in order and the test a team would run on a laptop
passes; 500 events crossed the threshold once, and one switch was enough to split every
payment, because each payment's events are spread across the whole run. The cliff is the
partitioner's byte count, not the event count — larger events reach it sooner.

## Cleaning up

The topics belong to this experiment, not to the catalog:

```
make reset-topic TOPIC=exp01.keyless   # recreate from YAML
make reset-topic TOPIC=exp01.keyed
```
