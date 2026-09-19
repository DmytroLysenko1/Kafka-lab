# Lab journal

One entry per run: what was expected, what the cluster did, and what was surprising. The
entries that matter most are the ones where the hypothesis did not survive contact — those
cannot be written without doing the run.

Numbers here are the source; the case files under [`static/`](static/) quote them.

## exp-01 — the key decides the order a payment is handled in

Date: 2026-09-20 · [run logs](../experiments/exp-01-partition-keys/results/) ·
`make exp-01`

**Hypothesis.** Without a key, the events of one payment scatter across partitions and lose
their business order. With `key=payment_id` they stay in one partition and the order holds.

**Setup.** Two topics of 6 partitions, RF 3, `min.insync.replicas=2`; 10 000 events over
100 payments (100 events each, ~100 bytes), produced twice — once with no key, once keyed
by `payment_id`. A single consumer reads from the start and records every event into
Postgres in handling order; violations are counted in Go over that order, not over offsets.

**Result.** Three runs:

| Run | Payments split across partitions | Order violations |
|---|---|---|
| keyless | 100 of 100, every time | **6 448 · 7 838 · 7 964** of 10 000 |
| keyed | 0 | **0 · 0 · 0** |

**What was surprising, twice.**

*The magnitude.* The plan predicted roughly 4 000 violations for the keyless run and the
cluster produced between six and eight thousand — up to four in five events. The estimate
had assumed a payment straddles two partitions; in fact every one of the 100 payments had
its events spread over all six, because the producer switches partition once 64 KiB have
gone to the current one and each payment contributes only ~10 KB. The consumer then drains
partition by partition, so almost every event of a payment arrives after a later one.

*The spread.* The keyless number is not a constant — 6 448, 7 838 and 7 964 across three
runs of identical input. Partition switching and the order in which a fetch returns
partitions both vary, so this is a shape, not a figure: "most events, unpredictably how
many". The keyed column, by contrast, is exactly zero every time, which is what a guarantee
looks like next to a tendency.

**What the instrument got wrong first.** The first version of the experiment would have
reported a rerun as if it were a fresh run: it read the topic from offset 0 and stopped
after N records, so a second `make exp-01` would have counted the *previous* run's events
while the new ones sat unread past the cursor — a plausible, self-consistent, wrong number.
Every event now carries a run id, the consumer counts only its own and refuses to report
unless the count matches exactly. The fix is visible in the numbers above: two runs
executed back to back returned 6 448 and 7 964, where the broken version would have
returned the same figure twice.

**Conclusion.** The key is not a detail of throughput, it is the ordering unit. Ordering
survives only inside one partition, so "ordered" means "keyed by the entity whose order you
care about". The keyed run producing zero is also the control on the instrument: a non-zero
number there would have meant the counter, not Kafka, was broken.

**Carried into:** [`static/01-write-path.md`](static/01-write-path.md) step 4 and its
measurement table.
