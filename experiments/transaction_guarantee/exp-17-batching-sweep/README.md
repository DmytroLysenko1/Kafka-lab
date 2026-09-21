# exp-17 — linger, batch size and the codec, under one fixed load

**Hypothesis.** Linger trades latency for throughput: waiting fills batches, and full
batches compress better and cost fewer requests. franz-go's defaults — linger 10 ms, a ≈1 MB
batch ceiling, snappy — have already made that trade, and the difference from Java's should
be visible and priced.

```
make exp-17
```

32 cells — linger 0 / 5 / 10 / 50 ms × batch ceiling 16 KiB / default × codec none / snappy /
lz4 / zstd — each offered the same load. The payload is generated payment JSON, seeded, so
every codec compresses identical bytes; a repeated-character padding would compress to
nothing and make every codec look brilliant. Latency is hand-off to acknowledgement; bytes
and batch sizes come from the client's own per-batch metrics.

| Knob | Default | Why |
|---|---|---|
| `RATE` | 20 000 records/s | the same for every cell, so a cell that cannot keep up shows it |
| `DURATION` | 10 s per cell | long enough for a stable median; the sweep runs about six minutes |

## Result — 2026-09-21

[run 1](results/run-2026-09-21-191906.log) · [run 2](results/run-2026-09-21-192434.log) · [Java client defaults](results/java-client-defaults.log)

| Linger | Records per batch | Median | p99 | zstd | snappy |
|---|---|---|---|---|---|
| 0 | ≈5 | 2.1–3.4 ms | 11–33 ms, unstable | 2.9–3.1× | 2.0–2.1× |
| 5 ms — Java since 4.0 | ≈22.5 | 6.6–6.9 ms | 11–19 ms | 4.7× | 2.8× |
| 10 ms — franz-go | ≈34 | 8.1–9.1 ms | 16–24 ms | 5.1× | 2.95× |
| 50 ms, 16 KiB | ≈40 | 11.7–12.2 ms | 23–32 ms | 5.2× | 3.0× |
| 50 ms, default | ≈140 | 29.8–33.1 ms | 57–65 ms | 5.6× | 3.3× |

**Compression belongs to the batch, not the codec.** zstd went from 3.0× to 5.6× on the
same bytes by changing nothing but how long the producer waited.

**Linger is an upper bound, not a price.** 10 ms added about 6 ms of median. At 50 ms a
16 KiB batch filled in about 12 ms and shipped; only the ≈1 MB ceiling let the producer wait
the linger out.

**Java's default is 5 ms, not 0** — since Kafka 4.0, printed here by the 4.3.1 client
itself. franz-go's 10 ms costs 1.5–2 ms of median against it and buys batches half as large
again.

**Not shown:** throughput and codec CPU cost. At this load no cell fell behind, so neither
differs; both need a saturated run. Single-cell p99 is noisy — two cells spiked to 74 and
125 ms in the second run only — which is why the table gives ranges over both runs.

The reasoning is in the [journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp17.sweep
```
