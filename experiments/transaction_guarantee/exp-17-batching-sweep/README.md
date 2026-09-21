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
and batch sizes come from the client's own per-batch metrics. Whether a cell kept up is
`drain`: the time from the last record offered to the last acknowledgement, with lingering
stopped by the flush, so it is a round trip for a cell that kept up and a backlog for one
that did not.

| Knob | Default | Why |
|---|---|---|
| `RATE` | 20 000 records/s | the same for every cell, so a cell that cannot keep up shows it in `drain` |
| `DURATION` | 10 s per cell | long enough for a stable median; the sweep runs about six minutes |

## Result — 2026-09-21

[run 1](results/run-2026-09-21-191906.log) · [run 2](results/run-2026-09-21-192434.log) · [run 3, with `drain`](results/run-2026-09-21-235508.log) · [Java client defaults](results/java-client-defaults.log)

| Linger | Records per batch | Median | p99 | zstd | snappy |
|---|---|---|---|---|---|
| 0 | ≈5 | 2.1–3.2 ms | 10–31 ms, unstable | 2.9–3.2× | 2.0–2.1× |
| 5 ms — Java's default value, set on franz-go | ≈22.5 | 6.6–7.0 ms | 11–19 ms | 4.7–4.8× | 2.8× |
| 10 ms — franz-go's default | ≈35 | 8.1–10.5 ms | 17–24 ms | 5.1–5.2× | 2.95× |
| 50 ms, 16 KiB | ≈40 | 11.7–12.3 ms | 24–31 ms | 5.2× | 3.0× |
| 50 ms, default | ≈145 | 29.8–33.8 ms | 57–63 ms | 5.6× | 3.3× |

Ranges are over all three runs, every codec and both batch ceilings where the row does not
split them. Three single-cell p99 spikes — 74, 125 and 132 ms, each in one run only — are
left out of the p99 column.

Every row is franz-go. The 5 ms row is franz-go set to Java's value, not the Java client:
franz-go's linger flushes every partition bound for a broker once any one of them is due,
and Java's 16 KiB batch ceiling is a separate knob — the nearest thing to Java's defaults in
the grid is `5ms / 16KiB / none`.

**Compression belongs to the batch, not the codec.** zstd went from 3.0× to 5.6× on the
same bytes by changing nothing but how long the producer waited.

**Linger is an upper bound, not a price.** 10 ms added about 6 ms of median. At 50 ms
with a 16 KiB ceiling the median was 12 ms, yet the average batch was ≈40 records of ≈250
bytes — about 10 KB, not 16 KiB. The batches did not each fill: franz-go sends every
partition bound for a broker as soon as any one of them is ready and resets all their
lingers (`ProducerLinger` in its `config.go`), so the first partition to reach 16 KiB
took the rest with it half-full. Only the ≈1 MB ceiling let the producer wait the linger
out. That mechanism is read from the client's documentation; per-partition fill times were
not measured.

**Java's default is 5 ms, not 0** — since Kafka 4.0, printed here by the 4.3.1 client
itself. On franz-go, 10 ms costs 1.5–2 ms of median against 5 ms and buys batches half as
large again.

**No cell fell behind** — now measured rather than inferred: `drain` was 1.3–11.9 ms and
`failed` 0 in all 32 cells of run 3. The first two runs printed `acked/s`, which divided by
the same window as `offered/s` and so could never have shown a backlog.

**Not shown:** throughput and codec CPU cost, since at this load neither differs; both
need a saturated run (exp-17b). Nor is the load exactly fixed: the 5 ms ticker drops ticks,
so offered/s ran 19 670–20 000 against 20 000 asked, within about 1.6%.

The reasoning is in the [journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp17.sweep
```
