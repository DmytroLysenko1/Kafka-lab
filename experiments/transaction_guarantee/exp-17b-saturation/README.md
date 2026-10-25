# exp-17b — the exp-17 producer flat out: throughput and what the codecs cost

**Hypothesis.** At a fixed 20 000 records/s ([exp-17](../exp-17-batching-sweep/)) no
configuration fell behind, so throughput and codec CPU could not differ. With nothing
holding the producer back they should: compression should cost producer CPU and buy bytes,
and which of the two limits throughput depends on where the ceiling is.

```
make exp-17b
```

exp-17's program with `-saturate`, on exp-17's topic. Eight cells — linger 0 and franz-go's
10 ms, each of the four codecs, the default ≈1 MB batch ceiling — each produce as fast as
franz-go's 10 000-record buffer allows for 10 s. The 200 000 payment events are generated
before the sweep and cycled, so generating JSON is not in the CPU measured and every codec
compresses the same bytes in the same order. CPU is the producer process's user plus system
time over the cell, per megabyte of uncompressed payload; the `none` row is the client's own
cost, and a codec's cost is its row above that.

| Knob | Default | Why |
|---|---|---|
| `DURATION` | 10 s per cell | long enough for a stable rate; the sweep runs under two minutes |

## Result — 2026-09-22

[1](results/run-2026-09-22-020202.log) · [2](results/run-2026-09-22-020336.log) ·
[3](results/run-2026-09-22-021448.log) · [4](results/run-2026-09-22-021622.log) — ranges over
all four runs and both lingers

| Codec | Records/s acknowledged | MB/s in | MB/s on the wire | Ratio | Producer CPU, ms per MB |
|---|---|---|---|---|---|
| none | 180 000–356 000 | 45–89 | 45–89 | 1.00× | 4.5–5.6 |
| snappy | 556 000–834 000 | 139–209 | 42–62 | 3.35× | 4.6–4.7 |
| lz4 | 407 000–782 000 | 102–196 | 29–56 | 3.53–3.54× | 5.1–5.3 |
| zstd | 591 000–923 000 | 148–232 | 26–40 | 5.78–5.79× | 7.1–7.5 |

Linger made no consistent difference at saturation — 0 was faster in some cells and 10 ms in
others — and every codec compressed better than under exp-17's fixed load, zstd 5.78–5.79×
against 5.64–5.65× at best there. Both fit batches filling far faster than any linger
expires; the 2026-10-25 runs below print the batch sizes, and they confirm it.

**Compression bought throughput, not just bytes.** Uncompressed, the producer put at most
89 MB/s on the wire and got no further. Every codec put less on the wire and delivered more
records: against `none` in the same run and linger, snappy 1.7–4.6×, lz4 1.1–3.1× and zstd
2.1–4.7×. On this stand the brokers share a laptop with the producer and write every byte
three times, so bytes, not the producer's CPU, were most likely the first ceiling for
`none` — an inference from the stand's shape, not a measurement: no broker, disk or network
figure was taken. Where the ceiling moved to for the compressed cells is not isolated
either; zstd delivered more records while putting less on the wire than `none` did, so the
two cells were not held by the same limit.

**What the codecs cost the producer.** Against `none` in the same run and linger, zstd
added 1.9–3.0 ms of CPU per megabyte — 34–67% more — for 5.78× compression. snappy and lz4
came out between 0.9 ms cheaper and 0.6 ms dearer: sending a third of the bytes saves about
as much CPU as compressing them costs, so their net price is nothing measurable at this
ratio.

**The spread is wide, and it is the stand.** `none` ranged from 180 000 to 356 000
records/s across four runs of the same cell, and its p99 from 107 ms to 2.1 s, because the
three brokers, the producer and everything else on this laptop compete for ten cores. The
ordering — every codec above `none`, zstd highest — held in every run; the magnitudes did
not.

## The batch ceiling on its own — 2026-10-25, three runs

[17:42](results/run-2026-10-25-174242.log) · [17:44](results/run-2026-10-25-174456.log) ·
[17:47](results/run-2026-10-25-174713.log). The same eight cells, now printing records per
batch from the client's own batch metrics, and four more: codec `none`, linger 0 and 10 ms,
the batch ceiling at 16 KiB against the default — so the only thing that differs inside a
pair is how many records a request may carry.

| Batch ceiling, codec `none` | Records per batch | Records/s acknowledged | Producer CPU, ms per MB | p99 |
|---|---|---|---|---|
| 16 KiB | **64** | **154 000–265 000** | 7.8–8.8 | 130–451 ms |
| default ≈1 MB | 288–308 | **371 000–527 000** | 4.2–4.5 | 53–107 ms |

**Bigger batches bought throughput, measured on their own.** In every run and at both
lingers the default ceiling delivered 1.4–3.0× the records of a 16 KiB one, at half the
producer CPU per megabyte and a lower p99: a request costs about the same whatever it
carries, and at 16 KiB the producer sent four to five times as many of them for the same
records. Full batches were the ceiling at 16 KiB — every batch held exactly 64 records — and
never at the default, where batches averaged about 300 records, 75 KB, a thirteenth of what
the ceiling allows: the buffer of 10 000 records spread over six partitions runs out before
a batch can grow further. That is the measurement behind "batches fill far faster than any
linger expires" above, and behind the checklist's advice to leave the ceiling at the default.

The eight codec cells reproduced the earlier ordering in all three runs — every codec above
`none` — with zstd no longer always highest: lz4 led in four of the six run-and-linger pairs.

**Not shown:** broker CPU and disk, which the codec also changes; a producer on its own
host, where the byte ceiling would be the network rather than three local replicas; and
tail latency, which is dominated here by time spent in a full buffer.

## Cleaning up

```
make reset-topic TOPIC=exp17.sweep
```
