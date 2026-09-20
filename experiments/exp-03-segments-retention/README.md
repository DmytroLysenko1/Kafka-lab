# exp-03 — segments, retention and compaction

**Hypothesis.** A compacted topic keeps the last value of each key rather than the history,
and shows it only once a segment is closed. Retention removes whole segments, so a topic
holds more than its setting suggests.

```
make exp-03
```

Two single-partition topics: one compacted, one kept for five seconds. The run resets both
before measuring — an experiment that reads a log has to own the log it reads — then writes,
rolls the segment and waits for the log to change rather than sleeping for a guessed while.

| Knob | Default | Why |
|---|---|---|
| `KEYS` × `UPDATES` | 50 × 40 | enough updates per key that compaction has something to remove |
| `TOMBSTONES` | 10 | keys deleted afterwards, to see what compaction does with a delete |
| `RECORDS` | 2 000 | written to the topic kept for five seconds |

Segments roll on `segment.ms`, not `segment.bytes`: **Kafka 4.x refuses a `segment.bytes`
below 1 MiB**, so the advice to shrink it to kilobytes no longer works.

## Result — 2026-09-20

| Topic | Before cleaning | After cleaning |
|---|---|---|
| compacted | 2 010 records, 40 live keys, 10 tombstones | 50 records, 40 live keys, 10 tombstones |
| kept 5 s | 2 000 records, log starts at 0 | 0 readable, log starts at 2 000, open segment survives |

The tombstones surviving is the correct answer, not a missing feature: they are kept for
`delete.retention.ms` so every consumer can learn the key is gone.

The reasoning is in the [journal](../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp03.compact
make reset-topic TOPIC=exp03.retention
```
