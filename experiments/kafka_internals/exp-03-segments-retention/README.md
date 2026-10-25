# exp-03 — segments, retention and compaction

**Hypothesis.** A compacted topic keeps the last value of each key rather than the history,
and shows it only once a segment is closed. Retention removes whole segments, so a topic
holds more than its setting suggests.

```
make exp-03
```

Three single-partition topics: one compacted, one kept for five seconds, and a control kept
for five seconds whose segments roll on the broker's own schedule. The run resets all three
before measuring — an experiment that reads a log has to own the log it reads — then writes,
rolls the segment and waits for the log to change rather than sleeping for a guessed while.

| Knob | Default | Why |
|---|---|---|
| `KEYS` × `UPDATES` | 50 × 40 | enough updates per key that compaction has something to remove |
| `TOMBSTONES` | 10 | keys deleted afterwards, to see what compaction does with a delete |
| `RECORDS` | 2 000 | written to each topic kept for five seconds |

Segments roll on `segment.ms`, not `segment.bytes`: **Kafka 4.x refuses a `segment.bytes`
below 1 MiB**, so the advice to shrink it to kilobytes no longer works.

## Result — 2026-09-20, repeated 2026-09-26

[run log](results/run-2026-09-20-200653.log) · [repeat](results/run-2026-09-26-025322.log),
made after the offset-read guard went in (`labkit.OffsetAt`): every number below came out
identical. Two earlier runs under
[`results/superseded/`](results/superseded/) were produced by a broken instrument and back
no number here.

| Topic | Before cleaning | After cleaning |
|---|---|---|
| compacted | 2 010 records, 40 live keys, 10 tombstones | 50 records, 40 live keys, 10 tombstones |
| kept 5 s | 2 000 records, log starts at 0 | 0 readable; the log starts past the last of them |

The tombstones surviving is the correct answer, not a missing feature: they are kept for
`delete.retention.ms` so every consumer can learn the key is gone.

**Half the hypothesis did not survive.** "A topic holds more than its setting suggests"
needs a record older than `retention.ms` still readable because its segment is open. This
run produced none: it rolls segments every second, so aged data always lands in a closed
segment and is always dropped. All that survives is the roll markers the instrument itself
wrote seconds earlier. What was shown is the clean half — closed segments age out whole.
The other half is listed as outstanding in
[case 02](../../../docs/static/02-log-segments-retention.md#still-to-be-measured).

## Result — 2026-10-25, three runs, with the values read and the control

[17:25](results/run-2026-10-25-172546.log) · [17:26](results/run-2026-10-25-172658.log) ·
[17:27](results/run-2026-10-25-172759.log), identical in every number below. The runs above
counted keys and tombstones; these also read each surviving value back, and add the control
the first half of the hypothesis needed.

| Cell | Result |
|---|---|
| compacted | 2 010 records → 50; **40 of 40 live keys hold their last update**, `{"update":39}` — the last value, read off the log, not inferred from the count |
| kept 5 s, a segment every second | 2 000 → 0 readable |
| control: kept 5 s, default `segment.ms` (7 days), a fresh record into the same segment every 2 s | **2 000 of 2 000 still readable after 15 s** — three times their retention — then **0** after 15 s with nothing new |

**Retention judges a segment by its newest record.** The control's 2 000 records were past
`retention.ms` three times over and stayed readable, because the segment holding them kept
receiving younger records; once those stopped, the segment's newest record aged out and the
whole batch went with it. That is the half of the hypothesis the first runs could not show: a
topic does hold more than its setting suggests, for as long as old records share a segment
with new ones — up to `segment.ms` of them on a quiet partition's default of 7 days.

**And the active segment is not safe from deletion.** In the control's last phase nothing
new arrived and nothing rolled the segment — `segment.ms` was 7 days — yet within 15 s the
whole batch was gone and the log started past it. The broker rolls an active segment whose
newest record has expired and deletes it — so "the active segment always survives" holds for
compaction, never cleaned while open, but not for retention.

`segment.bytes` below 1 MiB is refused by Kafka 4.x — the rejection is
[captured](results/segment-bytes-rejected.log) rather than quoted from memory.

The reasoning is in the [journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp03.compact
make reset-topic TOPIC=exp03.retention
make reset-topic TOPIC=exp03.retention-open
```
