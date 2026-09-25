# exp-02 — hot partition

**Hypothesis.** Key skew turns one partition into the bottleneck, and adding consumers does
not fix it.

```
make exp-02
```

One topic of six partitions, 20 000 events over 20 merchants with 80% of them keyed to a
single merchant, drained by consumer groups of 1, 2, 3, 6 and 7 members in turn. Every
group reads from the start of the topic under its own group id, so each measurement sees
exactly the same data.

| Knob | Default | Why |
|---|---|---|
| `EVENTS` | 20 000 | enough that the drain lasts seconds, not milliseconds |
| `HOT_SHARE` | 80 | percentage of events addressed to the one hot merchant |
| `HANDLER` | 200µs | fixed cost per record, so the number measures handling capacity rather than the network |
| `CONSUMERS` | 1,2,3,6,7 | six is the partition count; seven exists to show what the extra member gets |

## Result — 2026-09-20

| Consumers | Time to drain | Idle members |
|---|---|---|
| 1 | 5.63 s | 0 |
| 2 | 5.12 s | 0 |
| 3 | 4.95 s | 0 |
| 6 | 4.81 s | 0 |
| 7 | 4.76 s | 1 |

85% of the records landed on one partition — the hot merchant's 80% plus five of the
nineteen cold merchants that hashed onto the same partition — and one consumer handled
that partition in every run. Seven times the members bought 15%.

The reasoning, including why partition skew came out sharper than key skew, is in the
[journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp02.hotkey
```
