# exp-02 — hot partition

**Hypothesis.** Key skew turns one partition into the bottleneck, and adding consumers does
not fix it.

```
make exp-02
```

Two cells, three runs each. The **skewed cell**: 20 000 events over 20 merchants, 80% of
them keyed to a single merchant, on `exp02.hotkey`. The **control cell**: the same 20 000
events over a thousand merchants with none hot, on `exp02.uniform`. Each cell is drained
by consumer groups of 1, 2, 3, 6 and 7 members in turn; every group reads from the start
of the topic under its own group id, so each measurement sees exactly the same data.

The control cell is what makes the skewed one mean anything: without it, "adding consumers
did not help" could just as well be a handler, a client or a stand that does not scale.

Each cell also prints its **ceiling** — all records ÷ the records on the hottest partition.
One partition belongs to one member, so no group can drain faster than that member drains
its partition; the ceiling is the most any number of consumers can gain over one.

| Knob | Default | Why |
|---|---|---|
| `EVENTS` | 20 000 | enough that the drain lasts seconds, not milliseconds |
| `HOT_SHARE` | 80 | percentage of events addressed to the one hot merchant (skewed cell) |
| `HANDLER` | 200µs | fixed cost per record, so the number measures handling capacity rather than the network |
| `CONSUMERS` | 1,2,3,6,7 | six is the partition count; seven exists to show what the extra member gets |
| `RUNS` | 3 | each cell is repeated, so a difference between group sizes can be told from noise |

## Result — 2026-09-26, three runs

Speedup over one consumer, lowest–highest of three runs:

| Consumers | Skewed (ceiling 1.17×) | Control (ceiling 5.55×) |
|---|---|---|
| 1 | 5.07–5.35 s | 5.07–5.44 s |
| 2 | 1.06–1.11× | 1.88–2.05× |
| 3 | 1.05–1.10× | 2.78–3.07× |
| 6 | 1.12–1.16× | 5.17–5.82× |
| 7 | 1.14–1.24×, one member idle | 5.20–5.81×, one member idle |

In the skewed cell 85% of the records landed on one partition — the hot merchant's 80%
plus five of the nineteen cold merchants that hashed onto it — so the ceiling is 1.17×, and
every group from two members up sat at it: six members bought 12–16%, which is what the
ceiling allows and no more. The same stand, client and handler scale almost linearly once
the keys are even: six members drained the control cell 5.2–5.8× faster than one. The
limit is the key, not the consumers.

The seventh member handled nothing in either cell. Its cell is sometimes a few percent
faster than the sixth; with one member idle that is run-to-run variance, which is also why
one control run shows 5.82× against a ceiling of 5.55× — the one-consumer baseline it is
divided by varies by about 7% between runs.

[run](results/run-2026-09-26-032720.log). The first run, 2026-09-20, had only the skewed
cell and one pass of it ([log](results/run-2026-09-20-200615.log)); its "seven members
bought 15%" was within that single pass's variance, and is superseded by the table above.
The reasoning, including why partition skew came out sharper than key skew, is in the
[journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp02.hotkey
make reset-topic TOPIC=exp02.uniform
```
