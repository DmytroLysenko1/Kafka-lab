# exp-15 — raising a replication factor on a live topic

Three partitions at RF 2 holding 60 MiB, moved onto a third broker while producers keep
writing one record every 10 ms. Twice: at full speed, and throttled to 1 MiB/s.

```
make exp-15
```

## What it measures

| | cell A — no throttle | cell B — 1 MiB/s |
|---|---|---|
| how long the move took | **2.2 s · 2.5 s** | **24.1 s · 24.8 s** |
| copied onto the new replicas | 66.3 · 66.5 MiB | 69.2 · 71.1 MiB |
| effective replication rate | ~30 MiB/s | ~2.8 MiB/s across three brokers |
| producer median, before → during | 2.6 → 1.7 ms · 2.9 → 2.1 ms | 2.9 → 1.7 ms · 2.8 → 1.6 ms |
| producer p95, before → during | 6.1 → 5.3 ms · 5.8 → 8.2 ms | 6.8 → 3.4 ms · 6.7 → 3.9 ms |

Two runs, 2026-09-25, in [`results/`](results/).

## What it shows

**The throttle does exactly what it says.** A megabyte a second per broker turned a 2.2 s
move into a 24.8 s one, and the arithmetic matches: about 22 MiB arriving at each of three
brokers at 1 MiB/s. If you need to know how long a move will take, this is the number to
divide by — not the aggregate, the per-broker rate.

**And on this stand it bought nothing.** Producer latency did not get worse during the
unthrottled move — median and p95 during the copy were no higher than before it, in either
cell and in both runs. That is an honest null result rather than a recommendation: three
brokers on one laptop share a local SSD and a loopback network, and 66 MiB at 30 MiB/s
never came close to saturating either. On a cluster where replication and client traffic
compete for the same NIC, the throttle is the difference between a planned move and an
incident — this run simply cannot show that, and says so rather than implying it.

**What it does show is the shape of the operation.** A replication factor is not a setting
you edit. topicctl refuses the change outright — *"Replication in topic config (2) is not
equal to observed max ISR (3); this cannot be resolved by topicctl"* — so the experiment
has to drop and recreate the topic between cells. The move itself is a plan file, an
`--execute`, and a `--verify` you must keep running until it reports completion, because
`--verify` is also what removes the throttle it set. Forget it and every later replication
on that cluster crawls at a megabyte a second, with nothing in the topic's own config to
explain why. The tool warns about this, in one line, once.

## The run that measured nothing

The first version preloaded 60 MiB of a repeating byte pattern. The brokers stored 2.2 MiB
each: the batches compressed to almost nothing, the "60 MiB" move had six megabytes to
copy, and it finished in 2.4 s **with the throttle on** — which looked exactly like a
throttle that does not work, and was reported as such for about ten minutes.

The topic's own log directories settled it: 2.2 MiB per broker where 40 was expected. The
payload is incompressible random bytes now, and the run reports what the brokers actually
hold (121.2 MiB for 60 MiB produced at RF 2) rather than what it thinks it produced.
