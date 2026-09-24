# exp-11 — a record nobody can decode

One undecodable record among a hundred good ones, read twice by the same consumer: once
with a dead letter topic and once without. Nothing else differs between the cells.

```
make exp-11
```

## What it runs

The consumer is the service's own ([`internal/infrastructure/kafka/consumer.go`](../../../internal/infrastructure/kafka/consumer.go)),
not a stand written for the occasion — the claim being tested is about the code that runs
in production, so that is the code the experiment runs. The only thing swapped is the sink
it hands an unreadable record to: the real dead letter topic in cell B, and in cell A a
sink that refuses, which is what a service without the route configured amounts to.

Each cell gets its own topic, its own consumer group, its own merchant and its own range of
event ids. The last part matters: the inbox deduplicates by event id, so overlapping ranges
would make one cell's records look like duplicates of the other's.

A restart is a new client. franz-go keeps its own fetch position, so calling `Run` again on
the same client walks past the record that killed it — which measures the harness rather
than the service. Each restart therefore builds a new consumer, which resumes from the last
committed offset, exactly as a restarted pod does.

## What it measures

| | cell A — no dead letter route | cell B — dead letter topic |
|---|---|---|
| payments counted | **10** of 100 | **100** of 100 |
| records archived | 0 | 1 |
| committed offset | 10, with 91 records never read | 101, nothing left |
| consumer restarts | **124–125** in 30 s | 0 |
| outcome | never drained in 30 s | drained in **207–322 ms** |

Two runs, 2026-09-25, in [`results/`](results/).

An earlier pair of runs is not in there. A fresh-context review pointed out that the
backoff between restarts ignored the budget, and the log proved it: the run gave up after
30.162 s rather than 30.003 s, with the overshoot sitting in a `time.Sleep` that no
cancellation could reach. Changing that changes the instrument, so those numbers went with
it rather than being kept alongside numbers measured by a different tool.

## What it shows

The cost of a poison pill is not the record. It is everything queued behind it: ninety-one
payments that were produced, acknowledged, and never read, on a partition that looks healthy
from the broker's side — the records are there, the ISR is complete, nothing is lost. What
is broken is the consumer, and it is broken in the way that hides best: it comes back, it
reads, it dies, and the lag graph is a flat line rather than a spike.

Cell B is the same failure with somewhere to put it. The record leaves with its bytes
unchanged and the reason attached, the offset moves, and the ninety records behind it are
handled within a third of a second. The classification is the whole trick: `ErrUnprocessable`
separates "this will never work" from "try again later", and only the first one is allowed
to leave the partition. A retryable failure — a database that is down — still holds the
offset, because giving up on those is how payments go missing quietly.
