# Experiments

One directory per experiment, grouped by the key result it answers. `make exp-NN` finds an
experiment by its number whichever group it sits in, so the grouping is for readers, not
something to remember when running anything.

| Group | KR | Experiments |
|---|---|---|
| [`kafka_internals/`](kafka_internals/) | KR1 — fundamentals and internals | exp-01 partition keys · exp-02 hot partition · exp-03 segments and retention · exp-04 ISR and leader election (plus exp-04c, the timer varied) · exp-15 partition reassignment |
| [`transaction_guarantee/`](transaction_guarantee/) | KR2 — delivery guarantees, and KR4 — operating under failure | exp-05…07 delivery semantics (plus exp-05b/06b, the autocommit flavours) · exp-08 acks · exp-09 reordering · exp-10a–d transactions · exp-11 poison pill · exp-12 schema evolution · exp-18 retry chain under a locked merchant · exp-13 broker outage · exp-14 rebalance strategies · exp-14b static membership · exp-16 lag and backpressure · exp-17 batching sweep · exp-17b saturation |

[`labkit/`](labkit/) is the instrument several experiments share: reading a topic to its end
offsets, waiting on the in-sync set instead of sleeping, and restarting a consumer the way an
orchestrator does (exp-11 and exp-18). It exists because four
experiments wrote the same reader separately, and two of those copies once reported a
short or mid-election read as a finding.

Each experiment directory holds the same five things: `README.md` with the hypothesis and
the result, the Go program that measures it, `run.sh` that drives the cluster around it,
`topics/` with the YAML for the topics it owns, and `results/` with the run logs every
published number is quoted from. Logs under `results/superseded/` are kept only because the
journal discusses what was wrong with them; no published number rests on those.

Three rules the instruments follow, each learned by getting it wrong first:

- **An experiment owns the log it reads.** Its `run.sh` resets its own topics before
  measuring; a run id alone is not enough, because it stops a rerun counting another run's
  records without stopping them landing in the partitions being measured.
- **A short read is an error, not an ending.** Silence from the broker means the run fails,
  never that the log ended — a plausible wrong number is worse than a failure.
- **Ask the cluster, do not trust the YAML.** Settings quoted in a report are read back from
  the broker, so the log says what was enforced rather than what was intended.
