# exp-06b — franz-go's default autocommit is at-least-once

**Hypothesis.** The default autocommit does not lose records: it commits what the
*previous* poll returned, so a batch is committed only once the next poll proves the
handler finished it. A crash replays the batch in hand instead of losing it.

```
make exp-06b
```

The stand of [exp-06](../exp-06-at-least-once/), with the manual commit replaced by
franz-go's default autocommit, timed exactly as its greedy sibling
[exp-05b](../exp-05b-autocommit-greedy/): 100 ms interval, 5 ms a record, and the kill at the
first record past 500 handled after an autocommit has landed inside the batch.

| Knob | Default | Why |
|---|---|---|
| `PAYMENTS` | 1 000 | as exp-06 |
| `DIE_AFTER` | 500 | the kill waits for the first autocommit past it |

## Result — 2026-09-22

[run 1](results/run-2026-09-22-002154.log) · [run 2](results/run-2026-09-22-002402.log)

| Run | Killed at | Rows | Distinct | Lost | Duplicated |
|---|---|---|---|---|---|
| 1 | 512 handled | 1 012 | 1 000 | **0** | **12** |
| 2 | 512 handled | 1 012 | 1 000 | **0** | **12** |

**Same moment, opposite outcome.** Killed on the 12th record of a batch, the default had
committed only the previous batch; the replacement fetched this one again and wrote its
first 12 records a second time. Greedy, killed at the same kind of moment, lost the rest of
the batch instead. That is what `consumer_group.go` in franz-go says — the one-poll lag "is
what makes default autocommit at-least-once" — measured rather than quoted.

It holds only while the handler finishes a batch before it polls again. A handler that
hands records off and polls on moves the commit ahead of the work, and the default flavour
then loses like greedy; that case is argued, not run here.

## Cleaning up

```
make reset-topic TOPIC=exp06b.payments
```
