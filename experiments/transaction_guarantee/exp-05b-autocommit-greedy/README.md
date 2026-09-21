# exp-05b — at-most-once, through franz-go's `GreedyAutoCommit`

**Hypothesis.** Autocommit loses records only in the greedy flavour: `GreedyAutoCommit`
commits what the last poll returned, handled or not, so a crash after such a commit and
before the batch is written loses the unwritten rest of it.

```
make exp-05b
```

The stand of [exp-05](../exp-05-at-most-once/), with the manual commit replaced by franz-go's
autocommit in its greedy flavour. The interval is 100 ms, the shortest franz-go accepts, and
each record takes 5 ms, so an autocommit lands inside every batch of 50. Past 500 handled,
the consumer dies at the first record where an autocommit has already covered the whole
batch it is writing and records of that batch are still unwritten — the one moment the two
flavours differ. [exp-06b](../exp-06b-autocommit-default/) dies at the same moment with the
default flavour.

| Knob | Default | Why |
|---|---|---|
| `PAYMENTS` | 1 000 | as exp-05 |
| `DIE_AFTER` | 500 | the kill waits for the first autocommit past it |

## Result — 2026-09-22

[run 1](results/run-2026-09-22-002055.log) · [run 2](results/run-2026-09-22-002302.log)

| Run | Killed at | Rows | Lost |
|---|---|---|---|
| 1 | 512 handled | 962 | **38** |
| 2 | 509 handled | 959 | **41** |

**The loss is exactly the unwritten rest of the batch.** Run 1 died on the 12th record of
batch 11 and lost the other 38; run 2 died on the 9th and lost 41. The greedy autocommit
had already stored the offset past the whole batch, so the replacement resumed after it.

This is the only way autocommit reaches at-most-once without the handler's help. The other
is a handler that hands records to another goroutine and polls on, which makes the default
flavour commit records nobody has handled; that is argued from the same mechanism and not
run here. Nothing in the cluster reports the loss, as in exp-05.

## Cleaning up

```
make reset-topic TOPIC=exp05b.payments
```
