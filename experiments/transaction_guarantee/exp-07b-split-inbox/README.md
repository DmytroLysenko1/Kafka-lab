# exp-07b — the inbox, split

**Hypothesis.** [exp-07](../exp-07-inbox/)'s inbox absorbs redeliveries because the claim and
the write commit together. Move one boundary — claim in a transaction of its own, write in a
second — and a crash between them leaves a payment claimed and never written; the redelivery
then finds the claim taken and refuses it. The duplicate the inbox exists to remove becomes a
loss instead.

```
make exp-07b
```

The same stand, the same 1 000 payments, the same kill after 500 handled, the same poll batch
of 50 and commit after the batch. The only difference is the store: `claim` commits the inbox
row on its own, and the kill lands after that commit and before the payment's write
(`experiments/transaction_guarantee/stand/consume.go`, `writeSplit`).

## Result — 2026-10-25, three runs

[17:52](results/run-2026-10-25-175212.log) · [17:53](results/run-2026-10-25-175319.log) ·
[17:54](results/run-2026-10-25-175414.log), identical:

| | exp-07, one transaction | exp-07b, two |
|---|---|---|
| rows | 1 000 | **999** |
| lost | 0 | **1** |
| duplicated | 0 | 0 |
| redeliveries refused by the inbox | 50 | 50 |

The 50 refused are the batch the crash left uncommitted: 49 payments that had been claimed and
written, and the one that had only been claimed. The inbox treated all 50 alike, because the
claim is all it can see. Nothing errors, nothing is duplicated, every total reconciles with
what was written — and one payment is recorded as handled and exists nowhere.

## What it shows

The inbox is one transaction or it is a different bug. A split inbox fails quieter than no
inbox: exp-06's duplicates at least show up in a reconciliation against the producer, while
this payment's only trace is a claim row nobody reads.
