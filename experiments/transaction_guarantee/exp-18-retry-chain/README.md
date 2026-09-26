# exp-18 — one merchant's row held by another transaction, with and without the retry chain

**Hypothesis.** A payment that cannot be counted because another transaction holds its
merchant's row is a failure of that payment alone. Treated like any other database
failure, it stops every payment behind it for as long as the lock is held. Moved aside into
the retry chain, it stops them for no longer than the lock wait, however long the lock is
held — and it is still counted exactly once, including when the lock outlasts the whole
chain and the payment has to be replayed from the dead letter topic.

```
make exp-18
```

## What it runs

600 payments over six partitions, every hundredth one — six in all — for a merchant whose
`merchant_totals` row a second transaction holds `FOR UPDATE`. Every payment is one minor
unit, so each merchant's total must equal its number of inbox rows exactly. The consumer,
the detours, the replay and the 2 s `lock_timeout` are the service's own
([`consumer.go`](../../../internal/infrastructure/kafka/consumer.go),
[`totals.go`](../../../internal/infrastructure/postgres/totals.go)); the tiers are the
catalog's 5 s / 1 min / 10 min scaled to 3 s / 6 s / 12 s so a cell takes a minute.

| Cell | Chain | Lock | What differs |
|---|---|---|---|
| A | none | released after 30 s | the handler is wrapped so contention reads as any other database failure — what the service did before the chain: hold the offset, die, restart |
| B | main → 3 s → 6 s → 12 s → DLQ | released after 30 s | nothing: the service as it runs |
| C | the same | held until all six payments are archived as `exhausted`, then released and replayed | the operator's side: find the letters, let go, `kafka.Replay` |

Each cell has its own topics, groups, merchants and range of event ids, and `run.sh`
resets all of them first. The timings are read from `inbox.consumed_at`, not from the
harness.

## Result — 2026-09-25, rerun 2026-09-26

Three runs on the final code: [1](results/run-2026-09-25-210354.log) ·
[2](results/run-2026-09-25-210657.log) · [3](results/run-2026-09-26-161858.log), the third
also measuring order and waits (below). Six earlier runs, on the code before the review
fixes below, are not kept; they agree with these except for the replay latency in cell C,
which is what one of those fixes changed.

| | A — no chain | B — chain, lock 30 s | C — chain, lock past every tier |
|---|---|---|---|
| healthy payments counted while the merchant was still locked | 268–305 of 594 | **594 of 594** | **594 of 594** |
| the last healthy payment counted at | +30.5 s, just after the lock | **+17.0–17.1 s** | +16.4–18.7 s |
| the six contended payments counted at | +30.3–30.5 s | +38.5–38.9 s | 3.0–3.5 s after the replay |
| consumer restarts | **13** | 0 | 0 |
| copies into the 3 s / 6 s / 12 s tiers, and the DLQ | — | 6 / 6 / 3 / 0 | 12 / 6 / 6 / 6 (six of the twelve are the replay) |
| inbox rows = the merchants' totals | 600 = 600 | 600 = 600 | 600 = 600 |

**Without the chain the lock is the outage.** Every healthy payment behind a contended one
waited the full 30 s, because a consumer that cannot tell contention from a database that
is down does the safe thing for both: holds the offset and dies. It died 13 times in 30 s,
each restart meeting the same locked row after 2 s of waiting. About half of the healthy
payments got through before that, only because they came earlier in a batch than the first
contended one.

**With the chain the lock stops costing time the moment it outlasts the lock wait.** All
594 healthy payments were counted while the merchant was still locked. What they paid was
not the hold but the waiting: each contended payment still spends its lock wait inside the
batch before it is moved, and the partition behind it waits with it.

**That wait is not always 2 s, and the reason is a Postgres rule, not ours.** The gaps
between healthy payments in cell B of run 2, read from `inbox.consumed_at`, were 2.0 s four
times and 3.9–4.0 s twice — eight lock waits for six payments, 16 s of the 17. `lock_timeout` applies to each lock acquisition,
not to the statement: when a tier's transaction is already queued on the row, the main
stage first waits up to 2 s for the tuple lock behind it, then up to 2 s more for the row
itself. The bound is still finite — one wait per transaction queued ahead, and each stage
has at most one — but "2 s per contended payment" is the floor, not the ceiling.

**The chain buys that time with the partition's order.** The third run reads each
payment's partition off the topic and counts, within a partition — the only order Kafka
keeps — the payments counted ahead of an earlier contended one:

| | A — no chain | B | C |
|---|---|---|---|
| payments counted ahead of an earlier contended payment on their partition | **0** | **280** | **316** |
| waits between healthy payments of 300 ms or more | 4, together **29.5 s** | 6, together **15.8 s** | 6, together **15.8 s** |

Without the chain the partition keeps its order and pays for it in time: nothing behind a
contended payment is counted until the lock goes. With it, hundreds of later payments on the
same partition are counted first — which is the point, and also the price. A consumer
downstream that assumes one partition's events arrive in the order they were produced is
wrong for every merchant that ever meets a lock. Here that is harmless, because the inbox
counts authorisations and their order does not matter; for events of one payment whose
order does (authorised, then captured), the chain needs keying by payment and a rule that
parks the later event behind the earlier one, which this run does not have. Among the
contended payments themselves no pair on one partition was counted out of order, but there
were only one to four such pairs per cell — too few to call their order kept.

The metric's first version compared payments across partitions and counted 954 overtakes
in cell A, where nothing had overtaken anything: Kafka never ordered them in the first
place. That run's log is not kept.

**Escalation happened as designed.** In B every contended payment failed on the main topic
and in the 3 s and 6 s tiers, where the lock was still held; three of the six reached the
12 s tier, where the lock had gone by the time they were due, and none reached the DLQ. In
C the lock outlived the chain: all six ended in the DLQ as `exhausted`, the operator
released the row and replayed them in 50 ms, and they were counted 3.0–3.5 s later — the
first tier's delay. Before the tiers fetched with a short wait, that was 5–8 s: a partition
resumed after its delay joined only the next fetch, and franz-go's default fetch wait is
5 s ([case 07](../../../docs/static/07-retry-dlq.md#why-blocking-is-correct-in-the-retry-consumer)).

**Every counted payment was counted once, in every cell.** 600 inbox rows against totals
summing to 600, including C, where each contended payment existed twice in the first tier
(once from the main topic, once from the replay).

## What it does not show

- **One hot row, not a hot table.** A lock on every merchant — a migration, a bulk update —
  is contention on every payment, and the chain turns it into a dead letter topic full of
  `exhausted` letters eleven minutes in. That is the outage case, and it belongs to pausing
  (exp-16), not to the chain.
- **The batch budget.** It exists so that a partition full of one locked merchant cannot
  hold the rebalance past its timeout; six contended payments never come near it. It is
  tested in `make test-integration` (fourteen payments, the first commit before the last
  copy), not measured here.
- **Real tier delays.** 3 s / 6 s / 12 s stand in for 5 s / 1 min / 10 min. The mechanics
  are the same; the time a payment spends in the chain in production is minutes, not
  seconds.
