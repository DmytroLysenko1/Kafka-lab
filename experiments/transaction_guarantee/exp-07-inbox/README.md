# exp-07 — the inbox

**Hypothesis.** The duplicates of [exp-06](../exp-06-at-least-once/) are not removed by
delivering better. They are absorbed by writing idempotently: the consumer claims the
payment id and writes the row in one transaction, so a redelivery loses the race for the
primary key and changes nothing.

```
make exp-07
```

The same stand, the same crash, the same replay. The only difference is that the write
goes through `INSERT INTO delivery_inbox … ON CONFLICT DO NOTHING` and the business row in
a single transaction.

| Knob | Default | Why |
|---|---|---|
| `PAYMENTS` | 1 000 | enough that a replayed batch would be visible if it survived |
| `DIE_AFTER` | 500 | halfway, so the crash has a before and an after |

## Result — 2026-09-21

[run log](results/run-2026-09-21-180500.log) · [run with the refusal count](results/run-2026-09-21-234218.log)

| Rows | Distinct | Redelivered, refused by the inbox | Outcome |
|---|---|---|---|
| 1 000 | 1 000 | **50** | **exactly once** |

**This run was delivered the duplicates too.** It received 50 records twice, exactly as
exp-06 did — the broker has no idea anything is different. It holds 1 000 rows because the
second delivery found the claim already taken and wrote nothing; each such refusal is
written to `delivery_refused` in the same transaction, and the verdict counts them. A run
with a clean table and no refusals fails: it would look identical and prove nothing, which
is exactly what the first two runs could not rule out, since they only counted rows. *Exactly-once effect,
at-least-once delivery*, and the distinction is the single most useful sentence in this
whole group.

**Claiming first and writing second would be the same bug with more steps** — argued, not
measured: no run kills the process between the two statements. If the inbox
row were inserted in its own statement, a crash between the claim and the write would leave
a payment permanently unwritable: the claim exists, so every retry is refused. That is why
`recordOnce` does both inside one transaction and nothing else — and it is the same shape
as the outbox on the way out ([case 06](../../../docs/static/06-transactional-outbox.md)).

All three share [`../stand/`](../stand/). The reasoning is in the
[journal](../../../docs/00-journal.md).

## Cleaning up

```
make reset-topic TOPIC=exp07.payments
```
