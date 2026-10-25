# exp-12 — schema evolution

Changes to the payment event, asked twice: once of the registry, which decides what
may be published, and once of the wire, which decides what actually happens to a consumer
built against the schema before them.

```
make exp-12
```

## What the registry lets through

Each cell is a subject of its own, seeded with the schema the service really publishes
under and then set to the level under test — a subject that already holds another version
would compare the next change against that instead of against the baseline.

| change | NONE | BACKWARD | FULL |
|---|---|---|---|
| add an optional field | accepted | accepted | accepted |
| remove a field | accepted | **refused** | accepted |
| retype a field, keeping its number | accepted | **refused** | **refused** |
| append a message after the last | accepted | accepted | accepted |
| insert a message before the first | accepted | **accepted** | **accepted** |

Two things worth stopping at. **`NONE` is the default** — the third run asks before setting
anything, and a subject just created reports `NONE`, as does the registry-wide default;
under `NONE` it accepted every one of these changes, so "we have a schema registry" on its own buys nothing; the setting is
the protection, not the server. And **`FULL` is not `BACKWARD` plus more**: Apicurio 3.0.9
lets a field removal through under `FULL` while refusing it under `BACKWARD`. Whoever picks
the level by the strongest-sounding name gets the weaker rule for that change.

The last two rows are how the service's own schema grows: captures and refunds were appended
to the file the authorization lives in, and the registry accepts that at every level — the
change the service makes is let through. So is the one it must never make. A record names its
message by position in the file, so a message inserted before the first shifts every index:
records already on the topic at index 0 would be read as the new message by anyone using the
new file. No level refuses it, because to a protobuf compatibility check a new message type
is an addition. What stops it here is a rule and a check, not the registry: messages are
appended, never inserted (the comment above `PaymentCaptured` in `proto/payment/v1/events.proto`), and
the consumer refuses a record whose `event_type` header names a different message than its
index (`internal/infrastructure/kafka/decode.go`).

## What the bytes do anyway

The same three changes written as records, read by a consumer built against the baseline:

| record | what the v1 consumer did with it |
|---|---|
| v1 | counted |
| a field added at the end | **counted** — the unknown field is skipped |
| the amount removed | dead letter: *an amount to count is positive* |
| the amount retyped to a string, keeping its number | dead letter: *an amount to count is positive* |

Every record carries `occurred_at`, whatever its generation: the consumer now refuses an event
too old for its inbox to remember, and a record without the field reads as the zero time, so
without it the v1 control itself would be dead-lettered and the table would say nothing about
schemas.

Note what did **not** happen: neither destructive change produced a decoding error.
Protobuf skipped what it could not match and handed the consumer a message whose
`amount_minor` was zero — read off the refusal, which is the domain rule for a
non-positive amount; the run does not print the decoded value itself. The reason both records ended up in the dead letter topic is not
the parser — it is the domain rule that an authorised payment is for a positive amount.
Without that rule the merchant's total would have taken two payments of zero, and every
part of the system would have reported success.

Four runs in [`results/`](results/). The current one
([log](results/run-2026-10-25-171308.log)) is the only one made with the consumer as it is now
— the inbox horizon, three messages in the baseline — and the only one with the two message
rows; every table above is quoted from it. The three from 2026-09-25 and 2026-09-26 were made
against a one-message baseline and a consumer without the horizon, and wrote no `occurred_at`;
their wire half agreed with today's, under the old refusal text *an authorised amount is
positive*. The first two set `NONE` explicitly and did not record what an unconfigured subject
reports; the third asks first, as the current one does. Those three read the merchant's total
from `merchant_totals`, the single-currency table that per-currency totals have since
replaced; every payment in them is EUR.

## What it shows

The registry is a policy engine that is off by default, and the wire format will not tell
you when a field disappears. The thing that turned a silent zero into a visible refusal is
a line in the domain, written long before this experiment: *an authorised payment is for a
positive amount*. Validation at the edge of the consumer is not paperwork; on this evidence
it is the only layer that noticed.

## Why the run ends by cancelling

The consumer reports a context deadline as a failure and a cancellation as an orderly stop,
and that asymmetry is deliberate: the deadlines it can see are its own — the offset commit
and the handling of a record each have one — so treating them as clean exits would turn a
failed commit into a silent one. The budget here therefore cancels the consumer instead of
giving it a deadline.

## Two mistakes this experiment made first

The first compatibility matrix was contaminated: `BACKWARD` was set *after* an incompatible
version had already been registered under the same subject, so every later candidate was
compared against that instead of the baseline, and the matrix said adding a field was
incompatible. Every cell now gets a fresh subject and reads its level back before asking.

The first wire run put the new field at number 5 — which is `occurred_at` in the schema the
consumer knows. That measured a reused number, not an added field, and reported a decoding
error for what should be the safest change there is. Both halves now derive from the
service's own schema text, so the numbering cannot drift apart again.
