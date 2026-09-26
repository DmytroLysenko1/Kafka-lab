# exp-12 — schema evolution

Three changes to the payment event, asked twice: once of the registry, which decides what
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

Two things worth stopping at. **`NONE` is the default** — the third run asks before setting
anything, and a subject just created reports `NONE`, as does the registry-wide default;
under `NONE` it accepted every one of these changes, so "we have a schema registry" on its own buys nothing; the setting is
the protection, not the server. And **`FULL` is not `BACKWARD` plus more**: Apicurio 3.0.9
lets a field removal through under `FULL` while refusing it under `BACKWARD`. Whoever picks
the level by the strongest-sounding name gets the weaker rule for that change.

## What the bytes do anyway

The same three changes written as records, read by a consumer built against the baseline:

| record | what the v1 consumer did with it |
|---|---|
| v1 | counted |
| a field added at the end | **counted** — the unknown field is skipped |
| the amount removed | dead letter: *an authorised amount is positive* |
| the amount retyped to a string, keeping its number | dead letter: *an authorised amount is positive* |

Note what did **not** happen: neither destructive change produced a decoding error.
Protobuf skipped what it could not match and handed the consumer a message whose
`amount_minor` was zero — read off the refusal, which is the domain rule for a
non-positive amount; the run does not print the decoded value itself. The reason both records ended up in the dead letter topic is not
the parser — it is the domain rule that an authorised payment is for a positive amount.
Without that rule the merchant's total would have taken two payments of zero, and every
part of the system would have reported success.

Three runs, 2026-09-25 and 2026-09-26, in [`results/`](results/). The first two set `NONE`
explicitly and did not record what an unconfigured subject reports; the third
([log](results/run-2026-09-26-025512.log)) asks first, and that is what the default is
quoted from.

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
