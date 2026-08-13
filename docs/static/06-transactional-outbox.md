# 06 — Transactional outbox: where atomicity ends

**The question:** the state change and the event must never disagree — but they live in
two different systems, so where exactly does the atomic part stop?

KR3, KR5 · interactive version: [`docs/dynamic/outbox/`](../dynamic/outbox/)

```mermaid
sequenceDiagram
    autonumber
    participant API as payments-api
    participant DB as Postgres
    participant R as outbox-relay
    participant K as Kafka

    rect rgba(67, 160, 71, 0.16)
        Note over API,DB: one transaction - the only atomic part of this diagram
        API->>DB: BEGIN
        API->>DB: INSERT payment
        API->>DB: INSERT outbox row, published_at is null
        API->>DB: COMMIT
    end

    Note over DB: the event is a row, not a message<br/>the API has already answered its caller

    R->>DB: SELECT unpublished FOR UPDATE SKIP LOCKED
    R->>K: Produce event with key = payment_id
    K-->>R: ack

    rect rgba(245, 158, 11, 0.18)
        Note over R,DB: duplicate window - crash here and the event is published<br/>but still marked unpublished, so the next pass sends it again.<br/>This is the deliberate half of the trade: of the two orderings<br/>only this one cannot lose an event, and the consumer's inbox absorbs the copy
    end

    R->>DB: UPDATE outbox SET published_at = now()

    Note over K: broker unavailable means a growing table,<br/>not a failed payment and not a lost event
```

*Fig. 6 — atomicity covers the payment and its event because both are rows in the same
transaction; everything to the right of the commit is retryable delivery, which is why
an unreachable broker degrades latency instead of correctness (exp-10).*

## Why the obvious alternative is a bug

Calling the broker from the use case is a **dual write**: two systems, no shared
transaction, and four possible outcomes instead of two.

```mermaid
sequenceDiagram
    autonumber
    participant UC as use case
    participant DB as Postgres
    participant K as Kafka

    alt commit first, then publish
        UC->>DB: INSERT payment, COMMIT
        rect rgba(229, 57, 53, 0.16)
            Note over UC,K: kill -9, a broker timeout or a pod eviction here:<br/>the payment exists and no event is ever produced for it
        end
        UC->>K: Publish PaymentCaptured
    else publish first, then commit
        UC->>K: Publish PaymentCaptured
        rect rgba(229, 57, 53, 0.16)
            Note over UC,K: the same failure one arrow earlier: the event is on<br/>the topic and announces a payment that does not exist
        end
        UC->>DB: INSERT payment, COMMIT
    end
```

*Fig. 6b — both orderings are drawn because that is the whole argument: each one has a
window, the windows sit in different places, and neither can be closed from inside the
handler. A process that dies does not retry.*

Retrying is not the missing piece and neither is ordering. The only fix is to stop having
two writes: the event becomes a row, and Fig. 6 is what that looks like.

## What the relay must get right

| Rule | Why | What happens otherwise |
|---|---|---|
| mark `published_at` **only after** the broker ack | the ack is the first moment delivery is real | mark first and a crash loses the event silently — no error, no row to find |
| duplicates after the ack are acceptable | crash between ack and update republishes | harmless, because the consumer has an inbox ([05](05-delivery-semantics.md)) |
| the produce must not hold the row lock for long | `FOR UPDATE` keeps a transaction and a connection open across a network call | a slow broker turns into pool exhaustion in the API; keep batches small, put a hard timeout on the produce, or claim rows with `claimed_by`/`claimed_at` instead of holding the transaction |
| one relay, or partitioned work | two relays publishing events of the same aggregate race each other | `SKIP LOCKED` prevents fighting over rows, not out-of-order publication; fetch batches grouped by `payment_id`, or run a single relay with a lease |
| key every event by aggregate id | ordering in Kafka is per partition | `key=nil` puts `Refunded` and `Captured` on different partitions and the consumer sees them in the wrong order ([01](01-write-path.md)) |

## What it costs, honestly

An extra table, a process to operate and monitor, and events that arrive **after** the
state change rather than with it. The lag of the relay becomes a metric someone has to
watch, and a stuck relay is now a way to lose the *timeliness* of events while keeping
their correctness.

Do not reach for this pattern where a dropped notification is acceptable. Reach for it
where the event is money, an entitlement, or anything another service will treat as
fact. For this lab that is the whole domain, which is why payments were chosen.

## CDC is the same pattern with a different reader

Debezium and friends read the database WAL instead of polling a table. The atomic part
of the diagram is unchanged — it is still one transaction in Postgres — and the relay is
replaced by log-based capture. The trade is: no polling lag and no relay to write,
against an extra piece of infrastructure and a coupling to the physical schema. Worth
naming in the patterns doc; not worth introducing in a lab that has to demonstrate the
mechanism.

## To be measured

| Run | What it shows | Status |
|---|---|---|
| exp-10 | Kafka EOS covers Kafka only; the same run with a Postgres write needs outbox plus inbox | TBD |
| exp-13 | broker killed under load: outbox backlog grows, API keeps answering, relay drains afterwards | TBD |
