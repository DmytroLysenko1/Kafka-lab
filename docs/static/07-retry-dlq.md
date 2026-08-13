# 07 — Retry chain and dead letters

**The question:** a record fails to process — where does it wait, and who is blocked
while it waits?

KR3 · interactive version: [`docs/dynamic/retry-dlq/`](../dynamic/retry-dlq/)

```mermaid
flowchart LR
    M["payments.main<br/>6 partitions"] --> H{"handler outcome"}

    H -->|"ok"| OK["commit offset"]
    H -->|"transient<br/>provider timeout"| R5["payments.retry.5s"]
    H -->|"poison<br/>will not deserialize"| D["payments.dlq"]

    R5 --> RC["retry-consumer<br/>pause partition until ts plus delay"]
    RC -->|"fails again, attempt 2"| R1["payments.retry.1m"]
    R1 --> RC
    RC -->|"fails again, attempt 3"| R10["payments.retry.10m"]
    R10 --> RC
    RC -->|"attempts exhausted"| D

    D --> RP["dlq-replayer"]
    RP -->|"after the bug is fixed"| M
```

*Fig. 7 — the backoff schedule is visible in the topic list instead of being buried in a
sleep, and the main partition never waits for a failing record (exp-11).*

## The wrong fix, and why it is tempting

Sleeping inside the main consumer looks like the smallest possible change and blocks the
entire partition. Every other payment that happens to hash to that partition waits
behind the one that is failing — head-of-line blocking, in which a single failing
merchant destroys throughput for everyone sharing its partition.

Worse, it is silent: throughput drops, lag rises on one partition only, and the handler
logs look healthy because the record eventually succeeds.

## Why blocking is correct in the retry consumer

Kafka has **no delayed delivery**. The delay is implemented by the retry consumer
reading the record timestamp and pausing the partition until the delay has elapsed.
That is safe here and forbidden in the main consumer for one specific reason: inside
`payments.retry.5s` every record carries the same delay and they arrive in timestamp
order, so waiting for the head record wastes nothing. Mixing delays into one shared
retry topic reintroduces exactly the head-of-line blocking the pattern removes.

Pausing is not the same as sleeping in the handler: the client keeps polling and
heartbeating, so the group does not consider the member dead.

**Watch the timestamp type.** With `message.timestamp.type=LogAppendTime` on the topic,
the broker overwrites the producer timestamp and every delay calculation is wrong. The
retry topics must stay on `CreateTime`, or the delay has to travel in a header.

## The offset on the main topic advances immediately

Producing to the retry topic and then committing the main offset is itself a small dual
write. The order drawn above is the safe one: produce first, commit after the ack, so a
crash in between redelivers the record and it lands in the retry topic twice. That
duplicate is harmless only because the consumer has an inbox
([05](05-delivery-semantics.md)) — without one, this pattern manufactures duplicates by
design.

## What a dead letter must carry

Headers, not a log line: original topic, partition and offset, the error class, the
attempt count, the timestamp of the first failure, and the `trace_id`. That is enough to
reconstruct what happened without digging through consumer logs that have since rotated.

A DLQ you cannot replay from is a rubbish bin. `dlq-replayer` exists so that fixing the
bug and reprocessing is one command, and so that the person doing it at 3 a.m. is not
writing an ad-hoc export script.

## Poison pills skip the queue

Bytes that will not deserialize are not a transient failure. Retrying them a hundred
times changes nothing, and retrying them forever stops the partition permanently — the
consumer never advances past the offset, lag grows without bound, and the topic looks
broken rather than the message. They go straight to the DLQ, the offset is committed,
and a human looks at the bytes later.

Deserialization failure is a support problem. Business-logic failure is a retry problem.
Classifying the error at the boundary is what makes the difference visible in code.

## Retries reorder events by design

A record sent to `payments.retry.10m` comes back long after its neighbours. For payments
that means a handler must never assume arrival order: the state transition is guarded in
the database, `UPDATE ... WHERE status = $expected`, so a stale event finds the row in
the wrong state and changes nothing. Ordering is not restored by the retry chain and
must not be assumed by anything downstream of it.

## To be measured

| Run | What it shows | Status |
|---|---|---|
| exp-11 | poison pill: straight to DLQ and the partition keeps flowing, versus infinite retry and a permanently stuck partition | TBD |
| exp-16 | blocking sleep in the main consumer: lag on the affected partition versus the rest | TBD |
