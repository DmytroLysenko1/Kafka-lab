# transaction_guarantee — KR2, delivery guarantees

At-most-once, at-least-once and effectively-once, each shown as a pair of numbers rather
than a definition: a producer sends exactly 1 000 payments, a consumer writes them to
Postgres, the consumer is killed mid-stream, and the run prints `produced / rows /
distinct / duplicates`.

| Run | What it shows | Expected |
|---|---|---|
| exp-05 | offset committed **before** handling, then `kill -9` | `rows < 1 000` — payments gone |
| exp-06 | offset committed **after** handling, same kill | `rows > 1 000` — double charges |
| exp-07 | the same failure with an inbox | `rows = 1 000`, zero duplicates |
| exp-08 | `acks=1` under a leader kill against `acks=all` with `min.insync.replicas=3` | loss against a refusal |
| exp-09 | idempotence off, five requests in flight, a network failure | `Refunded` overtakes `Captured` |
| exp-10 | Kafka transactions, read-process-write | holds inside Kafka, not across a Postgres write |
| exp-17 | linger × batch size × codec under fixed load | throughput against p99 produce latency |

Nothing here is measured yet — this file is the shape the group will take, and each row
becomes a link with numbers as its run lands.
