# transaction_guarantee — KR2, delivery guarantees

At-most-once, at-least-once and effectively-once, each shown as a pair of numbers rather
than a definition: a producer sends exactly 1 000 payments, a consumer writes them to
Postgres, the consumer is killed mid-stream, and the run prints `produced / rows /
distinct / duplicates`.

| Run | What it shows | Expected |
|---|---|---|
| [exp-05](exp-05-delivery-semantics/) | offset committed **before** handling, then `kill -9` | **951 rows — 49 payments gone** |
| [exp-06](exp-05-delivery-semantics/) | offset committed **after** handling, same kill | **1 050 rows — 50 charged twice** |
| [exp-07](exp-05-delivery-semantics/) | the same failure with an inbox | **1 000 rows, zero duplicates** |
| exp-08 | `acks=1` under a leader kill against `acks=all` with `min.insync.replicas=3` | loss against a refusal |
| exp-09 | idempotence off, five requests in flight, a network failure | `Refunded` overtakes `Captured` |
| exp-10 | Kafka transactions, read-process-write | holds inside Kafka, not across a Postgres write |
| exp-17 | linger × batch size × codec under fixed load | throughput against p99 produce latency |

The first three share one stand and one program, because they differ in nothing but the
order of two statements: `make exp-05` runs all three and prints the table above. The rest
are not measured yet — each row becomes a link with numbers as its run lands.
