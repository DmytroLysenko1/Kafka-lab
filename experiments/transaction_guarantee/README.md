# transaction_guarantee — KR2, delivery guarantees

At-most-once, at-least-once and effectively-once, each shown as a pair of numbers rather
than a definition: a producer sends exactly 1 000 payments, a consumer writes them to
Postgres, the consumer is killed mid-stream, and the run prints `produced / rows /
distinct / duplicates`.

| Run | What it shows | Expected |
|---|---|---|
| [exp-05](exp-05-at-most-once/) | offset committed **before** handling, then `kill -9` | **951 rows — 49 payments gone** |
| [exp-06](exp-06-at-least-once/) | offset committed **after** handling, same kill | **1 050 rows — 50 charged twice** |
| [exp-07](exp-07-inbox/) | the same failure with an inbox | **1 000 rows, zero duplicates** |
| [exp-08](exp-08-acks/) | `acks=1` under a leader kill against `acks=all` with `min.insync.replicas=3` | **`acks=all` refused all 2 000 with `NOT_ENOUGH_REPLICAS`; `acks=1` lost nothing — the window is narrow, not absent** |
| [exp-09](exp-09-reordering/) | idempotence off, five requests in flight, repeated refusals | **0 reordered, 291 duplicated — franz-go turns reordering into duplication; idempotent: zero of both** |
| exp-10 | Kafka transactions, read-process-write | holds inside Kafka, not across a Postgres write |
| exp-17 | linger × batch size × codec under fixed load | throughput against p99 produce latency |

All of them read their logs back through [`../labkit/`](../labkit/), which refuses a short
read and a partition mid-election. The first three are separate experiments with separate topics, runs and logs, because each
is a claim in its own right. They share [`stand/`](stand/), the harness they differ from
each other in nothing but: one ordering decision, three programs of ten lines each. The
rest are not measured yet — each row becomes a link with numbers as its run lands.
