# transaction_guarantee — KR2, delivery guarantees and tuning

At-most-once, at-least-once and effectively-once, each shown as a pair of numbers rather
than a definition: a producer sends exactly 1 000 payments, a consumer writes them to
Postgres, the consumer is killed mid-stream, and the run prints `produced / rows /
distinct / duplicates`. Then the rest of KR2: the commit strategies including autocommit,
`acks` and `min.insync.replicas`, idempotence, transactions, rebalancing strategies,
consumer lag and backpressure, and batching and compression — with and without a load
ceiling.

| Run | What it shows | Expected |
|---|---|---|
| [exp-05](exp-05-at-most-once/) | offset committed **before** handling, then `kill -9` | **951 rows — 49 payments gone** |
| [exp-05b](exp-05b-autocommit-greedy/) | franz-go's `GreedyAutoCommit`, killed right after an autocommit lands mid-batch | **the unwritten rest of the batch lost: 38 and 41** |
| [exp-06](exp-06-at-least-once/) | offset committed **after** handling, same kill | **1 050 rows — 50 charged twice** |
| [exp-06b](exp-06b-autocommit-default/) | franz-go's default autocommit, killed at the same moment | **nothing lost, 12 written twice — the default is at-least-once** |
| [exp-07](exp-07-inbox/) | the same failure with an inbox | **1 000 rows, zero duplicates — the inbox refused the 50 redelivered** |
| [exp-08](exp-08-acks/) | `acks=1` with the followers paused under a leader kill, against `acks=all` with `min.insync.replicas=3` | **`acks=all` refused all 2 000 with `NOT_ENOUGH_REPLICAS`; `acks=1` with the followers paused lost all 2 000 it had acknowledged** |
| [exp-09](exp-09-reordering/) | idempotence off, five requests in flight, repeated refusals | **0 reordered in three runs; 291, 80 and 80 duplicated, and in the run that counted retries every duplicate was a record appended before a `REQUEST_TIMED_OUT` — a timeout duplicate, not a franz-go one; idempotent: zero of both** |
| [exp-10a](exp-10a-rollback/) | a crash mid-transaction | **`read_committed` 1 000 of 1 000 — the killed batch's output aborted, its offsets never committed** |
| [exp-10b](exp-10b-database-boundary/) | the same, with a Postgres write in the loop | **Kafka 1 000, Postgres 1 050 — the transaction stops at Kafka** |
| [exp-10c](exp-10c-read-uncommitted/) | the same, read `read_uncommitted` | **1 050 — the aborted batch is still in the log** |
| [exp-10d](exp-10d-hanging-transaction/) | a transaction left open | **unrelated records invisible to `read_committed` for 23.2 s behind a 20 s timeout** |
| [exp-14](exp-14-rebalance-strategies/) | a second consumer joins: eager, cooperative-sticky and KIP-848 | **eager revoked all six partitions for 39–75 ms; cooperative stopped only the three that moved, for 0.52–0.68 s; KIP-848 for 4.9–6.5 s, around its heartbeat; nothing lost or handled twice** |
| [exp-14b](exp-14b-membership/) | the same member leaving, with and without `group.instance.id` | **dynamic: back in 0.6 s, and a restart costs a second rebalance; static: a restart costs nothing but a death costs the 12 s session timeout** |
| [exp-11](exp-11-poison-pill/) | one record the consumer cannot decode, with and without a dead letter route | **without: 10 of 100 payments counted, the offset stuck, 91 records never read, the consumer restarting 124–125 times in 30 s; with: all 100, archived in 207–322 ms** |
| [exp-12](exp-12-schema-evolution/) | three schema changes against three compatibility levels, then the bytes on the wire | **a subject nobody configured accepts every one of them; `FULL` accepts a deletion `BACKWARD` refuses on Apicurio 3.0.9; a removed or retyped amount reaches the consumer as zero with no decoding error** |
| [exp-13](exp-13-broker-outage/) | brokers killed under the running service at 10 payments/s | **not one payment refused; one broker down cost 88–91 records of backlog, two cost 469–523 with an 18–24 s catch-up — and the cluster reported 0 under-replicated throughout, having lost the quorum that would update it** |
| [exp-16](exp-16-lag-backpressure/) | a 20 s dependency outage with a join in the middle: retry inline against pause-and-rewind | **the same lag either way; retrying inline got the member removed, its stale commit rewound the group 1 726–1 732 records in every run and 1 739 were handled twice in one of three; pausing: no removal, no rewind, no duplicates** |
| [exp-17](exp-17-batching-sweep/) | linger × batch size × codec under fixed load | **linger decides batching, batching decides compression: zstd 2.9× → 5.65× on the same bytes; Java's default is 5 ms, not 0** |
| [exp-17b](exp-17b-saturation/) | the same producer flat out | **without compression the producer stalled at 45–89 MB/s on the wire; zstd delivered 2.1–4.7× the records for 34–67% more CPU per megabyte** |

exp-08, exp-09 and exp-10a…c read their logs back through [`../labkit/`](../labkit/), which
refuses a short read, a partition mid-election, and an offset listing that did not answer;
exp-10d and exp-11 read their offsets through the same guard,
exp-05…07 and their autocommit siblings count what landed in Postgres, exp-14 and exp-16
record every record their consumers handle, and exp-17 reads the producer's own per-batch
metrics. Each is a separate experiment with its own topic, runs and logs, because each is a
claim in its own right. exp-05…07, 05b and 06b share [`stand/`](stand/) and exp-10a…c share
[`eos/`](eos/), the harnesses they differ from each other in nothing but: for the stand, when
the offset is committed; for exp-10a…c, whether the loop writes to Postgres and which claim
the verdict holds the run to. exp-17b is exp-17's program run with `-saturate`.

**Not measurable on this stand:** `acks=all` at `min.insync.replicas=1` losing like
`acks=1` needs the in-sync set shrunk to one, which on three combined broker and controller
nodes takes the KRaft quorum down with it — see [exp-08](exp-08-acks/).
