# Failure report

Every failure below was produced on this stand and measured. Nothing here is a scenario
someone imagined: each row names the run that made it happen, what it looked like from
outside, and what it cost. Where a failure was *not* reproduced — because three brokers on
one laptop cannot stage it — the row says so instead of guessing.

The stand is three KRaft brokers, one Postgres, one schema registry, all on one machine.
The shapes transfer. The magnitudes are the stand's.

## The short version

| What happened | What it looked like | What it cost | Run |
|---|---|---|---|
| One unreadable record in a partition | Consumer restarting, lag flat, nothing in the logs but the same offset | 10 payments of 100 counted, **91 never read**, 124–125 restarts in 30 s | [exp-11](../experiments/transaction_guarantee/exp-11-poison-pill/) |
| A broker killed under load | API unaffected, outbox backlog rising | **0 payments refused**, 88–91 records queued, drained before the broker was back | [exp-13](../experiments/transaction_guarantee/exp-13-broker-outage/) |
| Two brokers killed under load | Same, larger — and the cluster reporting itself healthy | 0 refused, 469–523 queued, 18–24 s to catch up | [exp-13](../experiments/transaction_guarantee/exp-13-broker-outage/) |
| A field removed from the event schema | Nothing. No error anywhere | Amount arrives as **zero**; only a domain rule caught it | [exp-12](../experiments/transaction_guarantee/exp-12-schema-evolution/) |
| The commit placed before the work | Consumer looked healthy | **49 payments lost** out of 1000 | [exp-05](../experiments/transaction_guarantee/exp-05-at-most-once/) |
| The commit placed after the work, no inbox | Consumer looked healthy | **50 payments charged twice** | [exp-06](../experiments/transaction_guarantee/exp-06-at-least-once/) |
| `acks=1` and the leader lost | Producer reported success | **2 000 acknowledged records lost** | [exp-08](../experiments/transaction_guarantee/exp-08-acks/) |
| Idempotence turned off, retries under load | Nothing visible | 80–291 duplicate events per run | [exp-09](../experiments/transaction_guarantee/exp-09-reordering/) |
| A handler retrying inside the poll loop | Lag identical to the healthy case | Member removed, a stale commit **rewound a partition 1 726–1 732 records** | [exp-16](../experiments/transaction_guarantee/exp-16-lag-backpressure/) |
| A transaction left open | `read_committed` consumers of the partition stalled, behind a producer they had nothing to do with | Records hidden for **23.2 s** | [exp-10d](../experiments/transaction_guarantee/exp-10d-hanging-transaction/) |

## The failures worth reading twice

### A cluster that reports itself healthy while two thirds of it is gone

With three nodes that are all controllers, killing two leaves no quorum. Nothing can update
the metadata, so the surviving broker keeps serving the picture it had *before* the kill:
during the whole outage exp-13 read **zero under-replicated partitions and zero partitions
without a leader**, while not a single record could be published. `kafka-topics.sh
--describe` does not even manage that much — it times out on an operation that needs the
controller.

The consequence for an operator is precise: a panel labelled *under-replicated partitions*
is not a health check for a cluster that may have lost its quorum. It is a report from a
component that can no longer tell you anything, and it reads as good news. The number that
moved within a second, in every cell of exp-13, was the outbox backlog — read from the
service's own database, which is the one component a broker outage cannot reach.

### A schema change that raises no error anywhere

exp-12 removed a field from the event and retyped another, and published both. The
consumer decoded them without complaint, by two different routes to the same ending: a
removed field leaves no tag on the wire, so the reader fills its own zero value; a retyped
one arrives with a wire type that does not match, so protobuf files it under unknown fields
and leaves the typed field at zero. Either way the schema id in the record was valid, the
registry was happy, and `amount_minor` simply arrived as **zero**.

The only thing that noticed was a line in the domain — *an authorised payment is for a
positive amount* — which turned a silent zero into a refusal and a dead letter. Without it
the merchant's projection would have taken two payments of zero and every component would
have reported success.

The registry is not the safety net people assume: a new subject starts at compatibility
`NONE` and accepts every breaking change. And the levels do not nest the way the names
suggest — on Apicurio 3.0.9, `FULL` accepts a field deletion that `BACKWARD` refuses.

### One record that stops a partition for good

A record nobody can decode is not an error that passes. exp-11 put one after the tenth of a
hundred good records: with no dead letter route the consumer handled ten, died, came back,
read the same record, died again — **124 and 125 restarts in thirty seconds**, with 91
payments produced, acknowledged and never read, on a partition the broker considered
perfectly healthy.

It hides better than an outage: the pod restarts, the lag graph is a flat line rather than
a spike, and the logs repeat one offset. With the dead letter route, the same content
drained in 207–322 ms and the record went to the DLQ with the reason attached.

### Waiting inside the handler

exp-16 held a 20 s dependency outage two ways. Through the outage the lag was the same
either way — 7 940–8 501 records — so lag alone does not distinguish them. What differed
was everything else: the handler that retried inline lost its membership after the
rebalance timeout, had a commit refused, and its next commit **rewound a partition by
1 726–1 732 records**, of which 1 739 were handled twice in one of three runs. The consumer
that paused its fetches and kept polling had none of that.

## What this stand could not stage

- **`acks=all` with `min.insync.replicas=1`.** Three combined broker-controller nodes
  cannot lose enough brokers to reach it without losing the KRaft quorum first (exp-08).
- **A throttle that protects production traffic.** exp-15 showed the throttle limits
  replication exactly as configured, but an unthrottled 66 MiB move cost the producers
  nothing measurable here: one machine, local SSD, loopback network. On a cluster where
  replication competes with clients for one NIC the trade is real; this stand is not
  evidence of it.
- **Reusing a deleted field number** (exp-12d/d2) — the two cells of the schema matrix that
  were not run.
