# exp-13 — a broker outage under load

Ten payments a second through the real API, with brokers killed underneath the relay for
thirty seconds. Two cells: one broker down, which the cluster is configured to survive, and
two, which it is not.

```
make exp-13
```

## What it measures

| | cell A — one broker down | cell B — two brokers down |
|---|---|---|
| payments accepted during the outage | **302 · 302** | **303 · 303** |
| payments refused | **0** | **0** |
| peak outbox backlog | 88 · 91 records | 469 · 523 records |
| what the cluster said about itself | 3 partitions under-replicated | **0 under-replicated, 0 without a leader** |
| back to the pre-outage backlog | already caught up when it returned | **18.3 s · 24.2 s** after they returned |

Two runs, 2026-09-25, in [`results/`](results/).

## What it shows

**The front door never faltered.** Not one payment was refused in either cell, because
taking a payment is a write to Postgres and nothing else. The events it owes Kafka wait in
the same transaction that stored it. That is the whole purpose of the outbox, and this is
what it looks like as a number: 304 payments accepted while the cluster could not take a
single record.

**One broker down is a pause, not an outage.** `min.insync.replicas=2` with three replicas
means the write still has a quorum — but the partitions led by the dead broker need a new
leader first, and the backlog grows by about ninety records while that happens. Then it
drains on its own, before the broker is even back: by the time the container was running
again the queue was already at its pre-outage level.

**Two brokers down is the case the outbox exists for.** Nothing can be published for the
whole thirty seconds, the backlog grows to four or five hundred records, and the relay
works it off in eighteen to twenty-four seconds once the cluster is whole — while payments keep
arriving at ten a second throughout.

**And the cluster reports itself healthy the entire time.** In cell B the metadata says
zero under-replicated partitions and zero without a leader, which is exactly wrong: two
thirds of the cluster is gone. All three nodes are controllers here, so killing two leaves
no quorum, and with no controller nothing can update the record. The surviving broker keeps
serving the last metadata it had, from before the kill. `kafka-topics.sh --describe` does
not even get that far — it times out on `listPartitionReassignments`, an operation that
needs the controller.

That is the part worth carrying into a runbook. The dashboard panel labelled
"under-replicated partitions" is not a health check for a cluster that has lost its quorum;
it is a report from a cluster that can no longer tell you anything. The number that did
move, in both cells and within a second, was the outbox backlog — measured in the service's
own database, which is the one component the outage could not reach.

## Four ways this run lied before it told the truth

Each was the same mistake wearing a different hat: **absence of data rendered as zero.**

1. The first version counted under-replicated partitions across the whole stand — dozens
   of them, left by earlier experiments — and reported 107 as if it were this run's.
2. Scoping it to the topic fixed that and broke something else: `kafka-topics.sh` prints
   connection warnings and an empty result when brokers are missing, and counting the lines
   of an empty result reported a perfectly healthy cluster at the moment two thirds of it
   was dead.
3. Reading the metadata through `kadm` instead of parsing CLI output was the right move,
   but ranging over a response that did not contain the topic produced zeros again.
4. And after all three were fixed for the cluster readings, the backlog read still returned
   `-1` on failure — which the drain check compared with `<=` and would have reported as a
   drained queue. A fresh-context review found that one; the author had just written the
   rule three times and not applied it to the fourth number on the same line.

A reading nobody could take is `unknown` now, and it prints as `unknown`. The zeros left in
cell B survived all four fixes, which is how they earned the right to be believed.
