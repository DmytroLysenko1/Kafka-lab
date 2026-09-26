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
| payments accepted during the outage | **302 · 302 · 302** | **303 · 303 · 303** |
| payments refused | **0** | **0** |
| outbox backlog, highest while the brokers were down | 88 · 91 · 133 records | 303 · 309 · 308 records |
| outbox backlog, highest overall | the same | 469 · 523 · 468 records — reached *after* the brokers returned |
| what the cluster said about itself | 3 partitions under-replicated | **0 under-replicated, 0 without a leader** |
| back to the pre-outage backlog | already caught up when it returned | **18.3 s · 24.2 s · 18.3 s** after they returned |
| what the dashboard's backlog panel showed | 0, and 84 on one scrape | **1, flat, through the whole outage** — and, with the relay fixed, 5 → 460 |

Three runs: two on 2026-09-25 and [one on 2026-09-26](results/run-2026-09-26-162208.log), the
last also dumping the dashboard's panels from Prometheus for each cell's window; and a
fourth, [after the relay fix](results/run-2026-09-26-171149.log), which reproduced the
cells (303 accepted, 0 refused, 308 during the outage, 458 at the peak, 17.3 s to catch up)
and is the one where the backlog panel moved.

## What it shows

**The front door never faltered.** Not one payment was refused in either cell, because
taking a payment is a write to Postgres and nothing else. The events it owes Kafka wait in
the same transaction that stored it. That is the whole purpose of the outbox, and this is
what it looks like as a number: 303 payments accepted while the cluster could not take a
single record.

**One broker down is a pause, not an outage.** `min.insync.replicas=2` with three replicas
means the write still has a quorum — but the partitions led by the dead broker need a new
leader first, and the backlog grows by about ninety records while that happens. Then it
drains on its own, before the broker is even back: by the time the container was running
again the queue was already at its pre-outage level.

**Two brokers down is the case the outbox exists for.** Nothing can be published for the
whole thirty seconds and the backlog grows by the load, about 300 records. It keeps growing
for a while after the cluster is whole — to 468–523 — because the relay's first sweeps
after the outage are still waiting out their timeouts against the dead connections; then it
works the queue off in 18–24 s while payments keep arriving at ten a second throughout.

**And the cluster reports itself healthy the entire time.** In cell B the metadata says
zero under-replicated partitions and zero without a leader, which is exactly wrong: two
thirds of the cluster is gone. All three nodes are controllers here, so killing two leaves
no quorum, and with no controller nothing can update the record. The surviving broker keeps
serving the last metadata it had, from before the kill. `kafka-topics.sh --describe` does
not even get that far — it times out on `listPartitionReassignments`, an operation that
needs the controller.

**And the dashboard's backlog panel lied too — that one is ours.** Read from Prometheus for
cell B's window, the panel sat at **1** from before the kill until the backlog had drained,
while the service's own database held 308 records at the height of the outage. The relay
reports the backlog when a sweep finishes, and during the outage no sweep finished: each
one waited on a publish to a cluster that could not answer, up to the relay's 30 s sweep
timeout. So in the one failure this panel was built for, every panel an operator would
look at said *healthy* — backlog 1, brokers 3, under-replicated 0 — and only
`published/s` falling to zero said otherwise. The number that moved, in both cells and
within a second, was the backlog as this experiment read it: straight from Postgres. The
relay now counts it on a clock of its own, in a goroutine beside the sweeps, and the rerun
shows the panel doing its job: 5 before the kill, then 10, 60, 110 … 460 every five
seconds through the outage, back to 5 once the relay caught up. It trails the database by
about 15 s — scrape interval plus the relay's tick — which is a lag, not a lie. The dashboard also trails the cluster by 15–20 s in cell A (brokers 2 from +40 s to
+65 s for a kill at +20 s and a restart at +50 s): scrape interval, exporter polling and
rate windows, stacked.

That is the part worth carrying into a runbook. The dashboard panel labelled
"under-replicated partitions" is not a health check for a cluster that has lost its quorum;
it is a report from a cluster that can no longer tell you anything. The backlog measured in
the service's own database is the one number the outage could not reach — as long as the
gauge is counted on its own clock, and not only when a sweep succeeds.

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
