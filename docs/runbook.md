# Runbook

What to do when the payments service is unwell, written from runs rather than from
principle. Every "why" here points at a measurement in the
[failure report](failure-report.md) or the [journal](00-journal.md).

## Before anything: which numbers to trust

Look at these three, in this order:

1. **`outbox_backlog_records`** — records the service owes Kafka. Read from the service's
   own Postgres, which is the one component a broker outage cannot reach.
2. **`kafka_consumergroup_lag`** — read from the cluster by the exporter, not by the
   consumer: a consumer that has stopped cannot report its own lag.
3. **`payment_events_dead_lettered_total`** — anything here is a record no retry will fix.

And know which number lies. **Under-replicated partitions is not a health check.** If the
cluster has lost its controller quorum, nothing can update the metadata and the survivors
keep serving the last healthy picture — exp-13 read zero under-replicated and zero
leaderless partitions through an outage in which nothing could be published. A green
replication panel plus a growing outbox backlog means the cluster, not the service.

## The backlog is growing

**It is supposed to, during a broker outage.** The outbox exists so that payments keep
being accepted while events wait. exp-13: 303 payments accepted and none refused while two
of three brokers were dead.

1. Is the API still answering? `http_requests_total` by status. If yes, the failure is
   behind the service, not in it — you have time.
2. Is the relay alive? `outbox_sweeps_total` should keep climbing whatever its outcome. A
   *flat* sweep counter means the relay is gone; a rising `failed` count means it is
   running and the broker is not answering.
3. Is the cluster short of brokers? `kafka_brokers` from the exporter, and
   `docker compose ps`. Below `min.insync.replicas` nothing publishes at all.

**What to expect once the cluster is back:** the relay drains on its own, at roughly its
batch size per sweep interval. exp-13 worked off 469–523 records in 18–24 s while payments
kept arriving at 10/s.

**Do not** restart the relay to "unstick" it. A restart re-reads from the last committed
state and changes nothing about a broker that is down; what it does change is the
`FOR UPDATE SKIP LOCKED` claims in flight, and the sweeps you were watching.

## The consumer is restarting and the lag is flat

This is the poison pill signature, and it is quiet: the pod restarts, the offset does not
move, the logs repeat. exp-11 measured 124–125 restarts in thirty seconds with 91 records
behind the bad one never read.

1. `payment_events_dead_lettered_total` — if it is climbing, the route is working and this
   is not your problem.
2. If it is not climbing and the consumer keeps dying, the dead letter path itself is
   failing. The consumer holds the offset deliberately in that case: a record nobody can
   store is better stuck than silently gone.
3. Find the record: the consumer logs partition, offset and reason on every dead letter.

**Fix forward, not backward.** Do not skip the offset by hand unless you have read the
record and can say what losing it costs. The dead letter topic keeps the bytes and the
reason; a manual offset bump keeps neither.

## Records are arriving in the dead letter topic

Read the `dlq_reason` header. Two classes, two different problems:

- **`undecodable`** — bytes nobody can read. Usually a producer at another version, or
  something that is not our event at all. Support problem: the record is archived with its
  origin topic, partition and offset.
- **`refused`** — decoded fine, and the consumer will not take it. exp-12 produced exactly
  this by removing `amount_minor` from the schema: the field arrived as zero and the domain
  rule refused it. **Check what changed in the producer's schema before assuming the
  consumer is wrong.**

## A broker is down

1. `make check` — non-zero means drift or an unhealthy cluster.
2. One broker with RF 3 and `min.insync.replicas=2` is survivable: writes continue, the
   partitions it led need a new leader first. exp-13 saw 88–91 records queue during that
   election and drain before the broker was back.
3. When it returns, leadership does **not** come back on its own: this stand has auto
   leader rebalance switched off. Run `make elect-preferred`, then `make check`.
4. Two brokers down is below `min.insync.replicas` *and* below the controller quorum.
   Nothing publishes, and the cluster's self-reporting freezes (see above). Bring a broker
   back before trying to diagnose anything from the cluster's own answers.

## Planned: changing a schema

1. Set the compatibility level on the subject **before** registering anything. A new
   subject accepts every breaking change until a level is set on it — measured, on all three
   changes (exp-12g). Set the level explicitly; do not assume the registry arrived with one.
2. Do not choose the level by the strongest-sounding name. On Apicurio 3.0.9, `FULL`
   accepts a field deletion that `BACKWARD` refuses.
3. Adding a field at a new number is safe, and old consumers skip it — measured, not
   assumed. Removing a field or retyping one at the same number is not: nothing raises an
   error and the value arrives as zero. Note where each is stopped: on Apicurio 3.0.9 a
   retype is refused at registration under both `BACKWARD` and `FULL`, so it reaches the
   wire only on a subject left at `NONE` — which is where a new subject starts.
4. Whatever the registry says, keep the domain rule that refuses a nonsense value. In
   exp-12 it was the only layer that noticed.

## Planned: changing partitions or replication factor

- **Partitions only grow, and growing them moves keys.** Events of one payment before and
  after the change land in different partitions. Treat it as a migration.
- **Replication factor is a reassignment**, and topicctl refuses to do it — it stops with
  *"this cannot be resolved by topicctl"* once the observed ISR no longer matches the YAML.
  Write a plan file, `--execute`, and then keep running `--verify` until it reports
  completion.
- **`--verify` is not a status check.** It is what removes the throttle `--execute` set.
  Stop before it and every later replication on that cluster crawls at the throttled rate
  with nothing in the topic's config to explain why.
- **Size the move by the per-broker rate, not the cluster's.** exp-15: 66 MiB onto three
  brokers at 1 MiB/s took 24 s, matching 22 MiB per broker — not 66.

## Planned: restarting a consumer

With `group.instance.id` set, a restart inside the session timeout costs one rebalance of
nothing: exp-14b measured 2.05 s of idle partitions and no reassignment. Without it, the
same restart costs a full rebalance — and its cost depends on the protocol: 39–75 ms eager,
0.52–0.68 s cooperative, 4.9–6.5 s under KIP-848 (exp-14).

The trade is the other side of that coin: with a static id, a member that dies for good is
only noticed when the session expires — 12.6 s of partitions nobody reads.

## What not to do, with the reason attached

| Don't | Because |
|---|---|
| Retry inside the handler through an outage | exp-16: the member was removed and its stale commit rewound a partition 1 726–1 732 records. Pause fetches instead |
| Commit the offset before the work | exp-05: 49 payments of 1000 lost |
| Publish to Kafka from the use case | The event and the state stop being one transaction; exp-13's whole result depends on them being one |
| Trust a green under-replicated panel | exp-13: it read zero through an outage that blocked every write |
| Turn off producer idempotence for throughput | exp-09: 80–291 duplicate events per run, invisible until someone counts |
| Skip an offset to clear a poison record | The DLQ keeps the bytes and the reason; a manual bump keeps neither |
