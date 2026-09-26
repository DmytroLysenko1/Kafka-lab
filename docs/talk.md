# Seven things I believed about Kafka until I measured them

A talk built out of this repository. Forty minutes, one laptop, three brokers, and no slide
that says something a run here did not produce.

The premise is the same as the repository's first rule: **a statement about Kafka behaviour
is worth exactly as much as the run that demonstrates it.** So the shape of every section
below is the same — the thing everyone repeats, the number the cluster produced when asked,
and what to do differently on Monday.

Everything is reproducible on the machine in front of you: `make up`, then `make exp-NN`.
The numbers in the margins are from [the journal](00-journal.md), which has all
twenty-three runs with what went wrong in each.

---

## 1. "The key is a performance detail"

**What everyone says.** Keys balance the load across partitions.

**What the run said.** Ten thousand events over a hundred payments, produced twice —
without a key and with `key=payment_id`. Keyless: **6 827 to 8 721 ordering violations**
per ten thousand events, across seven runs. Keyed: **zero, every time.**

The magnitude was the surprise. The plan predicted about four thousand; the cluster
produced up to four in five events out of order, because a payment's events land on several
partitions, not two, and the consumer drains partition by partition.

**Monday.** The key is not a load-balancing decision, it is the ordering unit. "Ordered"
means "keyed by the entity whose order you care about" — and zero against a tendency is
what a guarantee looks like next to a habit.

> exp-01. And the keyed column doubles as the control: a non-zero there would have meant
> the counter, not Kafka, was broken.

---

## 2. "Delivery semantics are a setting"

**What everyone says.** You pick at-most-once or at-least-once in the config.

**What the run said.** One stand, one crash, three orderings of the same three lines —
commit, write, claim. **49 payments lost. 50 charged twice. Nothing at all.** Same broker,
same consumer, same kill.

**Monday.** Semantics are not a setting, they are where you put the commit relative to the
work, and whether the claim and the write share a transaction. The config only decides
which of your mistakes is possible.

> exp-05, exp-06, exp-07.

---

## 3. "acks=all is the safe choice"

**What everyone says.** Set `acks=all` and your writes are safe.

**What the run said.** With `min.insync.replicas=3` and one broker killed, `acks=all`
**refused all 2 000 writes**. With `acks=1` and the followers held back, the producer
reported success for **2 000 records that were then lost**. And under that same failure,
step for step, `acks=all` at `min.insync.replicas=2` acknowledged **none of them** — the
records were gone just the same, but the producer had been told so within 3 s, and nothing
it believed written was lost.

**Monday.** `acks=all` does not make writes safe on its own — it is `min.insync.replicas`
that decides how many copies count, and this run had it at 3. Together they make *unsafe
writes fail*; `acks=all` at `min.insync.replicas=1` succeeds from a single machine. That is a
different promise, and it is the one you want — but only if the thing behind the producer
can hold what the broker refuses. Which is the next section.

> exp-08. Note also what could not be staged here: `acks=all` at
> `min.insync.replicas=1` needs more brokers than three combined nodes can lose.

---

## 4. "The outbox is bookkeeping"

**What everyone says.** The transactional outbox is a pattern for correctness — the state
and the event in one transaction.

**What the run said.** Ten payments a second through the real API while two of three
brokers were killed for thirty seconds: **303 payments accepted, none refused**, 468–523
records queued in Postgres, and the relay worked them off in 18–24 seconds once the cluster
was whole.

**Monday.** The outbox is not only about correctness, it is about *availability*. Taking a
payment stops depending on Kafka being up. The event is owed, not lost, and the queue depth
is the number that tells you how much is owed.

> exp-13.

---

## 5. "The dashboard will tell you the cluster is sick"

**What everyone says.** Watch under-replicated partitions.

**What the run said.** Through that same outage, with two of three nodes dead and nothing
publishable, the cluster reported **zero under-replicated partitions and zero partitions
without a leader.** All three nodes are controllers; killing two leaves no quorum; with no
controller nothing updates the metadata, so the survivor keeps serving the picture from
before the kill. `kafka-topics.sh --describe` does not even manage that — it times out on
an operation that needs the controller.

**Monday.** A green replication panel is not a health check for a cluster that may have
lost its quorum — it is a report from a component that can no longer tell you anything, and
it reads as good news. Watch a number the outage cannot reach: the outbox backlog, in your
own database — counted there, on a clock of its own, not by a process that only reports
it after a publish succeeds. This stand's own dashboard got that wrong at first: its
backlog panel read 1 through the whole outage while 308 records waited, because the relay
set it at the end of a sweep and no sweep ended.

> exp-13 again. This is the line the [runbook](runbook.md) opens with.

---

## 6. "The schema registry protects us"

**What everyone says.** We have a registry, so incompatible schemas cannot be published.

**What the run said.** Three changes, three compatibility levels, a fresh subject each
time. **A subject nobody configured accepts every one of those breaking changes** — the
registry being installed protects nothing until a level is set on the subject. And the
levels do not nest the way their names suggest: on Apicurio 3.0.9, **`FULL` accepts a field
deletion that `BACKWARD` refuses** — which, since `FULL` is supposed to be both directions
at once, is a defect of that version rather than a lesson about compatibility modes.

Then the same changes as records, read by a consumer built against the old schema. No
decoding error anywhere — valid schema id, content parser, `amount_minor` arriving as
**zero**. The only layer that noticed was a line in the domain: *an authorised payment is
for a positive amount.*

**Monday.** The registry is a policy engine that is off until you configure it, and the
wire format will not tell you when a field disappears. Keep the domain rule that refuses
nonsense; on this evidence it is the last one standing.

> exp-12.

---

## 7. "One bad message is one bad message"

**What everyone says.** A poison record is an annoyance; you deal with it eventually.

**What the run said.** One undecodable record after the tenth of a hundred, with no dead
letter route: **10 payments counted, the other 90 produced and never read, and 124–125
process restarts in thirty seconds.** With the route: the same content drained in **207–322 ms**
and the record left with its bytes and a reason attached.

It hides better than an outage. The pod restarts, the committed offset never moves, and the
logs repeat one offset — the run did not sample lag, so what the lag graph would have drawn
is not something it can say.

**Monday.** The cost of a poison pill is not the record, it is the queue behind it. And the
classification is the whole trick: only "this will never work" may leave the partition. A
database that is down still holds the offset, because giving up on those is how payments go
missing quietly.

> exp-11.

---

## The part that is not about Kafka

Four of the runs above were wrong the first time, and the wrongness was always the same
shape: **absence of data rendered as zero.**

- A backlog count that returned `-1` on failure, compared with `<=`, would have reported a
  database outage as a drained queue.
- A partition count read from an empty CLI result reported a perfectly healthy cluster at
  the moment two thirds of it was dead.
- A "60 MiB" replication move copied six megabytes, because the payload was a repeating
  pattern that compressed away — and the throttle under test looked broken.
- A restart in a harness reused the same client, which kept its own fetch position, so the
  "restarted" consumer walked past the record that had killed it.

Each was caught by a number that could not be true of the story being told: everything
committed with almost nothing handled, a healthy cluster with a growing queue, a throttled
move finishing in two seconds. **The instrument is part of the experiment**, and a run that
disagrees with itself is telling you about the run.

What this repository does about it: a superseded run is moved into a `results/superseded/`
folder with a README saying what was wrong with it, so the withdrawn number stays auditable
without being quotable, and the journal records the correction. A number measured with a
different instrument does not belong in the same table.

---

## How to run the demo live

```
make up                               # brokers, Postgres, registry, Prometheus, Grafana
make migrate
go build -o /tmp/api ./cmd/payments-api        # build, do not `go run`:
go build -o /tmp/relay ./cmd/outbox-relay      # go run does not pass signals to the
go build -o /tmp/consumer ./cmd/payments-consumer   # program it built, and the orphan
                                               # keeps taking rows from your demo
```

Open Grafana on `localhost:3000`, take a payment with `curl`, watch it go through. Then
`docker kill kafka-lab-kafka1 kafka-lab-kafka2` and watch the replication and broker panels
insist everything is fine while `published/s` drops to zero and the backlog panel climbs —
which it did only after exp-13 caught it sitting flat and the relay learned to count it on
its own clock.

The interactive walkthroughs in [`dynamic/`](dynamic/) tell the same stories step by step —
write path, read path, the lost message, the outbox, the rebalance, the retry chain — and
are the backbone of the slides.

## If the audience remembers three sentences

1. Ordering is a key, not a setting; delivery semantics are where you put the commit.
2. The outbox buys availability, not just correctness — and its backlog is the one number
   an outage cannot take from you.
3. Watch what the failure cannot reach, and distrust a component's opinion of its own
   health.
