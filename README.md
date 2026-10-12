# Kafka-lab

A lab for **Apache Kafka in depth** and for **running the same service on Kubernetes**:
Kafka's internals and delivery guarantees, a payments service built on them, and its
operation in containers. Every claim in this repository is backed by a run whose output is
committed — an experiment (`make exp-NN`) or a Kubernetes drill (`make k8s-drill-NN`) — or it
says it is not: `TBD` until the experiment has run, "not logged" or "not measured" where a
number comes from watching a session rather than from a log.

**What is here.** A payments service, end to end: `POST /payments` stores the payment and an
outbox row in one transaction; a relay publishes the outbox to Kafka as Protobuf through a
schema registry; a consumer counts each payment into its merchant's total per currency,
exactly once through an inbox — for as long as an event comes back within the topics'
retention and the inbox's 40 days, the limits listed in
[docs/k8s/decisions.md](docs/k8s/decisions.md#lab-not-production); a payment whose merchant row is locked moves through three
retry topics and, past the last, into a dead letter topic that `cmd/dlq-replayer` replays.
Around it: three KRaft brokers and the topic catalog as YAML ([`deploy/topics/`](deploy/topics/),
applied with [topicctl](https://github.com/segmentio/topicctl)), the experiments behind each
claim, a Grafana dashboard, a failure report and a runbook. The same service runs on kind with
Kafka on Strimzi, deployed by a Helm chart and exercised by scripted drills —
[`docs/k8s/`](docs/k8s/). It is a lab, and what it leaves out on purpose — backups, TLS,
separate database roles, alerting — is listed in
[docs/k8s/decisions.md](docs/k8s/decisions.md#lab-not-production).

**Terms used before they are explained.** *KRaft* — Kafka's own consensus for cluster
metadata, which replaced ZooKeeper; *ISR* — the in-sync replicas, the copies of a partition a
write must reach before `acks=all` succeeds ([04](docs/static/04-isr-leader-election.md));
*outbox* — the table the API writes an event into in the same transaction as the payment,
for a relay to publish ([06](docs/static/06-transactional-outbox.md)); *inbox* — the table of
event ids the consumer has counted, so a redelivered event counts once; *DLQ* — the dead letter
topic, where a record goes that cannot be processed, to be replayed by hand
([07](docs/static/07-retry-dlq.md)).

| KR (PDP) | Deliverable | Where it lives | State |
|---|---|---|---|
| Fundamentals and internals | cheat sheet with write and read path diagrams | [`docs/static/`](docs/static/) — index plus cases 01–04 | diagrams and cases 01–04 done; exp-01…04 measured — a key is worth 6 827–8 721 ordering violations per 10 000 events, a skewed key caps a group at 1.17× one consumer, a dead broker is noticed in 10.1–10.6 s. Open: exp-03b/03d (a segment dump, effective retention); exp-04b is blocked on this stand |
| Delivery guarantees and tuning | notes on semantics plus a tuning checklist | cases 05, 08, 10; [`docs/tuning-checklist.md`](docs/tuning-checklist.md) | measured: exp-05…10, 14, 16, 17 — the same crash loses 49 payments, charges 50 twice, or costs nothing, depending on where one line sits; `acks=1` lost 2 000 acknowledged records that `acks=all` kept. The checklist says when to use each setting |
| Production-shaped Go app | working repo, README, architecture diagram | [`internal/`](internal/), [`cmd/`](cmd/), [`proto/`](proto/), this file | built: outbox → relay → consumer with an inbox, a retry chain of 5 s → 1 m → 10 m with its own consumer groups, a dead letter topic and its replayer, Protobuf through Apicurio at `BACKWARD`. exp-18: 594 of 600 payments counted while their merchant's row stayed locked. `make test-integration` covers the database and Kafka edges against the stand ([Running the service's tests](#running-the-services-tests)) |
| Operate, observe, stress | Compose, Grafana dashboard, failure report, runbook | [`deploy/`](deploy/), [`docs/failure-report.md`](docs/failure-report.md), [`docs/runbook.md`](docs/runbook.md) | three KRaft brokers in Compose, Prometheus and a Grafana dashboard, a failure report and a runbook; broker loss, rebalance, reassignment and lag measured (exp-13…16) — at 10 payments/s none was refused while brokers died, and the cluster reported itself healthy with two of three gone |
| Patterns and anti-patterns | recommendations doc | [`docs/patterns.md`](docs/patterns.md) | written: every anti-pattern points at the run where the damage is a number — one unreadable record left 91 records unread without a dead letter route, and all 100 counted with one (exp-11). CDC, stream enrichment and unbounded retention are argued, not run, and say so |
| Share findings | write-up or tech talk | [`docs/talk.md`](docs/talk.md), and the walkthroughs in [`docs/dynamic/`](docs/dynamic/) | written: eight claims everyone repeats, each against the number this stand produced, with six interactive walkthroughs to present them |

## Running the cluster

```
make up                          # brokers, Postgres, the Apicurio registry, and the metrics stack
make topics                      # create or update the catalog in deploy/topics
make check                       # non-zero exit on drift or on a cluster unfit to measure on
make describe TOPICS=payments.main
make lag GROUP=payments-consumer
make reset-topic TOPIC=payments-consumer.dlq
make elect-preferred             # move leadership back to the preferred replicas
make stop                        # pause, keeping the brokers' data
make down                        # stop and wipe the brokers' data
```

Topics are declared, not scripted: one YAML file per topic, applied by
[topicctl](https://github.com/segmentio/topicctl). The permanent catalog lives in
[`deploy/topics/`](deploy/topics/). An experiment's own topics — `min.insync.replicas=3`
for exp-08, a compacted one for exp-03 — live in `experiments/<group>/<exp>/topics/` and
are applied with `make exp-topics EXP=<group>/<exp>`, so they neither become part of the
catalog nor keep being checked after the experiment is over.

Every YAML states `min.insync.replicas` explicitly, and `make check` refuses to run
otherwise. With eligible leader replicas (KIP-966) the
controller keeps `min.insync.replicas` as a cluster-wide dynamic default — copied from the
broker setting, 2 here, when the cluster is first formatted — and topicctl would report
that inherited value as drift on every run of a YAML that leaves it out. Why every number
in the catalog is what it is, and which experiment gets a topic of its own, is written
down in [`deploy/topics/README.md`](deploy/topics/README.md).

`make check` is the step to run before every experiment. It fails on two kinds of problem.
**Drift**: a partition count or declared setting that no longer matches the YAML, and any
topic-level setting the YAML never declared. **Health**: replicas out of sync, throttles
left over from a reassignment, leaders off their preferred replica. The second kind matters
as much as the first — a measurement taken on a broker that leads four of six partitions
after the previous experiment is a number about that experiment, not about this one:

```
$ make check
  config settings correct | ✗ | 1 keys have different values between cluster and topic config: [cleanup.policy]
make: *** [check] Error 1
```

Because topicctl reads the replication factor off the in-sync replicas, a dead broker shows
up in `make check` as a failure too — which is the right answer before a measurement — and
`make topics` should only be run with all three brokers up.

Two behaviours of `topicctl apply` shape the rest. It **elects the preferred leader** on
every topic it touches, and with `auto.leader.rebalance.enable=false` that is the only thing
that moves leadership back on its own — so `make topics` is never run in the middle of an experiment
that measures leadership, and `make elect-preferred` exists to do it deliberately. And it
**only warns** about a setting the YAML never declared, leaving it in place, so
`make reset-topic` is the way back: it drops the topic and recreates it from its own YAML,
resetting the log as well as the config, without touching any other topic. It refuses to
delete a topic no YAML declares.

Brokers are reachable from the host at `localhost:19092,29092,39092`, bound to loopback
only. There is no restart policy on purpose: an experiment that kills a broker needs it
to stay dead until the experiment brings it back. Several broker defaults are pinned in
the Compose file for the same reason — auto leader rebalance and the retention check both
run on 300 s timers that would otherwise decide an experiment's outcome instead of Kafka.

## Running the service

What the host needs: Go 1.26.4 (`go.mod`), Docker with Compose v2, golangci-lint v2 for
`make verify` (`.golangci.yml` is a v2 config), and for `make proto` buf (1.70.0 here) with
`protoc-gen-go` v1.36.11, the version `gen/` was generated with
(`go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11`). topicctl is not
installed: the Makefile runs a pinned version with `go run`. The Kubernetes stand needs more,
listed in [docs/k8s/README.md](docs/k8s/README.md#tools-as-the-evidence-was-produced-with-them).

The services read their configuration from the environment. `make` targets set it; a
`go run` from a plain shell needs it exported first — these are the compose stand's
addresses:

```
export DATABASE_URL='postgres://lab:lab@localhost:5432/lab?sslmode=disable'
export KAFKA_BROKERS=localhost:19092,localhost:29092,localhost:39092
export SCHEMA_REGISTRY_URL=http://localhost:8080/apis/ccompat/v7
```

```
make up                          # brokers, Postgres, the Apicurio registry, and the metrics stack
make migrate                     # apply the schema; services never migrate on boot
make proto                       # regenerate gen/ from proto/ after changing a schema
go run ./cmd/outbox-relay        # publish what the outbox holds, until SIGTERM
go run ./cmd/payments-consumer   # read the topic and the retry tiers, deduplicate, keep merchant totals
go run ./cmd/dlq-replayer        # send exhausted dead letters back to the first tier, then exit
PAYMENTS_API_KEY=... go run ./cmd/payments-api   # authorise, capture, refund; GET /merchants/{id}/total
```

`make up` also brings up Prometheus on `localhost:9090`, Grafana on `localhost:3000` with
the dashboard already provisioned, Tempo on `localhost:3200` (a Grafana datasource, fed over
OTLP on `4318`), and a kafka-exporter on `9308` that reads consumer group
lag and partition health from the cluster rather than from the services — a consumer that
has stopped cannot report its own lag, and that is exactly when the number matters. Grafana
has no login on purpose: a dashboard on loopback that asks for a password is a dashboard
nobody opens during the incident it was built for. Prometheus scrapes every 5 s, because
these experiments measure the seconds after a broker dies and the usual 15 s would draw a
recovery as a straight line between two samples.

Each service serves its metrics on a listener of its own — the relay on `:9101`, the
consumer on `:9102`, the API on `:9103` — so a scrape can never take a connection a payment
needed. What they publish is what an operator reads during an incident:
`outbox_backlog_records` (grows while the broker is away, falls when the relay drains — counted on a tick of its own, because exp-13 caught it flat at 1 with 308 records waiting when it was set only at the end of a sweep, and none ended during the outage),
`outbox_sweeps_total{outcome}`, `payment_events_handled_total{outcome}` where a duplicate
is counted apart from a fresh event, `payment_events_dead_lettered_total{reason}`, `payment_events_failed_total{stage}` — the
one that tells a crash-looping consumer from an idle one, since both draw the handled
counter to zero — and `http_requests_total{route,status}` labelled by route pattern rather
than path, because a label built from merchant ids grows a new series per customer.

The dashboard opens with a liveness row rather than a graph: `up` per service, `up` for the
exporter, and `kafka_brokers`. Every zero below it means either nothing happened or nothing
is left to report it, and that row is meant to tell the two apart. exp-13 showed where it
cannot: with the controller quorum gone, `kafka_brokers` read 3 with two brokers dead,
because the exporter asks the same frozen metadata that read 0 under-replicated. A panel
named after that run sits at the bottom, meant to be non-empty when the backlog piles up
while the cluster still calls itself healthy — and it stayed empty through that very
failure until the relay's backlog gauge stopped depending on a sweep ending. `make verify` checks
that every panel's query names a metric something actually publishes, so a rename cannot
silently blank a panel.

The API binds to loopback and requires `X-API-Key`; it refuses to start without
`PAYMENTS_API_KEY`, because a service that moves money should not have a mode where it
serves callers it cannot name. There is no TLS and one shared key — enough for a lab, not
for a network anyone else is on.

```
curl -X POST localhost:8081/payments -H "X-API-Key: $PAYMENTS_API_KEY" \
     -H 'Idempotency-Key: demo-1' -H 'Content-Type: application/json' \
     -d '{"merchant_id":"m-42","amount_minor":1999,"currency":"EUR"}'
curl -X POST localhost:8081/merchants/m-42/payments/$PAYMENT_ID/capture -H "X-API-Key: $PAYMENTS_API_KEY"
curl -X POST localhost:8081/merchants/m-42/payments/$PAYMENT_ID/refund  -H "X-API-Key: $PAYMENTS_API_KEY"
curl localhost:8081/merchants/m-42/total -H "X-API-Key: $PAYMENTS_API_KEY"
```

The first POST answers 201, the same request again answers 200 with the same payment id,
and the same key with a different amount answers 409 rather than quietly returning the
earlier payment. A payment moves `authorized → captured → refunded`, whole amount each time:
capture and refund answer 200 with the status the payment is in, and a repeat of a move that
already happened answers 200 again and changes nothing — safe to retry without a key of its
own. Refunding what was never captured, or capturing what was refunded, answers 409
`invalid_transition`; another merchant's payment answers 404, the same as one that does not
exist. The total comes back per currency, each kind of movement apart —
`{"merchant_id":"m-42","totals":[{"currency":"EUR","authorized_minor":1999,"captured_minor":1999,"refunded_minor":1999}]}`
— because amounts in different currencies are never added into one number, and money
authorised, taken and given back are three different questions; a merchant nobody has paid
has `"totals":[]`. Why those sums survive the retry tiers delivering a refund before its
capture is in [`docs/static/07-retry-dlq.md`](docs/static/07-retry-dlq.md#retries-reorder-events-by-design).

Every answer carries `X-Trace-Id`. Searched in Grafana's Explore against Tempo, it shows that
request end to end: the HTTP span, the use case, each SQL statement, the record the relay
published from the outbox row — the row keeps the trace it was written in (`outbox.trace_context`)
— and the consumer that counted it, down to its own statements. Logs carry the same
`trace_id`, and a dead letter carries it in `dlq_trace_id`.

Two bookkeeping tables would otherwise grow forever: the inbox (every event id the consumer
has counted) and the outbox (every event ever published). `make prune` (`cmd/prune`, an hourly
CronJob on Kubernetes) deletes outbox rows published more than 7 days ago — the life of
`payments.main`'s retention — and inbox rows counted more than 40 days ago. The inbox horizon is not a
tuning knob: an event can come back up to 39 days after it was counted (30 days in the DLQ and
7 more in a retry tier after a late replay, each plus the one-day segment Kafka deletes as a
whole), and an inbox that forgot it sooner would count the
payment twice, so `internal/domain/retention` refuses anything under 40 — the 39 days and one
more for the API's, the consumer's and prune's clocks not agreeing. The consumer, for its part,
refuses to count an event more than 39 days old: past that the inbox may no longer remember it. Unpublished outbox
rows are never deleted, however old.

`go run` does not pass a signal on to the program it built, so stopping either of these with
`kill` on the `go run` process leaves the real one running — building first (`go build -o
/tmp/relay ./cmd/outbox-relay`) is the way to keep a stray service from quietly competing
for the same outbox rows as your tests. That is not hypothetical; see the journal.

The registry is at `localhost:8080`; the producer talks to its Confluent-compatible API at
`/apis/ccompat/v7`, which is the wire format the records carry — a magic byte, the schema
id, the message indexes, then the payload. It keeps its schemas in a database of its own
inside the lab Postgres, created by [`deploy/postgres/init/`](deploy/postgres/init/) when
that volume is first initialised; a Postgres volume that predates the registry needs
`CREATE DATABASE apicurio` once, because init scripts only run on an empty data directory.
Its `/system/info` calls itself "Apicurio Registry (In Memory)" whatever the storage is —
the log line `Using postgresql SQL storage` is the one that tells the truth.

## Running the service's tests

```
make verify                      # build, vet, lint, and the unit tests under -race
make test-integration            # the same, against the stand from make up
```

`make verify` needs nothing running: the use case is tested against a fake that stages its
writes and applies them only on commit, so a rollback is distinguishable from a write that
never happened, and the relay's own tests run the ticker inside `testing/synctest` with
`goleak` watching for a goroutine that outlived its context. What a fake cannot prove goes
to `make test-integration`, which talks to the compose stand and is skipped by a plain
`go test` through the `integration` build tag: `ON CONFLICT` under eight concurrent
authorisations of one idempotency key, eight concurrent captures of one payment announcing one
capture, a refund racing its capture never refunding what was not captured, the `CHECK` constraints as the last line of defence
under the domain, `FOR UPDATE SKIP LOCKED` handing each outbox row to exactly one relay,
and a consumer decoding a published record through the registry — key, headers and all.
Each of those runs against a topic of its own, created and deleted by the test, so nothing
lands in the topics the experiments count.

## Code layout

| Where | What lives there | Imports |
|---|---|---|
| `internal/domain/payment`, `internal/domain/merchant` | the rules: a payment is authorised for a positive amount and moves only forward, `authorized → captured → refunded`; a movement the consumer counts carries an event id, a kind, a merchant and an amount the total can take. Values that break a rule cannot be made | nothing internal |
| `internal/application/*` | use cases — authorise, capture and refund a payment, record a movement once, read the totals, publish the outbox, prune — and the small interfaces each one needs | the domain |
| `internal/infrastructure/*` | Postgres (unit of work, idempotent insert, outbox, inbox, totals), Kafka (publisher, the chain's stages, detours, replay), metrics | application ports, domain |
| `internal/interfaces/http` | the HTTP front door and the one table that turns a domain refusal into a status | application, domain errors |
| `cmd/*` | the three services and the two one-shot jobs — `migrate` and `prune` — each a `run` function handed to `lifecycle.Main`, and `dlq-replayer`, a one-shot tool that keeps its own `main` because it has three outcomes, not two — exit 2 when it never started, 1 when it failed part-way, 0 when done | everything |
| `pkg/env`, `pkg/lifecycle`, `pkg/httpserve`, `pkg/telemetry` | what every process needs and no business rule touches: configuration that reports every problem at once, the signal-and-exit-code skeleton, graceful HTTP serving, tracing and trace-aware logs | the standard library, OpenTelemetry |

The same boundary is drawn twice on purpose: `payment.MerchantID` and `merchant.ID` have the
same rule today, and belong to two contexts — the one that issues merchant ids and the one
that counts them — so that neither has to change when the other does.

## Architecture

```mermaid
flowchart LR
    API["payments-api<br/>HTTP"] -->|"one tx:<br/>payment + outbox row"| PG[("Postgres<br/>payments · outbox · inbox")]
    PG -->|"unpublished rows,<br/>SKIP LOCKED"| REL["outbox-relay"]
    REL -->|"produce after the ack,<br/>key = payment_id"| MAIN["payments.main<br/>6 partitions"]
    MAIN --> CG

    subgraph CG["consumer group payments-consumer"]
        direction TB
        W1["worker 1 · schema v2<br/>p0 p1 p2"]
        W2["worker 2 · schema v1<br/>still rolling · p3 p4 p5"]
    end

    CG -->|"merchant row locked"| RETRY["payments-consumer.retry.*<br/>5s · 1m · 10m"]
    RETRY --> RC["tier stages, a group each<br/>pause the waiting partition<br/>until its delay elapses"]
    RC -->|"still locked<br/>after the last tier"| DLQ["payments-consumer.dlq"]
    CG -->|"undecodable or refused:<br/>no retry fixes it"| DLQ
    DLQ --> RP["dlq-replayer"]
    RP -->|"once the lock is gone,<br/>into the first tier"| RETRY
    CG -->|"one tx: inbox ON CONFLICT<br/>+ business write"| PG

    SR["Schema Registry · BACKWARD<br/>set by the relay on its subject,<br/>checked at registration,<br/>never in the data path"]
    REL -.->|"declare BACKWARD,<br/>register, keep the id"| SR
```

*Fig. — the bottom row is the write path and the top row is the consume path, and every
solid arrow between them is a boundary where atomicity ends: the API writes state and
event in one transaction and never calls the broker, the consumer writes the inbox row and
the business row in one transaction and never sleeps, and everything in between is
retryable delivery. The dotted edge is the only one that is not per record: the relay
declares BACKWARD for its subject and registers the schema once, when it starts. The consumer
does not call the registry at all — it strips the schema id off the front of each record,
reads the message index the framing carries after it, and decodes the message at that
position in `events.proto` (authorized, captured or refunded, `decode.go`); protobuf keeps
that safe across compatible versions, because fields are matched by number. The group is drawn mid-deploy, one worker on each schema version, because that
is the only state in which compatibility means anything
([09](docs/static/09-schema-evolution.md)).*

| Component | What it guarantees | Detail |
|---|---|---|
| `payments-api` | the payment and its event are written atomically, or neither is | [06](docs/static/06-transactional-outbox.md) |
| `outbox-relay` | an unavailable broker becomes a backlog, never a lost event or a failed request | [06](docs/static/06-transactional-outbox.md) |
| `payments.main` | ordering per `payment_id`, parallelism capped by partition count | [01](docs/static/01-write-path.md), [03](docs/static/03-read-path.md) |
| `payments-consumer` | a redelivered record changes nothing on the second pass | [05](docs/static/05-delivery-semantics.md) |
| `payments-consumer.retry.*` | a contended record waits without blocking the partition it came from; the chain belongs to the group, so a second group on `payments.main` gets its own. Run by `payments-consumer` itself: one process, one stage and one consumer group per tier | [07](docs/static/07-retry-dlq.md), [exp-18](experiments/transaction_guarantee/exp-18-retry-chain/) |
| `payments-consumer.dlq` | a dead letter carries enough context to be replayed, not just logged — a class, a reason, where it was first read — and is kept 30 days, long enough to fix, release and replay | [07](docs/static/07-retry-dlq.md) |
| `dlq-replayer` | one class of letters goes back into the group's own first tier, never into `payments.main`, and a second run replays nothing | [runbook](docs/runbook.md#replaying-dead-letters) |
| Schema Registry | an incompatible schema is rejected at registration, not discovered at read: the relay sets its subject to `BACKWARD` before it registers (Apicurio would otherwise start the subject at `NONE`, exp-12). The registry is never in the data path: the producer writes a schema id into the bytes; this consumer reads past it without a lookup, since the message index in the framing names which of the three event messages it is, and protobuf reads each one's compatible versions by field number | [09](docs/static/09-schema-evolution.md) |

Two things this diagram deliberately does **not** contain: a `kafka.Publish()` call
inside the use case, and a `time.Sleep` inside the main consumer. Both are the obvious
implementation, and both are why the two patterns above exist.

## Documentation

**[`docs/static/`](docs/static/)** — the deliverable, and the cheat sheet: it opens with
a one-screen summary of the internals, then one case per file, rendered by GitHub without
running anything. Where a case is a comparison — where the message is lost, which
assignor, which deploy order — the file draws every side of it, because the argument is
the difference between two pictures.

| # | Case | The question it answers | Interactive |
|---|---|---|---|
| [01](docs/static/01-write-path.md) | Write path | What must happen before `Produce` returns success? | [yes](docs/dynamic/write-path/) |
| [02](docs/static/02-log-segments-retention.md) | The log on disk | What is stored, and which "current offset" is meant? | — |
| [03](docs/static/03-read-path.md) | Read path | How does a consumer get a partition and a starting offset? | [yes](docs/dynamic/read-path/) |
| [04](docs/static/04-isr-leader-election.md) | ISR and leader election | What makes an acknowledged record disappear? | — |
| [05](docs/static/05-delivery-semantics.md) | Delivery semantics | Where exactly is a message lost or duplicated? | [yes](docs/dynamic/lost-message/) |
| [06](docs/static/06-transactional-outbox.md) | Transactional outbox | Where does atomicity end between Postgres and Kafka? | [yes](docs/dynamic/outbox/) |
| [07](docs/static/07-retry-dlq.md) | Retry chain and DLQ | Where does a failing message wait, and who is blocked? | [yes](docs/dynamic/retry-dlq/) |
| [08](docs/static/08-rebalance.md) | Eager vs cooperative rebalance | How long does the group stop processing? | [yes](docs/dynamic/rebalance/) |
| [09](docs/static/09-schema-evolution.md) | Schema evolution | Who gets upgraded first, producers or consumers? | — |
| [10](docs/static/10-transactions-eos.md) | Transactions and EOS | What does a Kafka transaction cover, and where does it stop? | — |

**[`docs/00-journal.md`](docs/00-journal.md)** — the lab journal: one entry per run, with
what was expected, what the cluster did and what was surprising. The case files quote its
numbers; it is where they come from. Each run lives in
[`experiments/`](experiments/), grouped by the KR it answers, and is reproducible with
`make exp-NN` whichever group it was filed under.

**[`docs/tuning-checklist.md`](docs/tuning-checklist.md)** — every setting that matters,
what it buys, what it costs, and the defaults where franz-go and the Java client disagree.

**[`docs/k8s/`](docs/k8s/)** — the same service containerised and run on Kubernetes (kind),
with Kafka on Strimzi: optimized images, a Helm chart, and a runbook built on scripted
drills whose logs are committed — rolling update and rollback under load, the HPA, seven
broken releases, a broker pod killed mid-traffic — with the requests and limits of every
long-running container sized from a logged measurement of the running stand (the short-lived
Jobs say where they are not measured).

**[`docs/dynamic/`](docs/dynamic/)** — six of those ten cases as interactive
walkthroughs: open `docs/dynamic/index.html` in a browser and step through with the arrow
keys. Built for learning the mechanics and for presenting them; not a replacement for the
static files.

## The three rules this repository follows

**Evidence, not assertion.** A statement about Kafka behaviour is worth exactly as much
as the run that demonstrates it. Every document ends with the experiments that back it
and the ones still outstanding.

**A caption states a conclusion.** Not "Fig. 3. The consumer", but "Fig. 3. The broker
delivers nothing and assigns nothing". A diagram that needs two sentences is drawing two
things.

**Failure windows are drawn, not described.** Half of these cases exist because
something goes wrong, so the window in which it goes wrong is on the diagram.

## Stack

| Choice | Why |
|---|---|
| Kafka, not a queue in Postgres | the PDP asks for Kafka, and at the rates the drills run (10–20 payments/s; drill 06 found no ceiling at 400) a Postgres table polled with `SKIP LOCKED` — the outbox already is one — would carry the load. What Kafka adds is what a table does not: a replayable log (7 days on `payments.main`) and per-key ordering across partitions, both used here, and independent consumer groups reading the same events at their own pace — which this service, with one group, does not use yet: that is the argument for the day a second reader appears. The price is a three-node quorum to run and the failure modes this repository measures |
| Kafka, not RabbitMQ or NATS | a log a consumer can read again — `cmd/dlq-replayer` and the rebuild of the totals in migration 00003 both did — and per-key order across partitions; a broker queue deletes what it delivers, and NATS core keeps nothing. JetStream or RabbitMQ streams would come closer, at the cost of the Kafka ecosystem this PDP is about |
| franz-go | pure Go, so a static binary in a distroless image (confluent-kafka-go needs cgo and librdkafka); transactions and the idempotent producer, which segmentio/kafka-go lacks; the consumer-group protocols exp-14 compares (eager, cooperative, KIP-848). Its defaults differ from the Java client's — [the tuning checklist](docs/tuning-checklist.md) lists where |
| Protobuf through a schema registry, not JSON or Avro | typed Go code generated from `proto/` (buf), compact records, and a registry that refuses an incompatible schema before it ships (`BACKWARD`, exp-12). Its cost is measured too: proto3 reads a removed or retyped field as zero without any error, where Avro would fail loudly (exp-12e2). What stands between that and a wrong total is the consumer's domain — an amount of zero or a missing currency is refused into the dead letter topic — so every field that matters has a rule that refuses its zero value. Apicurio rather than Confluent's registry for its license ([09](docs/static/09-schema-evolution.md)) |
| Kafka 4.x in KRaft mode, 3 brokers | no ZooKeeper, and a real ISR to break |
| Postgres | outbox and inbox need a transaction, not a cache |
| an inbox, not an idempotent write to the total | a total is a sum: adding the same payment twice changes it, and nothing in the sum says which payments are already in it. Something has to remember every counted event id for as long as that event can come back — that row is the inbox, and it lives in the transaction that adds to the total ([patterns](docs/patterns.md)) |
| payments as the domain | money makes every Kafka failure mode concrete |
