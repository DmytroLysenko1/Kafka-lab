# Decisions and lessons — the Kubernetes stand

What was chosen, what it was chosen over, and what the stand taught that the documentation did
not — the reference behind [the write-up for the team](talk.md). Numbers come from the logs under
`deploy/k8s/drills/results/` and `deploy/docker/results/`; the few observed in the session
without a log are marked as such.

## Choices

| Decision | Over | Why |
|---|---|---|
| **kind**, 1 control plane + 2 workers | minikube, k3d | nodes are Docker containers — the stand already lives in Docker; two workers make pod spreading and cross-node traffic real |
| **Kafka on Strimzi** inside the cluster | Kafka left in docker-compose | the Go services and Kafka share one network and one lifecycle; Strimzi 1.2 runs the same Kafka 4.3.1 as the compose stand, so the broker settings carry over 1:1 (`platform/kafka.yaml`) |
| Strimzi API `kafka.strimzi.io/v1` | `v1beta2` from most tutorials | Strimzi 1.x serves only `v1`; KRaft and node pools are the only mode, ZooKeeper is gone |
| **CloudNativePG** for Postgres | a hand-written StatefulSet, a Bitnami chart | the operator writes the connection secret (`payments-db-app`) — no password in the repository — and handles failover if instances go above 1 |
| **Traefik** as the ingress controller | ingress-nginx | ingress-nginx was retired by Kubernetes SIG Network in March 2026: no more releases, security fixes included. The `Ingress` objects stay standard, so the controller is swappable in one values file |
| **Helm** for the service | Kustomize | the migration has to run *before* the rollout and fail it if it fails — a Helm pre-upgrade hook is exactly that; `helm rollback` and `helm history` are the upgrade/rollback story the PDP asks about |
| one image per command, one Dockerfile | one image with all binaries | 28–38 MB each (see [01-docker.md](01-docker.md)); a component's image changes only when its code does |
| Loki + Alloy for logs | ELK/EFK | one small binary each; Grafana already exists for metrics |
| **kube-prometheus-stack** for metrics | a bare Prometheus with hand-written scrape configs | the Prometheus operator reads `ServiceMonitor`/`PodMonitor` objects, so the chart declares its own scraping next to its Deployments; cAdvisor and kube-state-metrics come with it, and every resource figure on this stand is read from them |

## Decisions inside the chart

- **Readiness only on the API.** It is the only component a Service routes callers to. The
  relay and the consumer have liveness only: taking them "out of rotation" means nothing when
  nobody calls them, and a readiness that tracked Kafka would only add noise.
- **Liveness never checks a dependency.** It answers on the metrics listener without touching
  Postgres or Kafka. Otherwise a database outage restarts every pod at once and turns a
  dependency failure into a crash loop of our own — the probe would do it to the API, which
  stays up and only goes unready.
- **Without Postgres, the relay waits and the consumer stops.** The relay's sweep fails, is
  counted as failed, and the next tick tries again (`TestRunKeepsSweepingAfterAFailedSweep`);
  it never exits for it. The consumer exits when it has a record to count and the database
  does not answer: the error leaves the process, the offset stays where it was, and the
  kubelet restarts it — so nothing is lost or counted twice, and a process that starts over
  reconnects with nothing stale in it. With no records arriving it has nothing to fail on:
  in drill 04f the database was gone for about 100 s, the API went unready and took no
  payments, and neither the consumer nor the relay restarted once; the first payment was
  accepted 4 s after the database pod was ready again and counted 1 s after that
  (`04f-database-outage-20261025-223219.log`; 3 s and 6 s in `…-20260928-210123.log`). A consumer that does meet a record during the
  outage pays the kubelet's back-off — restarts 10 s, 20 s, 40 s apart, up to five minutes —
  on top of it; that case needs records already published while the database is down, and
  the drill does not produce it. Pausing the partitions and polling until the database answers,
  what exp-16 recommends for a slow dependency, would keep the group membership and spare the
  back-off, at the cost of a second recovery path in the code. Nothing in the service
  implements a circuit breaker; where the docs mention one, it is as that recommendation.
- **The consumer exits on a fetch error, except the two the client heals itself.** franz-go
  reports a lost group session (`ErrGroupSession`) and a log truncated under the consumer
  (`ErrDataLoss`) as informational: it rejoins, or resets past the gap, on its own. Exiting for
  them would add the kubelet's back-off to an outage the client was already riding out, so
  both are logged, counted as a failed fetch, and the poll continues — a lost session skips
  that poll's records, whose offsets the group would refuse and which the inbox would drop as
  repeats when they come back. Every other fetch error still ends the process
  (`sortFetchErrors` in `internal/infrastructure/kafka/consumer.go`, and its test).
- **Readiness has its own deadline.** `/readyz` pings Postgres under a 2 s context inside a 3 s
  probe: a hung database reads as "not ready" rather than as a probe timeout. The unit test
  proves both halves: without the deadline the synctest bubble deadlocks, and with a
  deadline at or above the probe's 3 s the fake clock fails the assertion.
- **`maxUnavailable: 0`.** A release that never becomes ready stalls the rollout and leaves the
  old pods serving — drill 04 shows it for every break that stops a pod before readiness
  (a1, a2, b, c, e). It does nothing against one that passes readiness and fails later (d).
- **`preStop.sleep` instead of `exec: sleep`.** Distroless has no `sleep` binary; the native
  sleep action (Kubernetes 1.30+) does the same without a shell.
- **No CPU limit.** Memory is limited (the kernel kills on excess, so the limit is a real
  contract); CPU is only requested. A CPU limit throttles the Go GC while the node has spare
  cores.
- **Consumer: fixed replicas, no HPA, never above 6.** `payments.main` has six partitions; a
  seventh group member owns nothing. CPU is the wrong signal for a consumer group — lag is.
- **Relay: one replica.** Two would publish no record twice — every sweep claims rows with
  `FOR UPDATE SKIP LOCKED` — but they could publish two events of one payment out of order,
  each relay holding one of them. A payment has three event types now — authorized,
  captured, refunded — so that is a real reordering, and one relay is what prevents it;
  scaling the relay out needs per-key ordering in the claim first.
- **The migration is a hook, and rollback skips it.** Migrations only go forward, and a
  rollback runs without the hook. Normally each migration stays readable by the previous
  release (expand and contract), which is what makes `helm rollback` safe for the schema.
  Migration 00003 does not: it moves the totals to `merchant_currency_totals` and drops the
  old table in one step, because no running version still read it. **A rollback to a release
  from before 00003 is therefore refused by the runbook — roll forward instead**: the old
  consumer would find its table gone and stop, which is loud; had the table been kept, it
  would have counted into a table nobody reads, which is silent. The flip side of the hook:
  it is also the image check on upgrade (see lessons).
- **Rollout order: the relay before the API, for a new event type.** The relay encodes every
  outbox row it claims, in id order, and a row it cannot encode fails the sweep — and every
  sweep after it, since skipping the row would lose a money event. So a release that adds an
  event type ships the relay first and the API that writes the type second. The symptom of the
  wrong order: `outbox_backlog_records` growing while the brokers are healthy and
  `outbox_sweeps_total{outcome="failed"}` rising; the fix is the relay release, never an
  `UPDATE outbox SET published_at` by hand.
- **Pod spreading counts one ReplicaSet at a time.** `topologySpreadConstraints` with
  `matchLabelKeys: [pod-template-hash]`: without it a rollout balances the old and the new
  pods together and can leave every new pod on one node — both consumers ended up on one
  worker after a rollout on 27.09. Preferred (`ScheduleAnyway`), not required: the API's HPA
  can ask for five pods on two workers.

## Resources — sized from measurement

Requests and limits were sized twice. The first pass (27.09.2026, night) read what the stand
had used over 2.7 h of drills straight from Prometheus in the session; those figures were
never logged, and a restart of Prometheus — six hours of retention, in memory — erased them.
The second pass is logged: `make k8s-evidence` ran every drill, then `deploy/k8s/resources.sh`
wrote what each container requested and used over that 52-minute window to
`deploy/k8s/drills/results/resources-20260927-175351.log`. A third pass read the later session
log (`resources-20260927-214046.log`) and, for Grafana, Alloy and Prometheus, the whole day
Prometheus had kept — peaks from before that log's window opened. Every request and limit
names the file or query it came from. Prometheus now keeps 24 h on a volume, so a pass
survives a restart.

The rule: memory request ≈ typical working set, memory limit ≈ 1.5–2× the peak, CPU request ≈
typical use except where an HPA takes its percentage of it, no CPU limit. What changed along
the way ("first pass" figures are session observations; "logged" are from the files above):

| Container | Before | Seen | Now | Logged when sized (typical / peak) | 25.10 session (avg / max) |
|---|---|---|---|---|---|
| Grafana | 128Mi / 256Mi | first pass: **OOM-killed 7×**, pinned at 251–255Mi | 336Mi / 832Mi (640Mi until a 544Mi peak read from Prometheus over a day, not logged) | 326 / 413Mi | 327 / 505Mi |
| Kafka broker ×3 | 1Gi / 1Gi | first pass: 760–790Mi, 918Mi when drill 05's admin JVM ran inside | 800Mi / 1400Mi, the admin JVM gets 96m | 801–820 / 813–827Mi; 895Mi peak in a later session (`resources-20260927-214046.log`), 919Mi in the one after, drills 01–06 (`resources-20260928-043718.log`), hence 1400Mi | 655–777 / 731–837Mi |
| Prometheus | 384Mi / 768Mi | first pass: 607Mi and growing | 640Mi / 1152Mi, on a 4Gi volume | 480 / 558Mi, then 587 / 680Mi, then 628 / 701Mi as retention filled | 653 / 842Mi |
| Traefik | 64Mi / 128Mi | — | 64Mi / 192Mi | 41 / 63Mi, then 54 / 117Mi over drills 01–06 (`resources-20260928-043718.log`) | 36 / 141Mi |
| Strimzi operator | 256Mi / 384Mi, CPU limit 1 | first pass: 280Mi, 405 throttled periods; 18 restarts from lost leader elections while the Mac slept | 320Mi / 448Mi, no CPU limit, no leader election | 276 / 283Mi | 271 / 287Mi |
| Apicurio | 384Mi / 768Mi, heap 80% of limit | first pass: ~190Mi | 224Mi / 512Mi, heap 50% of limit, JDBC pool fixed at 20 | 187 / 195Mi, then 211 / 220Mi | 190 / 214Mi |
| Loki, Alloy | 128 / 384Mi, 96 / 192Mi | logged: both above their requests; Alloy 1.39× under its limit | 160 / 384Mi, 112 / 320Mi (224Mi, then 256Mi, until a 208Mi Alloy peak read from Prometheus over a day, not logged) | 162 / 222Mi, 102 / 138Mi; Alloy 170Mi | 107 / 189Mi, 90 / 214Mi |
| relay, consumer | 50m / 128Mi | — | 10–15m / 64Mi | 14–21 / 18–24Mi | 19–21 / 36–37Mi |
| sidecars, kube-state-metrics, config-reloader, prometheus-operator | none | ~270Mi together, invisible to the scheduler | requests set; Loki's rules sidecar removed (no rules) | 20–79Mi each | 13–72 / 45–86Mi |

The last column is `resources-20261025-224859.log`, the current session. Its `oom` column
counts the kubelet's `OOMKilled` reason, which drill 04's case d did not get on 25.10 (runbook,
case d): a 0 there does not rule out an OOM kill, and the restarts column is the one to read.
Every peak is under its limit; the nearest are the prometheus-operator (86 of 96Mi, 90%) and
Prometheus's config-reloader (49 of 64Mi, 77%), then Traefik and Prometheus at 73%. Postgres
peaked at 235 of 384Mi but ran at a CPU p95 of 497m against its 100m request through the write
drills — the request follows its idle use, and the HPA does not read it. The log was not used to
resize anything; the operator's 90% is the first candidate when it is.

**Capacity** (the latest log, `resources-20261025-224859.log`: a 58-minute window, the
~53-minute evidence session of 25.10 and the 5 minutes before it, so it also lists pods left
from the failed attempt before the session). Every kind node reports the whole Docker VM (7934Mi, 10 CPU) as allocatable — the
log lists all three at 8124516Ki — so the scheduler believes in three times the memory that
exists; requests do not protect the VM. Memory requests 5870Mi (74% of the VM); sum of limits
10812Mi (136% — and the kube-apiserver, 1006Mi on average and 1116Mi at peak in that log, has
none). The container working set peaked at 6391Mi (80%) and was 6164Mi at the end. In the
session of 28.09 it peaked at 7423Mi — 95% of the VM — while the session re-applied the
platform and two Grafana pods ran side by side for the rollout (both appear in
`resources-20260928-043718.log`). That left 1.5–1.8 GB at the session's end, and it did not last.
An hour later, after the reruns of drill 06 at up to 400 payments/s, the VM had 69 MiB free
and its 1 GiB of swap full, load average 145 with 70% of the CPU in the kernel
(`vm-memory-20261025-220007.log`); both workers had gone `NotReady` ("PLEG is not healthy"),
and minutes later the scheduler, the CNPG operator and API pods were crash-looping — those two
seen in the session, not logged. What grew is not measured: the resource log's window had
closed. That margin, not the scheduler, is what the stand runs on: it
holds a session, not a session and an hour of load after it, and it is why a third worker was
not added.

## Lab, not production

What this stand leaves out on purpose, so that nothing in it reads as a production claim:

| | On this stand | In production | Why the lab gets away with it |
|---|---|---|---|
| Cluster | kind: one control plane and two workers, all containers on one Docker VM; every node reports the whole VM as allocatable | a managed cluster across three or more zones | one laptop; the drills measure mechanics, not hardware failure |
| Kafka quorum | three dual-role nodes on two workers, anti-affinity only preferred — two of the three controllers can share a worker, and losing that worker loses the quorum (exp-13 shows what a quorum loss looks like) | dedicated controllers, required anti-affinity or rack awareness across zones | two workers cannot hold three brokers apart |
| Postgres | one CloudNativePG instance on local-path storage, no WAL archive, no backups | three instances, WAL archived to object storage, a restore that has been rehearsed | the data is drill traffic; there is nothing to recover |
| Database roles | one role owns both databases and every service connects as it, DDL included | a migration role with DDL, an application role with DML only, a role of its own for Apicurio | one secret written by the operator, no role management to maintain |
| Transport | Kafka plaintext without SASL or ACLs; HTTP without TLS; any pod in the cluster could write to `payments.main` | TLS or mTLS everywhere, SASL with ACLs so only the relay writes the topic | loopback-only access; the consumer's refusals (message index, `event_type`, domain checks) are the only guard |
| Network | no NetworkPolicy | default deny, with the few flows the services need allowed | same |
| API authentication | one shared API key | a key per client or OAuth, with rotation | README says so where the key is introduced |
| Observability access | Grafana anonymous with the Editor role, Loki without authentication | single sign-on, viewers by default | loopback only |
| Alerting | Alertmanager off, no alert rules — "on call" here is the dashboard | rules that page: the age of the oldest unreplayed dead letter, `outbox_backlog_records` rising while `published/s` is zero, consumer restarts, lag growing over minutes, a failed `prune` Job | named here and in `deploy/topics/README.md`, not built |
| PodSecurity | `restricted` warns and audits, does not enforce: Strimzi's pods do not meet it | enforce, with the operator's pods exempted by namespace | the payments pods meet it (drill output), the platform's do not |
| Images | built locally and loaded with `kind load`, no registry, no signature | a registry, signed images, an SBOM | nothing leaves the machine |
| Storage | local-path volumes, `deleteClaim: true` | a CSI driver with quotas, claims that outlive the cluster | `make k8s-down` should leave nothing behind |
| A lost database | the API goes unready, the relay retries every tick, the consumer exits on the first record it cannot count and is restarted | a breaker that pauses the partitions instead of the restart | a decision, above; drill 04f measured 3–4 s from the database pod ready to the first accepted payment, 5–6 s to it counted, over two runs |
| Platform upgrades | Kafka, Strimzi and CloudNativePG versions pinned, never upgraded on the stand: not rehearsed | a written procedure; Strimzi rolls brokers one at a time within `min.insync.replicas`, CNPG switches over to an upgraded replica | nothing on the stand needed it; say "not rehearsed" rather than guess |
| Restoring Postgres from a backup | not rehearsed; there is no backup | restore, then move the consumer group to the restore point before starting it | `event_id` is `outbox.id`, and payments, outbox, inbox and totals live in one database, so a restore rolls all four back together and new events reuse ids from the restore point on. The inbox stays consistent with the totals; what falls out is the group's offset, which still points past events the restore erased. Kafka offsets and the database must be restored as a pair, never one alone |
| Kafka disaster recovery | none: a lost cluster loses the topics' 7 and 30 days | MirrorMaker 2 or tiered storage to object storage, by the RPO | Postgres is the system of record; the outbox can republish what it still holds (7 days) |
| Consumer scaling | fixed replicas, never above the six partitions | KEDA scaling on lag | lag, not CPU, is the signal (above); a second operator was not worth it here |
| Reconciliation | none: nothing compares `payments` with `merchant_currency_totals`; a divergence would go unseen | a periodic check that the sum of authorised payments per merchant and currency equals the total, alerting on any difference | the totals are a projection the lab can rebuild, as migration 00003 did |
| A consumer down for over a week | its events leave `payments.main` with the 7-day retention, and the outbox rows that could republish them are pruned after 7 days: those payments are never counted | lag alerting long before that, and totals rebuildable from `payments` | the drills keep the consumer running; the week is written down in `deploy/topics/README.md` |

## Lessons the stand taught

- **Host port 80 did not work on this Docker Desktop.** The kind control plane published
  `127.0.0.1:80 → 30080`; `curl` connected and was reset. tcpdump on the node showed Docker's
  forwarder opening a connection to the NodePort and closing it without sending a byte. The
  same NodePort answered 200 from inside the Docker VM (`--network host`), from a container on
  the `kind` network, and through a relay on host port 8088; a plain container published on
  host port 443 also worked. Moving Traefik onto the control-plane node (to rule out the
  cross-node hop) changed nothing. The cause stayed unexplained; the stand uses host port 8800,
  and `kind.yaml` says why.
- **A zsh loop split nothing.** `set -- $t` does not word-split in zsh, so the first generated
  `topics.yaml` had topic names like `payments.main 604800000 CreateTime`. The API server
  rejected them (RFC 1123), which is the cheap place to find out. Generators run under `bash`.
- **Helm chart schemas are strict now.** Traefik's chart refused `logs:` (renamed to `log:` and
  `accessLog:`) — caught by rendering every values file with `helm template` before the first
  install, not by the install.
- **The migration hook is a release gate.** A tag that was never built reached the `migrate`
  Job first; the hook could not complete and the release failed with not one Deployment
  touched. Putting the migration in the chart bought a free pre-flight check of the image.
- **`helm rollback` without a revision goes to current−1** — after two failed upgrades in a row
  that is a failed one, and the drill rolled a never-built tag onto all three Deployments
  that way, past the migration hook that had stopped the same tag on upgrade. A remembered
  revision number is not safe either: `--history-max 10` pruned it. Restoring to known-good
  now means rolling forward to the chart in git.
- **A green rollout can ship a lethal memory limit.** With 8Mi the API was OOM-killed at
  start, then idled under the limit long enough to pass readiness and replace a working pod
  (on 28.09 the rollout completed). The traces were the restart count and `lastState` — exit
  137, whose reason read `OOMKilled` on 28.09 and `Error` on 25.10, when the kernel's log
  shows it was the OOM killer all the same (runbook, case d). The resource log's `oom`
  column counts the `OOMKilled` reason, so it shows those pods with 0 OOMs and 2 restarts.
- **Readiness did exactly its job in a database outage — and a third-party liveness did not.**
  API pods unready, zero restarts, ingress 503 instead of hanging requests, back by
  themselves when the database was. Apicurio, whose `/health/live` includes its datasource,
  was restarted by its liveness probe in the same outage; it now has a TCP liveness. Checking
  our own probes was not enough — every probe in the namespace needed the same question.
- **`*.localtest.me` also resolves to `::1`.** Under 1200 req/s the load generator sometimes
  dialled IPv6, where nothing was published: 0.66% of requests failed client-side. Traffic
  that still goes through the ingress goes to `127.0.0.1` with a `Host` header.
- **A load sent through the laptop's port forward measures the port forward.** Four attempts
  at the 25.10 session ended early, two during drill 03's load from the host; in the one whose
  Docker logs survive, Docker Desktop's port forward shut down 11 s into 800 req/s and the VM
  stopped a minute later.
- **A load sent straight to a Service measures its connections.** Moved into the cluster and
  pointed at the API's Service, vegeta's keep-alive connections were each pinned to one pod by
  kube-proxy: one pod at 191m while another sat at 19m (`api-cpu-per-pod-20261025-220313.log`),
  and the HPA's average described neither. Traefik picks a pod per request, so the heavy drills
  now send from the pod through Traefik's Service, the path every earlier run took (runbook,
  Scale).
- **Load that feels high may not move an HPA.** 400 req/s from the host kept the API at 51–62%
  of its request (one sample at 70%), at or below the 70% target, so the desired count stayed
  2; 1200 req/s scaled it out. Plan the load from the request, not from a round number.
- **A broker pod that dies comes back.** Compose kept a killed broker dead (`restart: "no"`, on
  purpose); Strimzi recreated it and under-replicated partitions were back to 0 within 11–19 s
  over five runs — before the pod reported Ready (22–25 s) — with all 2400 payments of each run
  accounted for.
- **A kubectl edit of a Helm release is a delayed failure.** Helm 4 uses server-side apply,
  so `kubectl set image` / `rollout undo` (drill 04, a2) made kubectl the owner of the image
  field. Nothing broke while every release reused the `dev` tag; the first upgrade to a
  commit tag failed with a field-manager conflict. The drill now removes kubectl's entries
  from `managedFields` after its undo, so Helm is the image field's only owner again; see the
  co-ownership lesson below for why `--force-conflicts` in the restore step was not enough.
- **A reused image tag hides both of the above.** With `dev` rebuilt in place, an upgrade
  changed no field, so it neither rolled pods nor surfaced the conflict. Tags are now the
  commit, and the chart refuses to render without one.
- **`--reuse-values` does not see a chart's new defaults.** It replays the last release's
  values; a key the chart added since is simply absent, and the first upgrade that needed one
  died rendering the template. `--reset-then-reuse-values` applies the chart's defaults first.
- **Server-side apply co-ownership outlives `--force-conflicts`.** When kubectl and Helm set the
  same value both own the field, a forced apply of that value moves nothing, and the conflict
  surfaces at the next change. Resetting `managedFields` to `[{}]` only hands the fields to
  `before-first-apply`; removing kubectl's own entries is what leaves Helm the sole owner
  (checked on a scratch Deployment).
- **A projection keyed wrongly is fixed by rebuilding it, not by altering it.** The totals had
  no currency to split by; the new table was rebuilt from outbox ⋈ inbox — exactly what the
  consumer had counted — with the consumer stopped, and the migration log shows zero merchants
  whose old and new totals differ.
