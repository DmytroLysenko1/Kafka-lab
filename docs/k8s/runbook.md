# Runbook — the payments service on Kubernetes

Deploy, upgrade, roll back, scale, and find out what is wrong. The symptoms and numbers below
were recorded by drill scripts (`make k8s-drill-NN`, sources in `deploy/k8s/drills/`) with the
raw output committed under `deploy/k8s/drills/results/`; the few that come from watching the
session instead are marked as such where they appear. Stand: kind with 3 nodes, Docker
Desktop with 8 GB (7934Mi to the VM, 10 CPU). The current evidence set was produced in one run
by `make k8s-evidence` on 26.10.2026 (01:48–02:45Z, images `05edc89`, the Mac kept awake by
caffeinate), on a cluster recreated from nothing a few hours before: the one before it, 28 days
old, had run the VM out of memory (Capacity, decisions.md). The session measures the
Dockerfiles first, with the kind nodes stopped, then brings the stand back and runs every drill.

It took ten attempts over two days. The ones that failed, and what each changed:
- 25.10, four attempts: drill 03 sent its load from the host, and Docker Desktop's port forward
  — and with it the VM — went down under it (Scale). The heavy drills moved into the cluster.
- 25.10, the fifth ran through, but its drills 03 and 06 sent their load straight to the API's
  Service, which pinned each connection to one pod and skewed their CPU and HPA figures
  (Scale); they now send through Traefik. Its logs are kept as that lesson's evidence.
- 26.10, the sixth stopped at drill 04, whose broken address clashed with a chart change
  (fixed); the seventh at drill 03, its vegeta OOM-killed at a limit set too low (raised) while
  the month-old cluster ran the VM out of memory (recreated).
- 26.10, the eighth ran through on the new cluster, and its logs were set aside: drill 07 summed
  its counters across a scale-in (fixed), Traefik peaked at 98% of its memory limit (resized),
  and one rebuild per Dockerfile next to the stand was too noisy to compare (now three, with
  the nodes stopped). Its drill 06 is the one that found a ceiling (Scale).
- 26.10, the ninth ran the VM out of memory and swap while measuring the Dockerfiles next to the
  stand, and its drill 01 timed out. The partial logs of the sixth to ninth were deleted.

Logs of earlier sessions are kept where a lesson below comes from them, and each says which
script version made it.

## On one page

The commands, in the order an operator needs them. Everything after this section is the
evidence and the reasons behind them.

| Task | Do | Then check | Details |
|---|---|---|---|
| Deploy | `make k8s-images && make k8s-deploy`, then `helm test payments -n lab` | `kubectl get pods -n lab` — every pod Ready, `RESTARTS` 0 | [Deploy](#deploy) |
| Upgrade | the same two `make` targets with the new code checked out | `RESTARTS` after the rollout, not only `READY` — a broken release can pass readiness (case d) | [Upgrade](#upgrade--rolling-update) |
| Roll back | `helm history payments -n lab`, then `helm rollback payments <REVISION> -n lab --wait` | the revision's DESCRIPTION names its image; never a bare `helm rollback` | [Roll back](#roll-back) |
| Something is wrong | `kubectl get pods`, `describe pod` → Events, `logs --previous`, `get events --sort-by=.lastTimestamp`, the EndpointSlice | match the symptom to the table of seven broken releases | [Troubleshooting](#troubleshooting--seven-broken-releases), [Where to look first](#where-to-look-first) |
| Is it logging and being scraped? | `make k8s-drill-07` | every pod `up` 1, the counters move by what was sent, every pod's start line in Loki | [Logs and metrics](#logs-and-metrics-shown-working) |
| A distroless pod to look inside | `kubectl debug -it -n lab <pod> --image=busybox:1.37 --target=<container> -- sh` | `kubectl exec … sh` cannot work: there is no shell | [Troubleshooting](#troubleshooting--seven-broken-releases), case g |
| A broker pod lost | nothing at first: Strimzi brings the pod back | under-replicated partitions back to 0 within 11–19 s and the pod Ready in 22–25 s (drill 05, six runs) | [Kafka broker pod lost](#kafka-broker-pod-lost) |
| Dead letters to replay | `make k8s-replay CLASS=exhausted` (or another class) | the merchant totals once the replayed letters are consumed | [Replaying dead letters](#replaying-dead-letters) |

## Deploy

```
make k8s-up          # once: cluster + platform (roughly 10 min from nothing, most of it image pulls; not logged)
make k8s-images      # build and load the images, tagged with the commit (+ a hash of uncommitted changes)
make k8s-deploy      # lint + kubeconform + API key secret + helm upgrade --install --wait
helm test payments -n lab
```

Order inside a release: the `migrate` Job runs first (pre-install/pre-upgrade hook); only
when it completes does Helm apply the Deployments. A migration that fails fails the release
with nothing replaced.

## Upgrade — rolling update

```
make k8s-images                        # tagged with the commit (+ a hash of uncommitted changes)
make k8s-deploy                        # helm upgrade --install with that tag
```

A tag that is already deployed changes no pod template and replaces no pod: `make k8s-images`
refuses nothing, but drill 01 refuses to "roll" to the deployed tag. To run it on its own,
give it a second tag of the same build, loaded into kind (what `session.sh` does):

```
IMAGE_TAG=$(helm get values payments -n lab -o json | jq -r .image.tag)   # the deployed tag
for c in payments-api outbox-relay payments-consumer migrate dlq-replayer prune; do
  docker tag kafka-lab/$c:$IMAGE_TAG kafka-lab/$c:$IMAGE_TAG-next
  kind load docker-image kafka-lab/$c:$IMAGE_TAG-next --name kafka-lab
done
IMAGE_TAG_NEXT=$IMAGE_TAG-next make k8s-drill-01
```

**A release that adds an event type goes out in two steps: the relay first, then the API that
writes the type.** The relay claims outbox rows in id order, and a row it cannot encode fails
every sweep from then on, since skipping it would lose a money event (decisions.md, rollout
order). Shipping both at once in one `make k8s-deploy` leaves a window in which the new API
writes rows the old relay cannot encode; the symptom is `outbox_backlog_records` rising while
the brokers are healthy and `outbox_sweeps_total{outcome="failed"}` climbing, and the fix is the
relay release, never an `UPDATE outbox SET published_at` by hand (docs/runbook.md). Argued from
the code; no drill stages it.

The tag is read from the release, not computed: `make`'s `IMAGE_TAG` hashes the working tree,
and after any change — a new drill log is enough — it names a tag that was never built. Drill
04 and `make k8s-replay` read the deployed tag the same way.

**Drill 01** (`01-rolling-update-20261026-040102.log`): 20 payments/s for 90 s through the
ingress, upgrade from `<tag>` to `<tag>-next` fifteen seconds in. `-next` is the same build
under another name, so the drill measures the rollout mechanics — every pod replaced under
traffic — not a change of behaviour between versions.

| | |
|---|---|
| requests / non-2xx | **1800 / 0** — every payment answered 201 |
| `helm upgrade --wait` returned after | **11 s**, migration hook included (16, 17 and 16 s in the sessions of 25.10, 28.09 and 27.09) |
| p99 latency over the whole 90 s run | 29.8 ms (max 89 ms); 75.2 ms on 25.10, 17.8 ms on 28.09, 8.0 ms on 27.09 |
| API pods by node, before → after | one on each worker → one on each worker |

The relay runs with `strategy: Recreate` now. The first run of this drill, before that
change and with the tags still `dev`/`v2` (`01-rolling-update-20260927-012247.log`, 1800 / 0,
10 s), lists two relay pods side by side after the rollout — the overlap `Recreate` removes;
the current run lists one.

The tail of the light drills moves from session to session at the same 20 payments/s through
the same ingress. p99 in drills 01, 02 and 05: 8–18, 7–14 and 9–14 ms in September; 75, 89 and
242 ms on 25.10, on the month-old cluster that ran out of memory that night; 30, 30 and 298 ms
on 26.10, on the new one. Nothing in the logs says why, and drill 05's tail stayed long.

The node column shows the spreading of decisions.md: one API pod per worker before and after.
It counts one ReplicaSet at a time, so it can also leave both on one worker — the 25.10 run
started with both API pods on `kafka-lab-worker2` (`01-rolling-update-20261025-220905.log`)
and ended with one on each.

The settings meant to make it zero: `maxUnavailable: 0` (an old pod leaves only after a new
one is ready), the readiness probe (a new pod gets traffic only once it reaches Postgres),
and `preStop.sleep: 5` (an old pod stays reachable while the routers drop it, then gets 20 s
to finish what it holds). The drill shows the result, not which of the three carried it — a
control run with `preStopSleepSeconds=0` would. Worth knowing: when `helm --wait` returned,
both old API pods were still listed, and still `READY true`, next to the two new ones (the
current log and the first run; one old pod on 25.10) — "release deployed" is not "old pods
gone"; that they were in their preStop pause is an inference.

## Roll back

```
helm history payments -n lab                        # DESCRIPTION names each revision's image
helm rollback payments <REVISION> -n lab --wait     # always with an explicit revision
```

The chart's version and app version are the same in every revision (`payments-0.1.0`, `dev`),
so `helm history` tells revisions apart by their description: `make k8s-deploy` and the
drills write the image tag they rolled out there (`image <tag>`), and drill 04's broken
releases say so. Revisions made before that — every one in the drill logs of 27–28.09 —
show helm's default "Upgrade complete" instead.

**Drill 02** (`02-rollback-20261026-040234.log`): rollback from drill 01's `-next` release to
the revision before it, found and named by number (17), under 20 payments/s.
**1200 / 0** non-2xx, rollback took **6 s** (6, 6 and 7 s in the sessions of 25.10, 28.09 and 27.09), and the migration Job kept its creation time:
rollback runs no pre-upgrade hook. So migrations must be backward compatible (expand, then
contract in a later release) — a rollback never undoes a schema change.

**Never roll back past migration 00003.** It moved the totals to `merchant_currency_totals`
and dropped the old table in one step (docs/k8s/decisions.md): a release from before it would
find its table gone. Fix forward instead — `make k8s-deploy` with a corrected build.
This rule is argued, not drilled: every commit from before 00003 — the last is `49443da` —
has two migrations and no `/readyz`, which the chart's readiness probe asks for, so none of
them can run under this chart, and no rollback across the schema change can be staged.

Two traps the drills walked into, both recorded:

1. **A bare `helm rollback` can roll out a failed revision.** Without a number it goes to
   the revision before the current one. After *one* failed upgrade that is the last good
   release; after two failed upgrades in a row it is the first failure. That is what happened:
   the first drill run (`04-broken-deployments-20260927-014639.log`) stopped right after its
   upgrade failed, leaving a failed revision behind; the second
   (`04-broken-deployments-20260927-015223.log`) failed another upgrade and "restored" with a
   bare rollback — onto the first failure. The broken tag went to all three Deployments
   (`Updated: 1/2`, `Pending termination`), **past the migration hook**, because a rollback runs
   no pre-upgrade hook: the gate that stopped the tag on upgrade (a1 below) does not exist on
   rollback. The old pods kept serving only because of `maxUnavailable: 0`. Read
   `helm history` and roll back to a revision you know was good, by number.
2. **A revision number can disappear.** Helm keeps 10 revisions (`--history-max`). A drill
   that recorded the good revision at its start and came back to it after five break/restore
   cycles got `Error: release has no 13 version` — the revision had been pruned. (The log of
   that run was deleted by mistake while cleaning up; the error is quoted from the session
   output.) The drills now restore by rolling *forward* to the chart in git:
   `helm upgrade payments deploy/helm/payments -n lab --reset-values --set image.tag=<tag> --force-conflicts`.

`kubectl rollout undo deploy/<name>` also exists, but it leaves a trap. Helm 4 applies
manifests with server-side apply, where every field has an owner; `kubectl set image` and
`rollout undo` make **kubectl** the owner of the image field. Nothing shows it while the
values agree — the drills kept upgrading fine with the same `dev` tag — and the first
upgrade that changes the image fails:
`conflict with "kubectl" using apps/v1: .spec.template.spec.containers[name="payments-api"].image`
(hit on 27.09.2026 when the tag moved to the commit; the failed revision is kept in
`helm-history-20260927-170131.log`). Drill 04's case a2 now prints the owners right after
the undo — `helm Apply`, `kube-controller-manager Update`, `kubectl Update`
(`04-broken-deployments-20261026-041347.log`). `kubectl get deploy … --show-managed-fields`
shows the owners (`kubectl Update` next to `helm Apply`).

`--force-conflicts` alone does not end it. When kubectl and Helm set the same value — as
after `rollout undo` back to the release's image — they **co-own** the field, and a forced
apply of that same value has no conflict to resolve, so kubectl stays an owner. The trap
fires at the next image change: drill 04's own restore used `--force-conflicts`, and the
first roll forward afterwards still failed on the image field
(`migrate-totals-20260927-210047.log`). Checked on a scratch Deployment: after a forced
apply of the same value the owners are `helm,kubectl-set`; resetting `managedFields` to
`[{}]` hands every field to `before-first-apply`, which conflicts just the same. What works:
- remove kubectl's own entries from `metadata.managedFields` with a JSON patch (drill 04 now
  does this after `rollout undo`) — Helm is left the only owner, and the next image change
  applies cleanly;
- or `helm upgrade … --force-conflicts` on the upgrade that actually changes the value — the
  way out on 27.09, when the totals migration below ran after the drills.

Neither as a default flag — the conflict is the only signal that someone changed the release
behind Helm's back.

## Moving to per-currency totals (27.09.2026, history)

The merchant total used to add every currency into one number. On 27.09 the live stand moved
to `merchant_currency_totals`, keyed by merchant and currency, rebuilt exactly from the events
the consumer had counted — outbox rows joined to the inbox by event id — with the consumer
stopped, because an event counted by an old consumer after the rebuild would have landed in the
old table only. The comparison of every merchant's old total with the sum of its new ones
(`migrate-totals-20260927-210149.log`): 10 merchants and 1,741,999 minor units before, 10
merchants in 10 merchant-currency rows and 1,741,999 minor units after, **no differences**;
the consumer was stopped for 76–123 s: from 18:00:47 (the second run's log) or 18:00:00 (the
first's) to 18:02:03 (the third's) — no log line shows it started again after the first,
failed run. Before the work was committed, that migration and the one after it were folded into
a single `00003` that also drops the old table, and the stands were rebuilt from the new chain;
the script that ran the move went with it, as nothing is left for it to move.

The two runs before the successful one failed, and each left a lesson in its log:
- `migrate-totals-20260927-210000.log` — `helm upgrade --reuse-values` replays the last
  release's values and ignores defaults the chart gained since, so the new CronJob rendered
  with no `.Values.prune` at all. An upgrade that must keep a release's own values uses
  `--reset-then-reuse-values`; the drills' restores use `--reset-values`, which goes back to
  the chart's defaults on purpose. Nothing reached the cluster; the error is at render time.
- `migrate-totals-20260927-210047.log` — the image-field conflict described under
  "Roll back". The pre-upgrade hook runs *before* any object is applied, so this run had
  already migrated the schema with the consumer stopped; rerunning it was safe.

## Housekeeping — prune

The `prune` CronJob (hourly, at :17) deletes outbox rows published more than 7 days ago and
inbox rows counted more than 40 days ago, in batches of 5,000, each its own statement. An
unpublished outbox row is never deleted. The inbox horizon cannot be set below 40 days
(`internal/domain/retention`): an event can come back 39 days after it was counted — 30 days
in the DLQ, then 7 in a retry tier after a late replay, each plus one daily segment, because
Kafka deletes a segment only once its newest record is past retention — and an inbox that forgot it would count
the payment again; the fortieth day is for the clocks of the API, the consumer and prune not
agreeing. An event older than 39 days is refused by the consumer rather than counted. Run it by hand with `kubectl create job --from=cronjob/prune prune-now -n lab`.
A run gets no retries (`backoffLimit: 0`): the next hourly run is the retry, and every batch a
failed run deleted stays deleted. A failed Job — the database was down, or ten minutes were not
enough — stays listed with its log (the last three are kept) for whoever looks next.

## Connection budget

Postgres accepts 97 connections (100 minus 3 for superusers). Each component caps its pool
(`DB_MAX_CONNS`); the sum at the HPA's maximum is 61, itemised in
`deploy/helm/payments/values.yaml`. Readiness pings through a one-connection pool of its own,
so a pod whose working pool is busy does not read as not ready — a ping through the working
pool would wait behind the busy connections and fail readiness on every pod at once, since they
are busy together (`TestPingAnswersWhileEveryWorkingConnectionIsHeld` holds every working
connection and still gets an answer).
Raising a replica count or a pool size means redoing that sum.

## Scale

**Drill 03**, HPA on the API (CPU target 70% of the 100m request, 2..5 replicas). The script
defaults to 400 req/s; the evidence session runs it as `RATE=800 make k8s-drill-03`, sent from
a pod in the cluster (`kafka-lab/vegeta:12.13.0`) through Traefik's Service with the Ingress's
`Host` — the path a caller's request takes, past the host's port forward. The September rows
were sent from the host through the same Traefik; the 25.10 row from the pod straight to the
API's Service, which skewed it (below).

| Load (GET, 180 s) | Result | Log |
|---|---|---|
| 800 req/s, in the cluster through Traefik, 26.10 — **current** | 164% at t=41 s → desired 4; 167% at t=51 s → 5 (max), 4 running, all 5 at t=72 s; then 70–94% on 5 pods, above the target at the HPA's maximum; **144000 / 0** non-2xx, p99 407 ms, max 3.0 s | `03-autoscaling-20261026-040335.log` |
| 800 req/s, in the cluster to the Service, 25.10 — **skewed**, see below | 127% at t=31 s → desired 4, 4 running at t=41 s; 104% at t=62 s → 5 (max), all 5 at t=72 s; then 61–103% on 5 pods; 144000 / 0 non-2xx, p99 15.6 ms, max 374 ms | `03-autoscaling-20261025-221137.log` |
| 400 req/s, host, 27.09 | 51–62% most of the run, one sample at **70%** (t=91 s) — at or below the target, so the desired count stays 2: **no scale-out** | `03-autoscaling-20260927-012531.log` (an earlier script version, same load logic, and the single-total read path from before per-currency totals — the answer was 51 bytes, 42 now) |
| 1200 req/s, host, 27.09 | 109% at t=30 s → desired 4; 160% at t=40 s → 5, all 5 running at t=61 s; settled at 64–66% on 5 pods; 216000 / 0 | `03-autoscaling-20260927-211420.log` |
| 1200 req/s, host, 28.09, in that day's evidence session | 138% at t=41 s → desired 4; 182% at t=51 s → 5, all 5 at t=71 s; then 88% twice and 72–78%, 97% in the last two samples of the load; 216000 / 0, but p99 **182 ms**, max 1.44 s | `03-autoscaling-20260928-040732.log` |
| 1200 req/s, host, 28.09, rerun on a settled stand | 134% at t=30 s → 4; 5 at t=91 s; then 60–67%; 216000 / 0, p99 3.3 ms, max 45 ms | `03-autoscaling-20260928-044933.log` |

Scale-in in the current run: the load ended at t≈180 s and CPU was at 8% by t=226 s; desired
went to 4 at t=504 s and to 2 at **t=514 s**, pods at t=535 s — the 300 s downscale
stabilization window, as documented. Read back from Prometheus (`api-cpu-per-pod-20261026-024559.log`), the five pods were
at 61–80m at one moment and 80–94m at the next: Traefik spread the load evenly, so the HPA's
average is each pod's. Five pods at 70–94% of 100m for 800 req/s is some 160 req/s per pod at
about 80% of its request — more CPU per request than September's 240 req/s at 64–66%, on a
different cluster and day; why is not measured. The p99 of 407 ms against September's 3.3–182
ms suggests the same: at its maximum the HPA had no room left for this load on this stand.

**The 25.10 row measures connection skew, not the API.** vegeta keeps its connections open,
and a connection to a Service is pinned to one pod by kube-proxy when it opens; Traefik, on
the earlier rows' path, picks a pod per request. Read back from Prometheus afterwards
(`api-cpu-per-pod-20261025-220313.log`), the five API pods during that run were at 191, 73, 37, 37 and 19m at one moment
and 177, 66, 27, 23 and 23m at the next — one pod at nearly twice its 100m request while
others idled; the HPA's percentage is their average. So the row says how the HPA reacts and
that every request was answered, not what a pod can take. The 27.09 rows give 240 req/s per
pod at 64–66% of the request (1200 req/s on five pods) — some 350–400 req/s per full request
if it scaled linearly, which was not measured. Neither is a capacity test.

**Sending the load from the host took the stand down.** On 25.10 the session was started five
times, and four ended early: the first when kubectl lost the API server (`TLS handshake
timeout`) during drill 03 at 1200 req/s; the second at its start, Docker Desktop answering 500;
the third in drill 04, the cluster unreachable and Docker Desktop down after drill 03 at 1200
req/s; the fourth during drill 03 at 800 req/s, Docker Desktop down again. Docker's own logs
of the last of them (`~/Library/Containers/com.docker.docker/Data/log/`, not committed) show
what failed: 11 s into the load, its host-side port forward — which carries `127.0.0.1:8800`
and the kind API server's port alike — logged `connection multiplexer has shutdown`; kubectl
lost the API server (`TLS handshake timeout`), and the VM stopped 72 s later. That attempt's
own drill 03 report — in its partial log, deleted with the others, so quoted here from the
session's output — had 137102 of the 144000 requests with no answer at all (vegeta's status
0). Docker's logs of the earlier failures were no longer there, so their cause is not shown.
A load that measures the port forward says nothing about the API: drills 03 and 06 now send
from inside the cluster (`deploy/k8s/drills/lib.sh`), and the session that followed ran
through.

The two runs of 28.09 are the same code on the same stand. In that evidence session drill 03
ran minutes after the session re-applied the platform — Prometheus and Grafana rolling, the
container working set peaking at 95% of the VM (`resources-20260928-043718.log`) — and the
five pods needed 88% of their request twice and then 72–78% for the same load, with a p99 some 55 times longer
(182 ms against 3.3 ms). Rerun after the stand had settled, it gave the numbers of 27.09 again.
That the busy stand caused the difference is an inference from the timing: nothing measured
what the pods got from the machine during the run. Every request succeeded in all three. A
rerun started straight after drill 06, with no cool-down, stopped at t=93 s when the host ran
out of ephemeral ports (`03-autoscaling-20260928-043833.log` ends there; kubectl's "can't
assign requested address" is from the session's output, not in the log) — a limit of the load
generator's machine, not of the service.

The first 1200 req/s run (`03-autoscaling-20260927-013557.log`) shows 1427 failures (0.66%),
all `dial tcp … [::1]:8800: connection refused`: `payments.localtest.me` also resolves to
`::1`, where nothing is published, and the load generator dialled it now and then. Not a
service failure; the light drills that still go through the ingress dial `127.0.0.1:8800`
with a `Host` header.

**Drill 06** (`06-write-throughput-20261026-043013.log`), the write path end to end: POST
/payments at rising rates, 30 s a step and 120 s for the last, spread over fifty merchants (one
merchant would measure its row lock, which is exp-18's subject), then how far the outbox fell
behind and how long until every stored payment was in its merchant's total. Sent from a pod in
the cluster through Traefik, as drill 03. One relay, two consumers; the API starts at two pods
and the HPA may add more.

| payments/s | accepted / stored | p50 / p99 / max | outbox left at the step's end | drained after | all counted after | samples after the end |
|---|---|---|---|---|---|---|
| 50 | 1500 / 1500 | 2 / 70 / 205 ms | 0 | 0.5 s | 0.5 s | 1 |
| 100 | 3000 / 3000 | 2 / 727 / 1111 ms | 43 | 0.7 s | 0.7 s | 2, 0.22 s apart |
| 200 | 6000 / 6000 | 2 / 285 / 1305 ms | 0 | 2.3 s | 2.3 s | 1 |
| 400, held 120 s | 48000 / 48000 | 5 / 1157 / 2519 ms | 0 | 1.8 s | 2.4 s | 2, 0.62 s apart |

Every sample is stamped by Postgres's clock and the step's end comes from vegeta's report —
the kind nodes' one clock — so "after" is a difference on one clock. Each figure is the first
sample that shows the step drained or counted: where that was the first sample of all, the
step was done before it, and the figure is only an upper bound set by when sampling began
(a second after the step's nominal end, plus a `kubectl exec`). The steps are not back to back
(`step_t`): the drill writes each step's targets before starting it, one `base64` per payment,
and the 48000 of the last step put 246 s between the end of the 200/s step and its start — an
idle stretch in which the HPA's readings fall back.

No ceiling within 400 payments/s in this run: every payment sent was accepted, stored and
counted within 2.4 s of its step's end. The API did reach its own: at 400/s it read 156% on
three pods at t=432 s (three since the 200/s step, which read 104% at t=154 s), ran five from
t=442 s at 89–143%, and the tail went to a p99 of 1.2 s. The eighth attempt of 26.10, the same
drill on the new cluster with Traefik at its old limit, did find a ceiling at 400/s (seen in
the session; its logs were set aside): five pods at 100–109%, p99 9.5 s, vegeta able to send
only 44405 of the 48000 payments in the step, and 555 answered 503 — every one, by the API's
own log in Loki, a `postgres.WithinTx begin` that hit its deadline, answered as
`storage_unavailable` with "repeat the request" (`internal/interfaces/http/errors.go`) and
stored nothing: accepted and stored were both 43850. So 400 payments/s is the edge of this
stand, on one side or the other of it from run to run.

The session of 28.09 sent the same steps from the host, through Traefik, 30 s each including
the last: 12000 / 12000 at 400/s, p99 17 ms, 137 outbox rows left, drained and counted in 2 s,
and the API stayed at two pods at 60% (`06-write-throughput-20260928-043357.log`). The 25.10
run (`06-write-throughput-20261025-231555.log`) went straight to the Service and is skewed like
that day's drill 03; its write-path figures (every payment accepted, stored and counted within
4 s) stand.

What gave first was the API, not the pipeline behind it: the relay and the consumers drained
and counted every step within seconds even at 400/s, while the API's five pods ran past their
request and, in the eighth attempt, its transactions timed out waiting to begin. Whether that
was the API's CPU, the database's (Postgres's p95 was 489m against a 100m request in this
session's resource log) or the VM's — the events of that attempt, seen in the session, showed the whole stand stalling
at once, probes timing out on every component — the drill does not separate. On a bigger
machine the next limit would be the single relay — one batch of 100 at a time, each waiting
for the `acks=all` of all three brokers — a reasoned guess, not a measurement; raising
`OUTBOX_BATCH` toward `MaxClaim` (1000) is its first lever, relays split by key the second
(decisions.md, relay). For the relay, one replica by design: a full batch of 100 is followed at
once by the next sweep, so it drains in batches, not at one batch a second.

The consumer is not autoscaled on purpose: six partitions, and CPU is not what limits a
consumer group. Scale it through the release, never above 6:
`helm upgrade payments deploy/helm/payments -n lab --reset-then-reuse-values --set components.payments-consumer.replicas=3`.
Not `kubectl scale`: it makes kubectl the owner of `.spec.replicas`, the same trap as
`kubectl set image` (Roll back, above). The next `make k8s-deploy` sets only the image tag, so it takes the
count back to the chart's 2; a lasting change belongs in `values.yaml`.

## Troubleshooting — seven broken releases

**Drill 04** (`04-broken-deployments-20261026-041347.log`; the sessions of 25.10, 28.09 and
27.09, `…-20261025-222151.log`, `…-20260928-041742.log` and `…-20260927-212429.log`, had the
same outcomes except in d): each
case breaks the API one way, records what kubectl shows, and restores. The API kept answering
through the ingress (200) in every case but f, where the database itself was down. a1 never
reached a Deployment: the migration hook stopped it. a2, b, c and e changed the Deployment, and
with `maxUnavailable: 0` a new version that never becomes ready never replaces the old one.
**d is the exception**, though not every time: the broken version can become ready for a while
and replace a working pod — it did on 27.09, 28.09 and 25.10, not in the current run; readiness
cannot see a failure that happens after it passed. f breaks the
database, not the release, and g breaks nothing.

| # | Break | Symptom (`kubectl get pods`) | Where the reason is | Fix |
|---|---|---|---|---|
| a1 | `helm upgrade --set image.tag=does-not-exist` | nothing changes on the Deployments; `migrate-…` pod `ErrImagePull` → `ImagePullBackOff` | `helm upgrade` error: `pre-upgrade hooks failed: resource Job/lab/migrate not ready` | the migration hook is a gate: fix the tag. The failed Job stays for its logs until the next upgrade replaces it |
| a2 | `kubectl set image … :does-not-exist` (past Helm) | new pod `ImagePullBackOff`, old two Running | `describe pod` → Events: `pull access denied, repository does not exist` | `kubectl rollout undo deploy/payments-api`, then remove kubectl's entries from `managedFields` so Helm owns the image again (see Roll back; `--force-conflicts` alone does not do it) |
| b | API listen address `0.0.0.0:99999` | new pod `CrashLoopBackOff`, restarts climbing | `kubectl logs <pod> --previous` → `httpserve: cannot listen on 0.0.0.0:99999: … invalid port` | fix the value, upgrade |
| c | Secret key `NO_SUCH_KEY` | new pod `Pending` / `CreateContainerConfigError`, never starts | `describe pod` → `couldn't find key NO_SUCH_KEY in Secret lab/payments-api-key` | fix the key name or create it |
| d | memory limit 8Mi | a new pod with restarts — `CrashLoopBackOff` with both old pods serving (current run), or **Ready**, having replaced an old pod (27.09 and 28.09: both new pods, the rollout green; 25.10: one of two) | `kubectl get pod -o jsonpath=…lastState.terminated` → `exit 137`, reason `OOMKilled` (current run, 28.09, 27.09) or `Error` (25.10); the node's kernel log has the kill either way | raise the limit. See below: the dangerous one |
| e | CPU request 64 | new pod `Pending` forever | `kubectl get events --field-selector reason=FailedScheduling` → `0/3 nodes are available: 1 node(s) had untolerated taint(s), 2 Insufficient cpu` | lower the request, or add nodes |
| f | database hibernated (CNPG) | pods Running, **READY false**; EndpointSlice `ready=false`; ingress **503** | EndpointSlice and ingress 503 (in the log); the API's own line `not ready: the database does not answer` is in Loki, not in the drill log | bring the database back; the pods recover on their own, no restart |
| g | — | "how do I look inside a distroless pod?" | `kubectl exec … sh` → `exec: "sh": executable file not found` | `kubectl debug <pod> --image=busybox:1.37 --target=payments-api -c debugger-<n> -- ps …` then `kubectl logs <pod> -c debugger-<n>` |

Notes the table cannot hold:

- **d is the dangerous one.** With 8Mi the Go process is killed by the kernel's memory-cgroup
  OOM killer at start, comes back, and may stay under the limit long enough to pass readiness —
  the outcome is a race: four runs, three answers. In the current run the one new pod
  never stayed ready: `CrashLoopBackOff` with 3 restarts, both old pods serving, the last
  termination `OOMKilled exit 137`, and the kernel's kills 7 s after the case began
  (`04d-kernel-oom-20261026-024610.log`). On 27.09 and 28.09 both new pods passed readiness
  with one restart each, and the rollout finished green (`04-broken-deployments-20260927-212429.log`,
  `…-20260928-041742.log`). On 25.10, 45 s in, one new pod was Ready with 2 restarts and had replaced
  one of the two working pods, the other in `CrashLoopBackOff`; the kubelet reported its
  termination as `Error exit 137`, not `OOMKilled`, while the kernel's log
  (`04d-kernel-oom-20261025-225000.log`) has the OOM kills. So read the exit code, 137
  (SIGKILL), against the limit; the reason string is not reliable. When a new pod does pass,
  the broken limit goes live, and nothing in `helm upgrade` or the probes says so — only the
  restart count and `lastState` do. Watch `RESTARTS` after every
  rollout, not only `READY`. (A first attempt with a 6Mi limit was refused by the API server:
  `requests … must be less than or equal to memory limit` — the request has to move with the
  limit. That run's log was replaced; the message is from the session output.)
- **f takes three minutes to start.** Annotation at 02:19:22, database pod gone at 02:22:27
  (3 min 5 s; 3 min 4 s, 3 min 3 s and 3 min 4 s in the sessions of 25.10, 28.09 and 27.09).
  That matches CloudNativePG shutting Postgres down in *smart* mode first — which waits for
  clients to disconnect, and connection pools never do — and switching to a fast shutdown
  when the 180 s smart timeout runs out. (An inference from the timing; the operator's log
  was not captured.) In
  `04-broken-deployments-20260927-020824.log` the drill still waited a fixed 40 s and saw both
  pods ready — the database was still up.
- **Readiness ≠ liveness in f.** Both API pods stayed Running with zero restarts while unready,
  and were back in the EndpointSlice by themselves once the database returned. A liveness
  probe that pinged Postgres would have restarted them for the whole outage — and one did:
  **Apicurio's** liveness was `/health/live`, which reports its datasource. Seen in the
  session, not in a drill log: `kubectl get events` showed `Liveness probe failed … 503` and
  `Killing`, and the container's last state was exit 143 at 23:36:08 — 23 s after the
  database was back; the three failures 20 s apart had accumulated during the outage. The
  relay depends on the registry, so the outage spread one hop further than the database.
  Its liveness is now a TCP check of the port. Drill 04f checks it, below.

- **g needs `-- <command>`.** `kubectl exec` into an ephemeral container fails
  (`container not found ("debugger")`); give the debug container its command up front and read
  its log, or use `kubectl debug -it` interactively. With `--target` it shares the service's
  process namespace: `ps` shows `/service` as PID 1 (uid 65532), and `/proc/1/root/` is the
  distroless filesystem. An ephemeral container never leaves its pod: the debugged pod keeps
  it until the pod is replaced, and the namespace's restricted-profile warning lists that
  pod from then on.

**Drill 04f** (`04f-database-outage-20261026-042259.log`) hibernates the database again and
watches everything that depends on it: the API, the registry, the relay and the consumer. It
starts only when every one of them is ready, so whatever goes unready is the outage's doing.
The database pod was gone at 02:26:02Z; the hibernation was lifted 91 s later, and the pod was
ready again 12 s after that, at 02:27:45Z.

| | during the outage | after the database pod is ready again |
|---|---|---|
| API (2 pods) | Running, **unready** — out of the Service, the ingress answers 503 | ready |
| Apicurio | Running, unready, **no new restart** (its liveness is the port) | ready |
| relay | Running, **no new restart** — each 1 s sweep fails and the next tries again | publishing |
| consumer (2 pods) | Running, **no new restart** — nothing arrived for it to fail on | counting |
| first payment accepted | — | **2 s** after (4 s on 25.10, 3 s on 28.09) |
| that payment counted | — | **4 s** after (5 s and 6 s) |
| every pod ready | — | 5 s after (6 s and 6 s) |

The restart counts did not move from the first listing to the last: 0 for the API, the relay
and the consumers, 3 for Apicurio — 3 before the drill began, one of them inside the session's
window by the resource log (39 on the month-old cluster of 25.10, 8 on 28.09); neither log
shows what caused them. The 2 s are, by the probe's settings rather than any log, the API
pods' readiness returning — a probe every 5 s, one success to pass — and the drill's own 1 s
retry of the POST. The consumer exits only on a
record it cannot count (docs/k8s/decisions.md): with the API unready no new payment was
published, so it had none. A consumer that does meet a record while the database is down
restarts with the kubelet's back-off until the database answers; this drill does not stage
that case.

The runs of 25.10 and 28.09 are `04f-database-outage-20261025-223219.log` and
`…-20260928-210123.log`. Two earlier versions of the
drill are kept. `04f-database-outage-20260928-042645.log` (same
behaviour, 0 restarts) stamped "database back" at the moment the hibernation was lifted — the
cluster's Ready condition still read true from before — so its 14 s / 15 s to the first
payment accepted / counted include Postgres starting again. `04f-registry-liveness-20260927-213333.log`
watched only the API and the registry.

### Recovery times, measured

What the drills measured on this stand, in one place. None is a target: the stand has no SLO,
and these are what to expect, not what to promise.

| Failure | Until what | Time | Log |
|---|---|---|---|
| broker pod killed | no under-replicated partition | ≤ 11–19 s over six runs | `05-broker-pod-loss-*` |
| broker pod killed | broker pod Ready | 22–25 s | same |
| database down (~100 s) | the database pod ready again, after the hibernation was lifted | 10–12 s, three runs | `04f-database-outage-20261026-042259.log`, `…-20261025-223219.log`, `…-20260928-210123.log` |
| database down (~100 s) | first payment accepted / counted, after the database pod was ready | 2–4 s / 4–6 s | same |
| a release that must be undone | `helm rollback` to a named revision, under traffic | 6–7 s | `02-rollback-*` |
| a rolling update | `helm upgrade --wait`, migration hook included | 11–17 s (10 s in the first run, tags `dev`/`v2`) | `01-rolling-update-*` |
| two brokers down 30 s (compose, exp-13) | backlog back to its pre-outage level | 17.3–24.2 s — measured before the relay drained full batches back to back | exp-13 |

### Where to look first

```
kubectl get pods -n lab -o wide                                   # state, restarts, node
kubectl describe pod <pod> -n lab | sed -n '/^Events:/,$p'        # why it is not running
kubectl logs <pod> -n lab --previous                              # what it said before dying
kubectl get events -n lab --sort-by=.lastTimestamp | tail         # what happened lately
kubectl get endpointslices -n lab -l kubernetes.io/service-name=payments-api   # who gets traffic
helm history payments -n lab                                      # which release is live
```

Logs of every pod are also in Grafana → Explore → Loki: `{namespace="lab", app="payments-api"}`,
narrowed to one pod with `pod="<pod>"`.

### Logs and metrics, shown working

**Drill 07** (`07-observability-20261026-043851.log`) asks Prometheus and Loki through the API
server's service proxy — what an operator's Grafana reads — instead of trusting the
ServiceMonitor and Alloy configs. It first waits for the API to be back at its two pods (330 s
after drill 06 here): a scale-in between its two readings took pods, and what they had counted,
out of the second one in an earlier run, and the summed counter fell from 57768 to 20104 (the
eighth attempt, seen in the session). It now compares each counter pod by pod.

| Checked | Result |
|---|---|
| every payments pod scraped, `up{job="payments"}` | 1 for all five running pods: two API, one relay, two consumers |
| the service's own counters, before and after 300 payments sent at 20/s, 30 s later, over the pods scraped both times | `http_requests_total{route="POST /payments"}` +300 over 2 pods, `outbox_records_published_total` +300, `payment_events_handled_total` +300 over 2 pods — every payment through the API, the relay and the consumer, as the metrics say |
| each running pod's start line in Loki, by `{namespace, app, pod}` | found for all five, from the API's `payments api started` to the consumers' `payments consumer started` |
| the relay's per-batch line, `outbox published`, in the last 2 minutes | 16 lines, the drill's own load included |

`outbox_records_published_total` reads "over 5 pods" in the log: every component registers the
relay's counter in the shared metrics package, and only the relay's moves — the other four
stay at 0. A pod that came or went between the readings would be named in the log; none did.

The 25.10 run (`07-observability-20261025-225052.log`) found the same with eight pods, five of
them API; its counters agreed because no pod left between its readings. The session's own run
that day looked for each component's lines in the last 10 minutes and found none for the
consumer, which logs at start and on errors, not per event, although its lines were in Loki —
so the drill looks for every pod's start line from the pod's start on.

## Kafka broker pod lost

**Drill 05**: `kubectl delete pod kafka-lab-dual-1 --grace-period=0 --force` under 20
payments/s.

| | run 1 (`…-20260927-023601.log`) | run 2 (`…-20260927-033552.log`) | run 3 (`…-20260927-213824.log`) | run 4 (`…-20260928-043137.log`) | run 5 (`…-20261025-223714.log`) | current (`05-broker-pod-loss-20261026-042751.log`) |
|---|---|---|---|---|---|---|
| under-replicated partitions right after | 33 | 33 | 33 | 33 | 33 | 33 |
| under-replicated back to 0 | **≤ 15 s** | **≤ 11 s** | **≤ 12 s** | **≤ 15 s** | **≤ 19 s** | **≤ 17 s** |
| broker pod Ready again | 22 s | 22 s | 23 s | 22 s | 25 s | 25 s |
| payments accepted / lost | **2400 / 0** | **2400 / 0** | **2400 / 0** | **2400 / 0** | **2400 / 0** | **2400 / 0** |
| witness broker memory, samples of the first ~25 s | — | — | 785Mi | 780–821Mi | 799–836Mi (limit 1400Mi) | 552–675Mi |
| outbox rows not yet published | — | — | — | — | 7, 13, 4, 7 (in the replication round) | 1–18, 10 samples 2–4 s apart |

The replication samples are as far apart as each round of kubectl calls takes: 3–8 s in runs
1–4, 6–13 s in run 5, which also read the outbox in that round, 4–6 s in the current one, which
reads it in a loop of its own and stamps each replication sample after it is read — so "≤ 17 s"
means the sample taken by 17 s already showed 0, the one at 11 s still 33. The six runs'
spread is partly the spacing of their samples.

Every merchant total was 240000 = 2400 × 100 — in the last four runs reported per currency,
the last two as `{"currency":"EUR","authorized_minor":240000,"captured_minor":0,"refunded_minor":0}`.
Five script versions: run 1 counted healing only once the pod was Ready, so its summary line
says "no under-replicated partition after 22s" while its samples show 0 at t=15 s; run 2
counted it on its own; runs 3 and 4 also require the loss to have been seen first, tell a
failed `kubectl exec` from zero, and log the witness's memory; run 5 added the outbox backlog;
the current one samples it on its own. The witness's admin tool now has a 96m heap of its own, where the first
runs let it inherit the broker's 512m and pushed the witness to 918Mi (seen in Prometheus in
the session, not logged).

The difference from the compose stand (exp-04: the controller reacted to the dead broker
after 10.6 s — `docs/00-journal.md` — and the broker *stayed* dead) is that Kubernetes brings
the broker back: Strimzi's pod set recreates it on its own volume, it rejoins the ISR, and
replication caught up before the pod even reported Ready. What the drill proves about
Kafka is the end-to-end count: every accepted payment reached the merchant total. The 201s
themselves only say the payment was written to Postgres (the outbox), which does not touch
Kafka. What does is the relay, and the last two runs sample its backlog: 4–13 unpublished rows
in run 5, 1–18 in the current one, sampled through the outage, while 20 new rows a second arrived — had publishing stopped, the backlog
would have grown by about 20 a second. The relay kept publishing with 33 partitions short of
a replica, as `min.insync.replicas: 2` of 3 lets `acks=all` writes through. The relay's errors were not recorded.

**Leadership does not come back by itself.** `auto.leader.rebalance.enable` is off on both
stands (`deploy/k8s/platform/kafka.yaml`, as in compose), so an experiment decides when leadership moves,
not a 300 s timer. After a broker is back, the partitions it led stay with the replacements
until someone asks; `make elect-preferred` does that on compose, and on Kubernetes the same
tool runs inside a broker pod:

```
kubectl exec -n lab kafka-lab-dual-0 -c kafka -- env KAFKA_HEAP_OPTS=-Xmx96m bin/kafka-leader-election.sh \
  --bootstrap-server localhost:9092 --election-type PREFERRED --all-topic-partitions
```

In production the setting would be left on (its default), or Cruise Control would own it.

## Replaying dead letters

Letters are archived by class — `undecodable`, `refused`, `exhausted` — and replayed one class
at a time into the first retry tier, never into `payments.main`, so no other consumer group
sees the replay (`deploy/topics/README.md`). Replay only after the cause is gone: an
`exhausted` letter whose merchant row is still locked goes round the chain again.

```
make k8s-replay CLASS=exhausted     # one pod, attached, removed when it exits
```

The replayer resumes from its group's committed offset, one group per class. A group idle
longer than `offsets.retention.minutes` (7 days) loses that offset, and its next replay starts
from the beginning of the dead letter topic (docs/static/07-retry-dlq.md). That re-sends every
letter of the class, and costs the totals nothing: a letter replayed twice is counted once,
because the inbox sees the same event id
(`TestAPaymentHeldPastEveryTierIsArchivedAndReplayingItTwiceCountsItOnce`).
