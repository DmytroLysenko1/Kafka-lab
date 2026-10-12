# Runbook — the payments service on Kubernetes

Deploy, upgrade, roll back, scale, and find out what is wrong. The symptoms and numbers below
were recorded by drill scripts (`make k8s-drill-NN`, sources in `deploy/k8s/drills/`) with the
raw output committed under `deploy/k8s/drills/results/`; the few that come from watching the
session instead are marked as such where they appear. Stand: kind with 3 nodes, Docker
Desktop with 8 GB. The current evidence set was produced in one run by `make k8s-evidence`
on 28.09.2026 (01:02–01:37Z, images `e628bfc-dirty-a4463bf2`, the Mac kept awake by caffeinate); a few earlier logs are
kept because a lesson below comes from them, and each says which script version made it.

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

The tag is read from the release, not computed: `make`'s `IMAGE_TAG` hashes the working tree,
and after any change — a new drill log is enough — it names a tag that was never built. Drill
04 and `make k8s-replay` read the deployed tag the same way.

```
IMAGE_TAG=$(helm get values payments -n lab -o json | jq -r .image.tag)   # the deployed tag
for c in payments-api outbox-relay payments-consumer migrate dlq-replayer prune; do
  docker tag kafka-lab/$c:$IMAGE_TAG kafka-lab/$c:$IMAGE_TAG-next
  kind load docker-image kafka-lab/$c:$IMAGE_TAG-next --name kafka-lab
done
IMAGE_TAG_NEXT=$IMAGE_TAG-next make k8s-drill-01
```

**Drill 01** (`01-rolling-update-20260928-040500.log`): 20 payments/s for 90 s through the
ingress, upgrade from `<tag>` to `<tag>-next` fifteen seconds in. `-next` is the same build
under another name, so the drill measures the rollout mechanics — every pod replaced under
traffic — not a change of behaviour between versions.

| | |
|---|---|
| requests / non-2xx | **1800 / 0** — every payment answered 201 |
| `helm upgrade --wait` returned after | **17 s**, migration hook included (16 s in the previous session) |
| p99 latency over the whole 90 s run | 17.8 ms (max 68 ms); 7.95 ms and 58 ms in the previous session |

The relay runs with `strategy: Recreate` now. The first run of this drill, before that
change and with the tags still `dev`/`v2` (`01-rolling-update-20260927-012247.log`, 1800 / 0,
10 s), lists two relay pods side by side after the rollout — the overlap `Recreate` removes;
this run lists one.

The settings meant to make it zero: `maxUnavailable: 0` (an old pod leaves only after a new
one is ready), the readiness probe (a new pod gets traffic only once it reaches Postgres),
and `preStop.sleep: 5` (an old pod stays reachable while the routers drop it, then gets 20 s
to finish what it holds). The drill shows the result, not which of the three carried it — a
control run with `preStopSleepSeconds=0` would. Worth knowing: in the first run, when
`helm --wait` returned, the old pods were still listed (and still `READY true`) next to the
new ones — "release deployed" is not "old pods gone"; that they were in their preStop pause
is an inference.

## Roll back

```
helm history payments -n lab
helm rollback payments <REVISION> -n lab --wait     # always with an explicit revision
```

**Drill 02** (`02-rollback-20260928-040631.log`): rollback from drill 01's `-next` release to
the revision before it, found and named by number (92), under 20 payments/s.
**1200 / 0** non-2xx, rollback took **6 s** (7 s in the previous session), and the migration Job kept its creation time:
rollback runs no pre-upgrade hook. So migrations must be backward compatible (expand, then
contract in a later release) — a rollback never undoes a schema change.

**Never roll back past migration 00003.** It moved the totals to `merchant_currency_totals`
and dropped the old table in one step (docs/k8s/decisions.md): a release from before it would
find its table gone. Fix forward instead — `make k8s-deploy` with a corrected build.

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
(`04-broken-deployments-20260928-041742.log`). `kubectl get deploy … --show-managed-fields`
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
the consumer was stopped for 76 s (18:00:47 to 18:02:03, from the second run's log to the
third's). Before the work was committed, that migration and the one after it were folded into
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
defaults to 400 req/s; the evidence session runs it as `RATE=1200 make k8s-drill-03`.

| Load (GET, 180 s) | Result | Log |
|---|---|---|
| 400 req/s | 51–62% most of the run, one sample at **70%** (t=91 s) — at or below the target, so the desired count stays 2: **no scale-out** | `03-autoscaling-20260927-012531.log` (an earlier script version, same load logic, and the single-total read path from before per-currency totals — the answer was 51 bytes, 42 now) |
| 1200 req/s | 109% at t=30 s → desired **4**; 160% at t=40 s → **5** (max), all 5 running at t=61 s; settled at 64–66% on 5 pods; **216000 / 0** non-2xx | `03-autoscaling-20260927-211420.log` |
| 1200 req/s, 28.09, in the evidence session | 138% at t=41 s → desired 4; 182% at t=51 s → 5, all 5 at t=71 s; then **72–78%**, 97% in the last two samples of the load — above the target with the HPA at its maximum; scale-in: desired 3 at t=508 s, 2 at t=518 s, pods at t=538 s; 216000 / 0, but p99 **182 ms**, max 1.44 s | `03-autoscaling-20260928-040732.log` |
| 1200 req/s, 28.09, rerun on a settled stand | 134% at t=30 s → 4; 5 at t=91 s; then 60–67%; **216000 / 0**, p99 3.3 ms, max 45 ms | `03-autoscaling-20260928-044933.log` |

Scale-in after the load stopped (t≈213 s): desired went to 4 at t=496 s and to 2 at
**t=507 s**, pods at t=527 s (in the settled rerun of 28.09: desired 2 at t=495 s, pods at
t=506 s) — the 300 s downscale stabilization window, as documented. The API is cheap: five pods
held 1200 req/s at 60–67% of 100m each, so one pod serves some 350–400 req/s of reads
inside its request (an estimate from the samples, not a capacity test).

The two runs of 28.09 are the same code on the same stand. In the evidence session drill 03
ran minutes after the session re-applied the platform — Prometheus and Grafana rolling, the
container working set peaking at 95% of the VM (`resources-20260928-043718.log`) — and the
five pods needed 72–78% of their request for the same load, with a p99 some 55 times longer
(182 ms against 3.3 ms). Rerun after the stand had settled, it gave the numbers of 27.09 again. What
the pods get from the machine shows in the HPA's percentage and in the tail, not in the
answers: every request succeeded in all three. A rerun started straight after drill 06, with
no cool-down, stopped at t=93 s when the host ran out of ephemeral ports
(`03-autoscaling-20260928-043833.log` ends there; kubectl's "can't assign requested address"
is from the session's output, not in the log) — a limit of the load generator's machine, not
of the service.

The first 1200 req/s run (`03-autoscaling-20260927-013557.log`) shows 1427 failures (0.66%),
all `dial tcp … [::1]:8800: connection refused`: `payments.localtest.me` also resolves to
`::1`, where nothing is published, and the load generator dialled it now and then. Not a
service failure; the drills now dial `127.0.0.1:8800` with a `Host` header, and the current
run has none.

**Drill 06** (`06-write-throughput-20260928-043357.log`), the write path end to end: POST
/payments at rising rates, 30 s a step, spread over fifty merchants (one merchant would
measure its row lock, which is exp-18's subject), then how far the outbox fell behind and
how long until every accepted payment was in its merchant's total. Two API pods, one relay,
two consumers, as deployed.

| payments/s | accepted | p50 / p99 / max | outbox left at the step's end | drained after | all counted after |
|---|---|---|---|---|---|
| 50 | 1500 / 1500 | 2 / 8 / 20 ms | 11 | 1 s | 1 s |
| 100 | 3000 / 3000 | 2 / 8 / 32 ms | 0 | 0 s | 0 s |
| 200 | 6000 / 6000 | 1 / 6 / 22 ms | 0 | 1 s | 1 s |
| 400 | 12000 / 12000 | 2 / 17 / 45 ms | 137 | 2 s | 2 s |

Where it would stop first is a reasoned guess, not a measurement: the single relay — one
batch of 100 at a time, each waiting for the `acks=all` of all three brokers — before the
consumers, whose six partitions and one short transaction per record had room to spare at
400/s, and before the API, which did not move its HPA. Raising `OUTBOX_BATCH` toward
`MaxClaim` (1000) is the first lever; relays split by key the second (decisions.md, relay).

No ceiling within 400 payments/s: every step was accepted in full and counted within two
seconds of its end, and the API stayed at two pods (60% CPU after the last step). The ceiling
is above what this drill asked for; the next step to find it is `RATES="800 1600" make
k8s-drill-06`. For the relay, one replica by design: a full batch of 100 is followed at once
by the next sweep, so it drains in batches, not at one batch a second.

The consumer is not autoscaled on purpose: six partitions, and CPU is not what limits a
consumer group. Scale it through the release, never above 6:
`helm upgrade payments deploy/helm/payments -n lab --reset-then-reuse-values --set components.payments-consumer.replicas=3`.
Not `kubectl scale`: it makes kubectl the owner of `.spec.replicas`, the same trap as
`kubectl set image` (Roll back, above). The next `make k8s-deploy` sets only the image tag, so it takes the
count back to the chart's 2; a lasting change belongs in `values.yaml`.

## Troubleshooting — seven broken releases

**Drill 04** (`04-broken-deployments-20260928-041742.log`, the same outcomes as the previous
session's `…-20260927-212429.log`): each case breaks the API one way,
records what kubectl shows, and restores. The API kept answering through the ingress (200) in
every case but f, where the database itself was down. With `maxUnavailable: 0` a new version
that never becomes ready never replaces the old one — a1, a2, b, c, e. **d is the
exception**: the broken version *did* become ready and replaced the working one; readiness
cannot see a failure that happens after it passed.

| # | Break | Symptom (`kubectl get pods`) | Where the reason is | Fix |
|---|---|---|---|---|
| a1 | `helm upgrade --set image.tag=does-not-exist` | nothing changes on the Deployments; `migrate-…` pod `ErrImagePull` → `ImagePullBackOff` | `helm upgrade` error: `pre-upgrade hooks failed: resource Job/lab/migrate not ready` | the migration hook is a gate: fix the tag. The failed Job stays for its logs until the next upgrade replaces it |
| a2 | `kubectl set image … :does-not-exist` (past Helm) | new pod `ImagePullBackOff`, old two Running | `describe pod` → Events: `pull access denied, repository does not exist` | `kubectl rollout undo deploy/payments-api`, then remove kubectl's entries from `managedFields` so Helm owns the image again (see Roll back; `--force-conflicts` alone does not do it) |
| b | API listen address `0.0.0.0:99999` | new pod `CrashLoopBackOff`, restarts climbing | `kubectl logs <pod> --previous` → `httpserve: cannot listen on 0.0.0.0:99999: … invalid port` | fix the value, upgrade |
| c | Secret key `NO_SUCH_KEY` | new pod `Pending` / `CreateContainerConfigError`, never starts | `describe pod` → `couldn't find key NO_SUCH_KEY in Secret lab/payments-api-key` | fix the key name or create it |
| d | memory limit 8Mi | pods **Ready with 1 restart** — the rollout *completed* | `kubectl get pod -o jsonpath=…lastState.terminated` → `OOMKilled exit 137` | raise the limit. See below: the dangerous one |
| e | CPU request 64 | new pod `Pending` forever | `kubectl get events --field-selector reason=FailedScheduling` → `0/3 nodes are available: 1 node(s) had untolerated taint(s), 2 Insufficient cpu` | lower the request, or add nodes |
| f | database hibernated (CNPG) | pods Running, **READY false**; EndpointSlice `ready=false`; ingress **503** | EndpointSlice and ingress 503 (in the log); the API's own line `not ready: the database does not answer` is in Loki, not in the drill log | bring the database back; the pods recover on their own, no restart |
| g | — | "how do I look inside a distroless pod?" | `kubectl exec … sh` → `exec: "sh": executable file not found` | `kubectl debug <pod> --image=busybox:1.37 --target=payments-api -c debugger-<n> -- ps …` then `kubectl logs <pod> -c debugger-<n>` |

Notes the table cannot hold:

- **d is the dangerous one.** With 8Mi the Go process was OOM-killed once at start, came back,
  stayed under the limit while idle, passed readiness, and the rollout finished green. The
  broken limit went live. Nothing in `helm upgrade` or the probes says so — only the restart
  count and `lastState` do, and under load it would OOM again. Watch `RESTARTS` after every
  rollout, not only `READY`. (A first attempt with a 6Mi limit was refused by the API server:
  `requests … must be less than or equal to memory limit` — the request has to move with the
  limit. That run's log was replaced; the message is from the session output.)
- **f takes three minutes to start.** Annotation at 01:23:11, database pod gone at 01:26:14
  (3 min 3 s; 3 min 4 s in the previous session's log, `…-20260927-212429.log`).
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

**Drill 04f** (`04f-database-outage-20260928-210123.log`) hibernates the database again and
watches everything that depends on it: the API, the registry, the relay and the consumer. It
starts only when every one of them is ready, so whatever goes unready is the outage's doing.
The database pod was gone at 18:04:27Z; the hibernation was lifted 91 s later, and the pod was
ready again 10 s after that, at 18:06:08Z.

| | during the outage | after the database pod is ready again |
|---|---|---|
| API (2 pods) | Running, **unready** — out of the Service, the ingress answers 503 | ready |
| Apicurio | Running, unready, **no new restart** (its liveness is the port) | ready |
| relay | Running, **no new restart** — each 1 s sweep fails and the next tries again | publishing |
| consumer (2 pods) | Running, **no new restart** — nothing arrived for it to fail on | counting |
| first payment accepted | — | **3 s** after |
| that payment counted | — | **6 s** after |
| every pod ready | — | 6 s after |

The restart counts stayed where the stand's earlier stop and start had left them, from the
first listing to the last. The 3 s are the API pods' readiness returning — a probe every 5 s,
one success to pass — and the drill's own 1 s retry of the POST. The consumer exits only on a
record it cannot count (docs/k8s/decisions.md): with the API unready no new payment was
published, so it had none. A consumer that does meet a record while the database is down
restarts with the kubelet's back-off until the database answers; this drill does not stage
that case.

Two earlier versions of the drill are kept. `04f-database-outage-20260928-042645.log` (same
behaviour, 0 restarts) stamped "database back" at the moment the hibernation was lifted — the
cluster's Ready condition still read true from before — so its 14 s / 15 s to the first
payment accepted / counted include Postgres starting again. `04f-registry-liveness-20260927-213333.log`
watched only the API and the registry.

### Recovery times, measured

What the drills measured on this stand, in one place. None is a target: the stand has no SLO,
and these are what to expect, not what to promise.

| Failure | Until what | Time | Log |
|---|---|---|---|
| broker pod killed | no under-replicated partition | ≤ 11–15 s over four runs | `05-broker-pod-loss-*` |
| broker pod killed | broker pod Ready | 22–23 s | same |
| database down (~100 s) | the database pod ready again, after the hibernation was lifted | 10 s | `04f-database-outage-20260928-210123.log` |
| database down (~100 s) | first payment accepted / counted, after the database pod was ready | 3 s / 6 s | same |
| a release that must be undone | `helm rollback` to a named revision, under traffic | 6–7 s | `02-rollback-*` |
| a rolling update | `helm upgrade --wait`, migration hook included | 16–17 s (10 s in the first run, tags `dev`/`v2`) | `01-rolling-update-*` |
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

Logs of every pod are also in Grafana → Explore → Loki: `{namespace="lab", app="payments-api"}`.

## Kafka broker pod lost

**Drill 05**: `kubectl delete pod kafka-lab-dual-1 --grace-period=0 --force` under 20
payments/s.

| | run 1 (`…-20260927-023601.log`) | run 2 (`…-20260927-033552.log`) | run 3 (`…-20260927-213824.log`) | current (`05-broker-pod-loss-20260928-043137.log`) |
|---|---|---|---|---|
| under-replicated partitions right after | 33 | 33 | 33 | 33 |
| under-replicated back to 0 (samples every 3.7–4.4 s) | **≤ 15 s** | **≤ 11 s** | **≤ 12 s** | **≤ 15 s** |
| broker pod Ready again | 22 s | 22 s | 23 s | 22 s |
| payments accepted / lost | **2400 / 0** | **2400 / 0** | **2400 / 0** | **2400 / 0** |
| witness broker memory, samples of the first ~23 s | — | — | 785Mi (limit then 1280Mi) | 780–821Mi (limit then 1360Mi, now 1400Mi) |

Every merchant total was 240000 = 2400 × 100 — in the last two runs reported per currency,
`{"currency":"EUR","authorized_minor":240000}`. Three script versions: run 1 counted healing
only once the pod was Ready, so its summary line says "no under-replicated partition after
22s" while its samples show 0 at t=15 s; run 2 counted it on its own; the current one also
requires the loss to have been seen first, tells a failed `kubectl exec` from zero, and
logs the witness's memory — its admin tool now has a 96m heap of its own, where the first
runs let it inherit the broker's 512m and pushed the witness to 918Mi (seen in Prometheus in
the session, not logged).

The difference from the compose stand (exp-04: the controller reacted to the dead broker
after 10.6 s — `docs/00-journal.md` — and the broker *stayed* dead) is that Kubernetes brings
the broker back: Strimzi's pod set recreates it on its own volume, it rejoins the ISR, and
replication caught up before the pod even reported Ready. What the drill proves about
Kafka is the end-to-end count: every accepted payment reached the merchant total. The 201s
themselves only say the payment was written to Postgres (the outbox), which does not touch
Kafka; the relay's backlog and errors during the outage were not recorded.

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
