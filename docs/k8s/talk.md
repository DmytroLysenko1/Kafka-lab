# Eight things Kubernetes did that the manifests did not say

A write-up for the team, and the script of a 25-minute talk, built out of the Kubernetes half
of this repository: the payments service in Docker images, on kind, with Kafka on Strimzi,
deployed by a Helm chart and broken on purpose by scripted drills. Every number below is from
a log committed under `deploy/k8s/drills/results/` or `deploy/docker/results/`; the decisions
behind the stand, and what it leaves out on purpose, are in [decisions.md](decisions.md).

Each section has the same shape: what the manifest or the tutorial says, what the stand did
when it was made to, and what to do differently.

---

## 0. The image, before any of it

**What the tutorial says.** `FROM golang`, `COPY . .`, `go build`, done.

**What the stand did.** That image is **1.92 GB**: a 985 MB toolchain base, the sources, and
a 487 MB build layer holding the build cache next to the binary. A two-stage build onto
distroless is **33.7 MB** — a 20.6 MB binary on a 6 MB base — runs as uid 65532, and has no
shell: `docker run --entrypoint sh` fails with `executable file not found`. A rebuild after
editing one `.go` file took 136.5 s naive and 14.6 s with the module and build caches mounted;
taking the techniques out one at a time showed the mounts doing nearly all of it, and the
`go.mod` split paying only without them (68.1 → 49.5 s) ([01-docker.md](01-docker.md)).

**What to do.** Two stages, a static binary, distroless `:nonroot`. Debug with
`kubectl debug --target=<container>` — an ephemeral container in the pod's process
namespace — because `kubectl exec … sh` can never work, and that is the point.

## 1. "Readiness and liveness are two names for a health check"

**What the stand did.** Drill 04f hibernated the database under the API. The API pods went
**unready with zero restarts**, the ingress answered **503** instead of hanging, and the pods
came back into the EndpointSlice by themselves when the database did: the first payment was
accepted 4 s after the database pod was ready, and counted 1 s after that. In an earlier run
of the same outage Apicurio, whose liveness endpoint then included its datasource, was
**restarted by its own probe** — a dependency failure turned into a restart loop (seen in the
session, not in a log).

**What to do.** Readiness answers "may I get traffic"; liveness answers "am I stuck". A
liveness probe never checks a dependency. And checking our own probes was not enough: every
probe in the namespace needed the same question.

## 2. "A green rollout is a good rollout"

**What the stand did.** Drill 04 case d set the API's memory limit to 8Mi. The kernel killed
the Go process at start, it came back, stayed under the limit long enough to **pass
readiness**, and replaced a working pod: on 28.09 the rollout completed with both new pods
Ready; on 25.10 one new pod was Ready and had taken an old pod's place while the other
crash-looped. The traces were `RESTARTS` and `lastState` — `exit 137`, reported as `OOMKilled`
on 28.09 and as plain `Error` on 25.10, when the kernel's log shows it was the OOM killer all
the same.

**What to do.** Read `RESTARTS` and the exit code after every rollout, not only `READY` and
not the reason string. Of the six broken releases, the migration hook stopped one before it
touched a Deployment, and `maxUnavailable: 0` held off four more — a version that never
becomes ready never replaces the old one. The one that got through was the one that looked
healthy for a moment.

## 3. "`helm rollback` undoes the last change"

**What the stand did.** A bare `helm rollback` goes to revision current−1. After two failed
upgrades in a row, current−1 is a failed one, and drill 04 rolled a never-built tag onto all
three Deployments that way — past the migration hook that had stopped the same tag on
upgrade. A remembered revision number was not safe either: `--history-max 10` had pruned it.
Every revision also read `payments-0.1.0 dev` in `helm history`, so the history could not say
which one was good.

**What to do.** Roll back to an explicit revision, chosen by what it deployed: every upgrade
now writes its image tag into the revision's description. To get back to known-good, roll
forward to the chart in git. Drill 02 rolled back by number under 20 payments/s: 1200 / 0
non-2xx, 6 s.

## 4. "Load that feels high moves the autoscaler"

**What the stand did.** At 400 req/s the API sat at 51–62% of its 100m CPU request, one sample
at 70% — at or below the 70% target, so the HPA kept 2 replicas. It took 1200 req/s: 109% at
30 s, desired 4; 160% at 40 s, desired 5; 216 000 requests, none failed. Scale-in waited out
the 300 s stabilisation window, as documented. In a run right after the platform was
re-applied, the same load needed 72–78% of the request on five pods and p99 rose from 3.3 ms
to 182 ms — the busy stand is the likely cause, inferred from the timing, not measured.

**What to do.** Plan the load from the request, not from a round number: the HPA sees a
percentage of what the pod asked for. And what the machine gives the pods shows in that
percentage and in the tail latency, long before it shows in errors.

## 5. "The scheduler protects the node"

**What the stand did.** Every kind node reports the whole Docker VM — 7934Mi, 10 CPU — as
allocatable, so the scheduler believes in three times the memory that exists. In the 25.10
session memory requests came to 74% of the VM and the sum of limits to 136%; the container
working set peaked at 80% (`resources-20261025-224859.log`), and at **95%** in the 28.09
session, while the platform was re-applied and two Grafana pods overlapped. Requests and limits were sized from a logged
measurement of the running stand, twice; the first pass was read off Prometheus without a log,
and a Prometheus restart erased it.

**What to do.** Size requests from what containers actually use, and log the measurement:
memory request about the typical working set, limit 1.5–2× the peak, no CPU limit. Grafana at
its chart's 256Mi was OOM-killed seven times before anyone looked — seen in the session that
first sized the stand, not in a log, which is the lesson.

## 6. "A lost broker pod is an incident"

**What the stand did.** Drill 05 force-deleted a Kafka broker pod under 20 payments/s, five
runs. Strimzi recreated it; under-replicated partitions went from 33 back to 0 within
11–19 s — before the pod reported Ready at 22–25 s — and **2400 of 2400** payments were
accepted, none lost. The relay's backlog stayed at 4–13 rows through the outage while 20 a
second arrived: it kept publishing to partitions one replica short. Compose, by contrast, keeps a killed broker dead, on purpose, so that the
Kafka experiments can watch an outage.

**What to do.** On Kubernetes the operator's job is the recovery; ours is to check that
`min.insync.replicas` lets writes continue while it happens, and to watch the
under-replicated count fall.

## 7. "`kubectl edit` is a quick fix"

**What the stand did.** Helm 4 uses server-side apply. A `kubectl set image` and
`rollout undo` in drill 04 made kubectl a co-owner of the image field. Nothing broke while
every release reused the same tag; the first upgrade to a new tag failed with a field-manager
conflict. `--force-conflicts` did not clear it, because both managers owned the same value;
removing kubectl's entries from `managedFields` did.

**What to do.** A change made past Helm is a delayed failure. Either undo it through Helm, or
hand the field back explicitly — and never reuse an image tag, because a reused tag hides the
conflict and the rollout both.

## 8. "The load test measures the service"

**What the stand did.** Drill 03 sent 1200, then 800 GET/s from the laptop to the ingress on
`127.0.0.1:8800`. Four sessions in a row ended early. In the one whose Docker logs survive,
Docker Desktop's port forward — the path from the host into every container, the kind API
server's port included — shut down 11 s into the load: kubectl lost the cluster, 137 102 of
144 000 requests got no answer, and the VM stopped 72 s later. The API was not what broke.
Sent from a pod inside the cluster to the Service, the same 800 req/s gave 144 000 of 144 000
answered, p99 15.6 ms, and a scale-out to five pods.

**What to do.** Put the load generator where the clients are, or at least past the parts of
the path that are not under test. A load test that runs through a laptop's port forward
measures the port forward, and on a laptop that is the weakest link in the chain.

---

## The decisions, in one table

| Chosen | Over | Because |
|---|---|---|
| kind, one control plane and two workers | minikube, k3d | the stand already lives in Docker; two workers make pod spreading real |
| Kafka on Strimzi | Kafka left in Compose | one network and one lifecycle with the services; the same Kafka 4.3.1 |
| CloudNativePG | a hand-written StatefulSet | the operator writes the connection secret; no password in the repository |
| Helm | Kustomize | the migration must run before the rollout and fail it — a pre-upgrade hook is exactly that; it also stopped a never-built tag before a single Deployment was touched |
| Traefik | ingress-nginx | ingress-nginx was retired in March 2026 |
| Loki and Alloy, kube-prometheus-stack | ELK, hand-written scrape configs | one small binary each; the chart declares its own scraping next to its Deployments |

What the stand leaves out — backups, TLS, NetworkPolicy, alerting, a multi-zone cluster — is
listed with the production equivalent in [decisions.md](decisions.md#lab-not-production).

## Where the 25 minutes go

| Part | Planned |
|---|---|
| The image (section 0) | 3 min |
| Sections 1–8, one story each | 17 min |
| A live drill — `make k8s-drill-04` breaking a release while `kubectl get pods -w` runs | 3 min |
| Questions | 2 min |

A plan, not a timing: the whole file is about 1 800 words, 10–12 minutes spoken at 130–150
words a minute, and the stories are told around the drill logs on screen. The first time it is given,
its real timings replace this table.

## If the audience remembers three sentences

1. Liveness never checks a dependency; readiness is how a pod says it cannot serve.
2. Read `RESTARTS` after a rollout, and roll back to a revision you can name.
3. Size from measurement and keep the log: the scheduler trusts requests, not the machine.
