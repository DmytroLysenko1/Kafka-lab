# Kubernetes — cheat sheet, anchored in this repository

Each concept in one or two lines, then where it lives in *this* stand, so the definition can
be checked against a manifest that runs rather than against memory. Paths are relative to
the repository root; a bare file name such as `deployment.yaml` is a template of the chart,
in `deploy/helm/payments/templates/`.

## Workloads

| Object | What it actually is | Here |
|---|---|---|
| Pod | one or more containers sharing a network namespace and volumes, scheduled as a unit. Mortal: a pod is never repaired, only replaced | every component runs one container per pod |
| Deployment | keeps N identical pods running and replaces them in a controlled order when the template changes. Owns ReplicaSets, one per template revision | `deploy/helm/payments/templates/deployment.yaml` — api, relay, consumer |
| ReplicaSet | the "N copies of this exact template" object a Deployment creates per revision; the old ones are kept for rollback (`revisionHistoryLimit: 5`) | `kubectl get rs -n lab` |
| StatefulSet | pods with stable names and their own volumes (`-0`, `-1`…), started and stopped in order | Loki; Strimzi manages the broker pods itself (StrimziPodSet) |
| Job | runs pods to completion; retries up to `backoffLimit` | `migrate-job.yaml` — the schema migration, as a Helm hook |
| Operator | a controller that turns a custom resource into the objects above and keeps reconciling them | Strimzi (`Kafka`, `KafkaNodePool`, `KafkaTopic`), CloudNativePG (`Cluster`, `Database`) in `deploy/k8s/platform/` |

## Networking

| Object | What it actually is | Here |
|---|---|---|
| Service (ClusterIP) | a stable virtual IP and DNS name in front of the pods its selector matches — **only the ready ones** | `service.yaml`; `payments-api.lab.svc` |
| EndpointSlice | the list the Service actually routes to, with a ready flag per pod. Where "is my pod receiving traffic" is answered | `kubectl get endpointslices -n lab -l kubernetes.io/service-name=payments-api` |
| Headless Service | no virtual IP; DNS returns the pod IPs. What Kafka clients need, because they talk to specific brokers | `kafka-lab-kafka-brokers` (Strimzi) |
| NodePort | a port opened on every node that forwards to the Service | Traefik on 30080, mapped to `127.0.0.1:8800` by `deploy/k8s/kind.yaml` |
| Ingress | HTTP routing rules (host, path → Service). Does nothing by itself: an ingress **controller** implements it | `ingress.yaml` → `payments.localtest.me`; controller Traefik, `deploy/k8s/platform/values/traefik.yaml` — ingress-nginx was retired in March 2026 |

## Configuration

| Object | What it actually is | Here |
|---|---|---|
| ConfigMap | non-secret key/values, mounted as files or injected as env | `configmap.yaml` → `envFrom` (brokers, registry URL) |
| Secret | the same, base64-encoded and access-controlled separately. **Not encrypted** by default — base64 is an encoding | `payments-db-app` (written by CloudNativePG), `payments-api-key` (`make k8s-secret`) |
| env from a Secret key | `valueFrom.secretKeyRef`; a missing key is a `CreateContainerConfigError`, the pod never starts | `DATABASE_URL`, `PAYMENTS_API_KEY` in `deployment.yaml` |
| config change → rollout | env is read at process start, so a ConfigMap edit changes nothing until the pods restart. The chart hashes the ConfigMap into a pod annotation, so a change is a new template and a rollout | `checksum/config` in `deployment.yaml` |

## Resources

| Term | What it actually is | Here |
|---|---|---|
| request | what the scheduler reserves on a node. A pod whose requests fit no node stays `Pending` | api: 100m CPU, 32Mi |
| limit (memory) | the cgroup ceiling. Exceeding it = the kernel kills the process: `OOMKilled`, exit 137 | api: 64Mi |
| limit (CPU) | throttling, not killing. Deliberately **not set** here: a CPU limit throttles a Go service's GC and tail latency while the node has idle cores | none |
| QoS class | Guaranteed (requests = limits), Burstable (some set), BestEffort (none). Decides who is evicted first under node pressure | Burstable |

## Health probes

| Probe | Question | Failure means | Here |
|---|---|---|---|
| startup | has it finished starting? Holds the other two off until it passes | restart after `failureThreshold × period` | `/healthz` on the metrics port, up to 60 s |
| liveness | is the process stuck? | **restart** the container | `/healthz` on the metrics port — never a dependency check, or a Postgres outage restarts every pod at once |
| readiness | should traffic come here now? | **removed from EndpointSlices**, not restarted | only the API: `/readyz` pings Postgres with a 2 s deadline (`internal/interfaces/http/server.go`) |

Relay and consumer have no readiness probe on purpose: no Service routes callers to them, so
"not ready" would change nothing.

## Rollouts

| Knob | Effect | Here |
|---|---|---|
| `maxSurge: 1, maxUnavailable: 0` | one extra pod first, an old one goes only after the new one is ready. A broken version stalls the rollout instead of replacing working pods | API and consumer |
| `strategy: Recreate` | all old pods go before any new one starts: a gap instead of an overlap | the outbox relay — two relays side by side could publish one payment's events out of order; during the gap the outbox holds them |
| `preStop` + `terminationGracePeriodSeconds` | on delete: the pod leaves the endpoints **and** gets `preStop` at the same time; the pause lets the routers catch up before SIGTERM; the grace period is the total budget before SIGKILL | `preStop.sleep: 5`, grace 30 s ≥ 5 s + the API's 20 s shutdown |
| `helm upgrade` / `helm rollback` | a release revision per upgrade; rollback re-applies an old revision's manifests. Rollback does **not** run pre-upgrade hooks — migrations only go forward | `make k8s-deploy`, drills 01–02 |
| PodDisruptionBudget | limits *voluntary* evictions (drain, upgrade of a node), not crashes | api: `maxUnavailable: 1` — one pod at a time, whether there are 2 or the HPA's 5; CloudNativePG's own PDB on the single database instance allows 0 disruptions, so no node that hosts it can be drained |

## Scaling

| Object | What it actually is | Here |
|---|---|---|
| HorizontalPodAutoscaler | every 15 s compares the metric to the target and sets `replicas`; scale-in waits 300 s of stable lower recommendations | api: CPU 70% of the request, 2..5 |
| metrics-server | where the HPA gets CPU from. On kind it needs `--kubelet-insecure-tls` | `deploy/k8s/platform/values/metrics-server.yaml` |
| when **not** to autoscale | a consumer group: a member beyond the partition count owns no partition, and CPU does not measure lag | consumer: fixed 2, never above 6 (partitions of `payments.main`) |

## Troubleshooting — which command answers which question

| Question | Command |
|---|---|
| what state are the pods in? | `kubectl get pods -n lab -o wide` |
| why is this pod not running? | `kubectl describe pod <pod> -n lab` — read **Events** at the bottom first |
| what did it print before it died? | `kubectl logs <pod> -n lab --previous` |
| what happened in the namespace lately? | `kubectl get events -n lab --sort-by=.lastTimestamp` |
| is it receiving traffic? | `kubectl get endpointslices -n lab -l kubernetes.io/service-name=<svc>` |
| is the rollout progressing? | `kubectl rollout status deploy/<name> -n lab`, `helm history payments -n lab` |
| how much does it use? | `kubectl top pods -n lab` |
| shell in a distroless pod? | there is none — `kubectl debug -it <pod> --image=busybox:1.37 --target=<container>` |

The failure modes these commands were run against, with their recorded output, are in
[runbook.md](runbook.md).
