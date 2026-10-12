# Kubernetes & Containerization — the PDP block on this repository

The payments service from the Kafka block, containerised and run on a local Kubernetes
cluster (kind) next to the Kafka cluster it depends on, which runs there too, through the
Strimzi operator. Nothing here is a hello-world: the service moves money through an outbox,
Kafka and a consumer, so rollouts, probes and broken releases have real consequences to
measure.

## KR → artifact

| KR | Deliverable in the PDP | Where |
|---|---|---|
| Udemy "Docker & Kubernetes: The Practical Guide" (10%) | certificate + course code + notes | outside this repository |
| Docker for Go services (5%) | optimized Dockerfile(s), before/after image size, local Compose | [01-docker.md](01-docker.md), `deploy/docker/`, the `app` profile in `deploy/docker-compose.yml` |
| Kubernetes fundamentals (5%) | notes / cheat sheet + a Go service on local Kubernetes with manifests | [cheatsheet.md](cheatsheet.md), `deploy/k8s/`, `deploy/helm/payments/` |
| Deploy and operate end-to-end (5%) | manifests / Helm chart + runbook: deploy, upgrade, rollback, troubleshooting | `deploy/helm/payments/`, [runbook.md](runbook.md), `deploy/k8s/drills/` |
| Share learnings (0%) | write-up or tech talk | [decisions.md](decisions.md) |

## Running it

```
make stop                 # the compose stand and the kind stand do not fit in Docker together
make k8s-up               # kind + platform: Traefik, metrics-server, Prometheus/Grafana, Loki/Alloy,
                          # Strimzi (Kafka 4.3.1, 3 KRaft nodes), CloudNativePG, Apicurio
make k8s-images           # build the six images and load them into the kind nodes
make k8s-deploy           # helm lint + kubeconform, the API key secret, helm upgrade --install
helm test payments -n lab

KEY=$(kubectl get secret payments-api-key -n lab -o jsonpath='{.data.PAYMENTS_API_KEY}' | base64 -d)
curl -X POST http://127.0.0.1:8800/payments -H 'Host: payments.localtest.me' -H "X-API-Key: $KEY" \
     -H 'Idempotency-Key: k8s-1' -H 'Content-Type: application/json' \
     -d '{"merchant_id":"m-42","amount_minor":1999,"currency":"EUR"}'
curl http://127.0.0.1:8800/merchants/m-42/total -H 'Host: payments.localtest.me' -H "X-API-Key: $KEY"

open http://grafana.localtest.me:8800   # macOS; elsewhere open the URL in a browser
make k8s-down
```

`*.localtest.me` resolves to 127.0.0.1 in public DNS, so no `/etc/hosts` editing is needed;
the kind control plane maps host port 8800 to Traefik's NodePort (not 80 — see decisions.md).
The curl examples dial 127.0.0.1 with a `Host` header rather than the name: the name also
resolves to `::1`, where nothing listens, and a client that tries it first fails
(runbook, drill 03). Grafana opens the same payments dashboard as the compose stand, plus the
Loki logs under Explore.

## Tools, as the evidence was produced with them

Installed with Homebrew (`brew install kind helm kubectl kubeconform vegeta jq`), not pinned
by the repository: kind v0.33.0 (node image pinned by digest in `deploy/k8s/kind.yaml`,
Kubernetes v1.37.0), Helm v4.3.0 (server-side apply — see the field-ownership note in the
runbook), kubectl v1.37.1 (Docker Desktop ships v1.34.1, one minor version too far from the
1.37 server; the earliest drill logs were made with it), kubeconform v0.8.0, vegeta 12.13.0,
jq 1.7.1 (the drills read Helm and vegeta output with it), openssl (`make k8s-secret` draws the
API key with it), python3 (`deploy/k8s/resources.sh` and `deploy/docker/measure.sh` format their
tables with it), Docker 29.3.1 with 8 GB of memory for its VM. A different Helm major changes
how upgrades and conflicts behave; the other versions only change output formats.
`make k8s-verify` downloads the CRD schemas it validates against from GitHub, pinned to one
commit of the catalog, so it needs the network.

The whole evidence set — Docker measurement, every drill, the resource log — is one command:
`make k8s-evidence`. It runs under macOS's `caffeinate` (a Mac that slept mid-drill spoiled a
run), so elsewhere run the steps in `deploy/k8s/drills/session.sh` by hand; keep the machine on
power. Its duration is in the session's resource log header.

## Layout

| Path | What |
|---|---|
| `deploy/docker/Dockerfile` | one multi-stage file for all six commands (`--build-arg SERVICE=…`) |
| `deploy/k8s/kind.yaml` | 1 control plane + 2 workers, host port 8800 → ingress |
| `deploy/k8s/up.sh` | idempotent bring-up, every chart pinned |
| `deploy/k8s/platform/` | Helm values of the platform charts and the operators' custom resources |
| `deploy/helm/payments/` | the service's chart: Deployments, Services, Ingress, HPA, PDB, ServiceMonitor, the migration hook, the prune CronJob, `helm test` |
| `deploy/k8s/drills/` | the scripted drills behind the runbook (`make k8s-drill-NN`), results under `results/` |
