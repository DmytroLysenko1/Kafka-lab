# 01 — Docker images for the Go services

**The same payments-api goes from 1.66 GB to 23.2 MB, and a rebuild after editing one `.go`
file from 22–28 s to about 2 s** (28.3 → 1.9 s and 22.0 → 2.2 s in the two logged runs). Nothing in the service changed to get there: every number
below comes from the Dockerfile alone, measured on this repository (arm64, Docker 29.3,
BuildKit). `make image-measure` reproduces the sizes and the rebuild times and keeps its log
in `deploy/docker/results/` — `measure-20260927-211030.log` and `measure-20260928-040407.log`,
the two runs behind every pair of numbers here — which also hold the binary sizes, the six
images and the distroless shell refusal quoted below.

The headline is one comparison, not one technique: it changes the base image (Debian
`golang:1.26.4` against `golang:1.26.4-alpine` for the build and distroless for the run) and
the number of stages at the same time. The table under *What each technique bought* says
which of the techniques were measured on their own and which were not.

Files: [`deploy/docker/Dockerfile`](../../deploy/docker/Dockerfile) (the real one, all
six commands through `--build-arg SERVICE=…`),
[`deploy/docker/Dockerfile.naive`](../../deploy/docker/Dockerfile.naive) (the "before",
never deployed), [`.dockerignore`](../../.dockerignore).

## Before and after

| | naive (`golang:1.26.4`, Debian, one stage) | multi-stage → distroless |
|---|---|---|
| payments-api image (unpacked, as `docker image ls` shows it) | **1.66 GB** | **23.2 MB** |
| what ships | Go toolchain, module cache, the sources, a shell, apt | one static binary |
| user | root | `nonroot` (uid 65532) |
| rebuild after editing one `.go` file | 28.3 s / 22.0 s (modules downloaded again, everything recompiled) | 1.9 s / 2.2 s (module and build caches reused) |

All six images built by `make images`:

| image | size |
|---|---|
| outbox-relay | 29.0 MB |
| payments-consumer | 28.4 MB |
| payments-api | 23.2 MB |
| dlq-replayer | 22.0 MB |
| migrate | 19.1 MB |
| prune | 18.8 MB |

The relay and the consumer are the largest because they link franz-go and the protobuf
runtime *and* pgx and the Prometheus client; the replayer has franz-go but no database, the
API has the database but no Kafka, and migrate links only pgx and its embedded SQL.

## What each technique bought

| Technique | Where | Effect | Measured on its own |
|---|---|---|---|
| multi-stage build | `FROM golang… AS build` → `FROM distroless` | the toolchain stays in the build stage (`golang:1.26.4-alpine` here; the naive file ships the Debian-based `golang:1.26.4` whole — most of its 1.66 GB); only `/out/service` is copied out | no — it is part of the headline comparison |
| `CGO_ENABLED=0` | build `RUN` | a static binary with no libc dependency — the precondition for `distroless/static`, which has no libc | no |
| `-ldflags="-s -w"` | build `RUN` | binary 18,395,337–18,401,912 → 12,583,074 bytes (−32%, two runs): symbol table and DWARF dropped. Panics still print file:line — the runtime keeps its own pc→line table | yes, binary sizes in both logs |
| `-trimpath` | build `RUN` | no `/src/...` or `$HOME` paths from the build machine inside the binary; builds are reproducible across machines | no |
| `go.mod`/`go.sum` copied before the sources | build stage | `go mod download` sits in a layer that survives every code edit | together with the cache below |
| `RUN --mount=type=cache` (module + build cache) | build `RUN` | the 22–28 s → ~2 s rebuild. The cache lives in BuildKit, not in a layer, so it never reaches the image | yes, rebuild times in both logs |
| `.dockerignore` | repo root | `docs/`, `experiments/`, `deploy/`, `.git` never enter the context, and editing them does not invalidate `COPY . .` | no |
| `ARG SERVICE` | both stages | one file for six commands; the shared build stage downloads modules once for all of them | no |
| `distroless/static-debian12:nonroot` | runtime stage | no shell, no package manager: `docker run --entrypoint sh` fails with `exec: "sh": executable file not found` | yes, the refusal is in both logs |
| `USER nonroot` | runtime stage | the process runs as uid 65532, not root, so it can change only what that user is allowed to; it is `readOnlyRootFilesystem` in Kubernetes, next to `runAsNonRoot`, that closes the image's own filesystem to it | no |

## What distroless costs

There is no shell to `exec` into. `kubectl exec -it pod -- sh` does not work, and that is the
point. Debugging goes through an ephemeral container that shares the pod's process
namespace — `kubectl debug -it -n lab <pod> --image=busybox:1.37 --target=<container> -- sh` — which the
runbook uses for the broken-deployment drills. For the same reason a Kubernetes `preStop`
hook cannot be `exec: sleep 5`: there is no `sleep` binary, so the chart uses the native
`preStop.sleep` action instead.

## Compose for local multi-service development

The services also run as containers next to the stand, behind the compose `app` profile —
so `make up` still starts only Kafka, Postgres, the registry and monitoring, and the
experiments keep running the services from a terminal where they can be killed at will.

```
make up && make topics
PAYMENTS_API_KEY=… make app-up      # migrate runs once, then api, relay, consumer
make app-down
```

- `migrate` is a one-shot service; the other three start only after it exited 0
  (`service_completed_successfully`) — the compose version of "migrations are a command,
  not something a service does on boot".
- The containers publish the same loopback ports the host-run services use (8081, 9101–9103),
  so Prometheus scrapes `host.docker.internal:910x` in both modes and its config does not
  fork. The price: host-run and containerised services cannot run at the same time.
- Inside the containers the services bind `0.0.0.0` via `PAYMENTS_API_ADDR`/`METRICS_ADDR`;
  the code's loopback default stays for the host-run mode.

Checked by hand on 27.09.2026 (from the session, no log kept): `make app-up`, a POST `/payments` answered 201, the merchant total
read 1999 four seconds later (API → outbox → relay → Kafka → consumer → Postgres), and
`up{job="payments"}` was 1 for all three services.
