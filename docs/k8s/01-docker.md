# 01 — Docker images for the Go services

**The same payments-api goes from 1.92 GB to 33.7 MB, and a rebuild after editing one `.go`
file from 136.5 s to 14.6 s.** Nothing in the service changed to get there: every number
below comes from the Dockerfile alone, measured on this repository by `make image-measure`
(arm64, Docker 29.8.2, BuildKit) in one log,
[`measure-20261025-215950.log`](../../deploy/docker/results/measure-20261025-215950.log),
which also holds the layer listings, the binary sizes, the six images and the distroless
shell refusal quoted below.

The rebuild times were taken inside the evidence session (runbook.md), with the whole kind
stand running on the same 10-CPU VM, so they are slow in absolute terms; what they compare is
the Dockerfiles, built one after another under the same load. The headline is one comparison,
not one technique: it changes the base image (Debian `golang:1.26.4` against
`golang:1.26.4-alpine` for the build and distroless for the run), the number of stages, the
build flags and the caching at the same time. *What each technique bought* prices the two
caching techniques on their own; the rest is said to be unseparated where it is.

Files: [`deploy/docker/Dockerfile`](../../deploy/docker/Dockerfile) (the real one, all
six commands through `--build-arg SERVICE=…`),
[`deploy/docker/Dockerfile.naive`](../../deploy/docker/Dockerfile.naive) (the "before",
never deployed), [`.dockerignore`](../../.dockerignore),
[`deploy/docker/measure.sh`](../../deploy/docker/measure.sh) (the measurement).

## Before and after

| | naive (`golang:1.26.4`, Debian, one stage) | multi-stage → distroless |
|---|---|---|
| payments-api image (`docker image ls`) | **1.92 GB** (1,924,669,463 bytes) | **33.7 MB** (33,713,303 bytes) |
| what ships | Go toolchain, the sources, the build cache, a shell, apt | one static binary on distroless |
| user | root (`Config.User` empty) | `nonroot:nonroot` (uid 65532) |
| rebuild after editing one `.go` file | 136.5 s | 14.6 s |

Where the size goes, by `docker history`:

- **naive** — the `golang:1.26.4` base is five layers, 156 + 63.7 + 208 + 280 + 277 MB (the
  Debian root, three `apt-get install` layers — compilers, VCS tools — and the Go toolchain);
  `COPY . .` adds 19.2 MB of sources and the `go build` layer 487 MB, the build and module
  caches written into the image along with the binary.
- **real** — the binary, 20.6 MB, on distroless's layers (4.24 MB of them tzdata, 582 kB
  base-files, 319 kB CA certificates, the rest under 200 kB each). The base alone, pulled by
  the Dockerfile's digest, is 6,177,026 bytes.

The layers listed add up to less than `docker image ls` shows for either image (about 1.49 GB
and 26 MB); the log has both, and the headline quotes `docker image ls`, which is what a
reader running `make image-measure` sees.

All six images built by `make images`, at the session's tag:

| image | size |
|---|---|
| outbox-relay | 38.4 MB |
| payments-consumer | 38.4 MB |
| payments-api | 33.7 MB |
| dlq-replayer | 30.1 MB |
| prune | 30.1 MB |
| migrate | 28.3 MB |

The relay and the consumer are the largest: they link franz-go and the protobuf runtime *and*
pgx and the Prometheus client (`go list -deps`). migrate, the smallest, links pgx and its
embedded SQL and neither the Kafka nor the Prometheus client.

## What each technique bought

The rebuild after one edited `.go` file, each Dockerfile warmed up first (`measure.sh`
derives the three variants from the real Dockerfile with `sed`, in a copy of the repository):

| Dockerfile | go.mod split | cache mounts | rebuild | module download |
|---|---|---|---|---|
| real | yes | yes | 14.6 s | cached |
| no-split | — | yes | 14.3 s | in the build step |
| no-mounts | yes | — | 49.5 s | cached |
| neither | — | — | 68.1 s | in the build step |
| naive | — | — | 136.5 s | in the build step |

- **Cache mounts** are what makes the rebuild fast: 68.1 → 14.3 s without the split, 49.5 →
  14.6 s with it. The module and build caches live in BuildKit, not in a layer, so they never
  reach the image.
- **The go.mod split** pays only without the mounts: 68.1 → 49.5 s, because the downloaded
  modules then survive in a layer. With the mounts the module cache survives anyway, and the
  split changes nothing measurable (14.6 against 14.3 s). It is kept for a build whose mounts
  start empty — a fresh builder — where the layer is what still saves the download; that
  case was not measured.
- **neither → naive** (68.1 → 136.5 s) is the base image, the single stage and the build
  flags together — the naive file builds with cgo on (Debian ships gcc) and writes a 487 MB
  layer. The log does not separate them.

| Technique | Where | Effect | Measured on its own |
|---|---|---|---|
| multi-stage build | `FROM golang… AS build` → `FROM distroless` | the toolchain stays in the build stage; only `/out/service` is copied out — the naive image's 985 MB base and 487 MB build layer stay behind | no — part of the headline comparison |
| `CGO_ENABLED=0` | build `RUN` | a static binary with no libc dependency — the precondition for `distroless/static`, which has no libc | no |
| `-ldflags="-s -w"` | build `RUN` | binary 29,797,405 → 20,578,466 bytes (−31%), both `-trimpath`, same Go as the image: symbol table and DWARF dropped. Panics still print file:line — the runtime keeps its own pc→line table | yes |
| `-trimpath` | build `RUN` | no `/src/...` or `$HOME` paths from the build machine inside the binary; builds are reproducible across machines | no |
| `go.mod`/`go.sum` copied before the sources | build stage | 68.1 → 49.5 s without cache mounts; nothing with them | yes, the table above |
| `RUN --mount=type=cache` (module + build cache) | build `RUN` | 68.1 → 14.3 s, or 49.5 → 14.6 s with the split | yes, the table above |
| `.dockerignore` | repo root | `docs/`, `experiments/`, `deploy/`, `.git` never enter the context, and editing them does not invalidate `COPY . .` | no |
| `ARG SERVICE` | both stages | one file for six commands; the shared build stage downloads modules once for all of them | no |
| `distroless/static-debian12:nonroot` | runtime stage | no shell, no package manager: `docker run --entrypoint sh` fails with `exec: "sh": executable file not found in $PATH` | yes, the refusal is in the log |
| `USER nonroot` | runtime stage | the process runs as uid 65532, not root, so it can change only what that user is allowed to; it is `readOnlyRootFilesystem` in Kubernetes, next to `runAsNonRoot`, that closes the image's own filesystem to it | the user is in the log |

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

Checked by hand on 27.09.2026, and not since; **no log was kept**, so this is the one claim
on this page without one: `make app-up`, a POST `/payments` answered 201, the merchant total
read 1999 four seconds later (API → outbox → relay → Kafka → consumer → Postgres), and
`up{job="payments"}` was 1 for all three services.
