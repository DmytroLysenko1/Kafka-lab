# 01 — Docker images for the Go services

**The same payments-api goes from 1.92 GB to 33.7 MB, and a rebuild after editing one `.go`
file from 39.5 s to 10.9 s.** Nothing in the service changed to get there: every number below,
but those of the earlier sessions said so, comes from the Dockerfile alone, measured on this
repository by `make image-measure` (arm64, Docker 29.8.2, BuildKit) in one log,
[`measure-20261026-035012.log`](../../deploy/docker/results/measure-20261026-035012.log),
which also holds the layer listings, the binary sizes, the six images and the distroless
shell refusal quoted below.

Each rebuild was made three times and the median is quoted; the three are within 1.5 s of
each other for every Dockerfile but the naive one (35.9–40.3 s). They were taken at the start
of the evidence session after it stopped the kind nodes (`deploy/k8s/drills/session.sh`), so
the stand did not run beside them; the log itself does not record that. Earlier sessions
measured next to the running stand: with one rebuild each, the naive Dockerfile took 136.5 s
in one (`measure-20261025-215950.log`) and 13.7 s in the next; with three, `neither` took 302,
490 and 1159 s in a third, when the builds also pushed the VM into swap (those two logs were
deleted with their failed sessions; the figures are from the session's output). A rebuild time is a measure of
the machine as much as of the Dockerfile unless the machine is otherwise idle.

The headline is one comparison, not one technique: it changes the base image (Debian
`golang:1.26.4` against `golang:1.26.4-alpine` for the build and distroless for the run), the
number of stages, the build flags and the caching at the same time. *What each technique
bought* prices the two caching techniques on their own; the rest is said to be unseparated
where it is.

Files: [`deploy/docker/Dockerfile`](../../deploy/docker/Dockerfile) (the real one, all
six commands through `--build-arg SERVICE=…`),
[`deploy/docker/Dockerfile.naive`](../../deploy/docker/Dockerfile.naive) (the "before",
never deployed), [`.dockerignore`](../../.dockerignore),
[`deploy/docker/measure.sh`](../../deploy/docker/measure.sh) (the measurement).

## Before and after

| | naive (`golang:1.26.4`, Debian, one stage) | multi-stage → distroless |
|---|---|---|
| payments-api image (`docker image ls`) | **1.92 GB** (1,924,639,921 bytes) | **33.7 MB** (33,713,303 bytes) |
| what ships | Go toolchain, the sources, the build cache, a shell, apt | one static binary on distroless |
| user | root (`Config.User` empty) | `nonroot:nonroot` (uid 65532) |
| rebuild after editing one `.go` file, median of 3 | 39.5 s | 10.9 s |

Where the size goes, by `docker history`:

- **naive** — the `golang:1.26.4` base is five large layers, 156 + 63.7 + 208 + 280 + 277 MB (and two of a few kB) (the
  Debian root, three `apt-get install` layers — compilers, VCS tools — and the Go toolchain);
  `COPY . .` adds 19.2 MB of sources and the `go build` layer 487 MB, the build and module
  caches written into the image along with the binary.
- **real** — the binary, 20.6 MB, on distroless's layers (4.24 MB of them tzdata, 582 kB
  base-files, 319 kB CA certificates, the rest under 200 kB each). The base alone, pulled by
  the Dockerfile's digest, is 6,177,026 bytes.

The layers listed add up to less than `docker image ls` shows for either image (about 1.49 GB
and 26 MB); the log has both, and the headline quotes `docker image ls`, which is what a
reader running `make image-measure` sees.

All six images built by `make images`, at the session's tag (`measure.sh` lists them but does
not build them: run `make images` first, as the session does):

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

The rebuild after one edited `.go` file, three times each after a warm-up (`measure.sh`
derives the three variants from the real Dockerfile with `sed`, in a copy of the repository).
The log also times the `go build` step and the image export inside each rebuild:

| Dockerfile | go.mod split | cache mounts | rebuild, median | `go build` step | module download |
|---|---|---|---|---|---|
| real | yes | yes | 10.9 s | 0.5 s | cached |
| no-split | — | yes | 11.0 s | 0.5 s | in the build step |
| no-mounts | yes | — | 25.2 s | 8.7–9.9 s | cached |
| neither | — | — | 25.5 s | 13.5–15.1 s | in the build step |
| naive | — | — | 39.5 s | 13.6–16.9 s, then 10.7–12.2 s of export | in the build step |

- **Cache mounts** do the work: with them the `go build` step takes 0.5 s instead of 9–15,
  because the build cache survives the edit; the rebuild falls from 25.2 to 10.9 s. They live
  in BuildKit, not in a layer, so they never reach the image.
- **The go.mod split** changes nothing measurable with the mounts (10.9 against 11.0 s) —
  the module cache mount already keeps the modules. Without them it takes the download out of
  the build step (13.5–15.1 → 8.7–9.9 s), yet the whole rebuild took as long (25.2 against
  25.5 s); the log does not show where those seconds went. It is kept for a build whose mounts
  start empty — a fresh builder — where the layer is what still saves the download; that case
  was not measured.
- **Much of every rebuild is outside both timed steps**: 10.3–11.7 s for real, no-split and
  neither (10.4 s of real's 10.9), 10.6–14.5 s for naive, 15.2–16.4 s for no-mounts — which is where the split's
  seconds went, though the log does not break it down. BuildKit's handling of the build
  context is the likely part, not a measured one.
- **neither → naive** (25.5 → 39.5 s) is mostly the export: the naive image writes its 487 MB
  build layer every time (10.7–12.2 s), the real one a 20.6 MB binary (0.0 s). The base
  image, the single stage and the build flags (the naive file builds with cgo on, as Debian
  ships gcc) are not separated by the log.

| Technique | Where | Effect | Measured on its own |
|---|---|---|---|
| multi-stage build | `FROM golang… AS build` → `FROM distroless` | the toolchain stays in the build stage; only `/out/service` is copied out — the naive image's 985 MB base and 487 MB build layer stay behind, and so does the 11 s it takes to export that layer | no — part of the headline comparison |
| `CGO_ENABLED=0` | build `RUN` | a static binary with no libc dependency — the precondition for `distroless/static`, which has no libc | no |
| `-ldflags="-s -w"` | build `RUN` | binary 29,797,405 → 20,578,466 bytes (−31%), both `-trimpath`, same Go as the image: symbol table and DWARF dropped. Panics still print file:line — the runtime keeps its own pc→line table | yes |
| `-trimpath` | build `RUN` | no `/src/...` or `$HOME` paths from the build machine inside the binary; builds are reproducible across machines | no |
| `go.mod`/`go.sum` copied before the sources | build stage | the download out of the build step without cache mounts, the rebuild no faster; nothing with them | yes, the table above |
| `RUN --mount=type=cache` (module + build cache) | build `RUN` | `go build` step 9–15 s → 0.5 s; rebuild 25.2 → 10.9 s | yes, the table above |
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
