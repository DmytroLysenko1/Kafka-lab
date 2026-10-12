# One file builds every command under cmd/: docker build --build-arg SERVICE=payments-api.
# The build stage is shared, so the module download and the build cache are paid for once
# however many services are built after it.

# Both bases pinned by digest: a tag moves when its maintainers rebuild it, and an image that
# changes under the same Dockerfile is a size or rebuild measurement that cannot be repeated.
# The GO_VERSION build arg names the version the digest is for; change both together.
ARG GO_VERSION=1.26.4
ARG GO_DIGEST=sha256:3ad57304ad93bbec8548a0437ad9e06a455660655d9af011d58b993f6f615648

FROM golang:${GO_VERSION}-alpine@${GO_DIGEST} AS build
WORKDIR /src

# go.mod and go.sum change far less often than the code: copied alone, they keep the
# downloaded modules in a layer that survives every edit to a .go file.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG SERVICE
# CGO off gives a static binary that needs no libc, which is what lets the runtime stage be
# distroless/static. -trimpath keeps the build machine's paths out of the binary, and
# -s -w drop the symbol table and DWARF, which a production stack trace does not use.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    test -n "${SERVICE}" && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${SERVICE}

# No shell, no package manager, uid 65532: there is nothing in the image for an intruder to
# run, and debugging goes through an ephemeral container (kubectl debug) instead.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG SERVICE
LABEL org.opencontainers.image.source="https://github.com/DmytroLysenko1/Kafka-lab" \
      org.opencontainers.image.title="${SERVICE}"
COPY --from=build /out/service /service
USER nonroot:nonroot
ENTRYPOINT ["/service"]
