#!/usr/bin/env bash
# The numbers of docs/k8s/01-docker.md, measured: image size of payments-api built by the
# naive and by the real Dockerfile, and how long each takes to rebuild after one .go file
# changed. The edit is made in a copy of the repository, never in the working tree.
set -euo pipefail

cd "$(dirname "$0")/../.."
readonly service=payments-api
readonly log="deploy/docker/results/measure-$(date +%Y%m%d-%H%M%S).log"
mkdir -p deploy/docker/results
exec > >(tee "$log") 2>&1

context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
rsync -a --exclude .git ./ "$context/"

build() {
  local dockerfile=$1 tag=$2 started
  started=$(date +%s.%N 2>/dev/null || python3 -c 'import time; print(time.time())')
  docker build -q -f "$context/deploy/docker/$dockerfile" --build-arg SERVICE=$service \
    -t "kafka-lab/$service:measure-$tag" "$context" >/dev/null
  python3 -c "import time; print(f'{time.time() - $started:.1f}')"
}

echo "measured $(date -u +%FT%TZ) on $(uname -m), docker $(docker version --format '{{.Server.Version}}')"
echo "warm-up (fills the caches, not reported): naive $(build Dockerfile.naive naive)s, real $(build Dockerfile dev)s"

echo "// measure.sh: one edited line" >> "$context/internal/interfaces/http/server.go"
echo "rebuild after editing one .go file: naive $(build Dockerfile.naive naive)s"
echo "// measure.sh: a second edited line" >> "$context/internal/interfaces/http/server.go"
echo "rebuild after editing one .go file: real  $(build Dockerfile dev)s"

echo "image sizes (unpacked, as docker image ls reports them):"
# Tags of their own: the build from an edited copy must never overwrite an image a stand runs.
docker image ls --format '  {{.Repository}}:{{.Tag}}  {{.Size}}' "kafka-lab/$service" | grep -E ':measure-'

echo "the same binary, linked with and without -s -w (linux/$(go env GOARCH), bytes):"
GOOS=linux CGO_ENABLED=0 go build -o "$context/plain" ./cmd/$service
GOOS=linux CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "$context/stripped" ./cmd/$service
echo "  plain $(wc -c < "$context/plain")  stripped $(wc -c < "$context/stripped")"

echo "every command's image at IMAGE_TAG=${IMAGE_TAG:-unset} (make images):"
for command in payments-api outbox-relay payments-consumer migrate dlq-replayer prune; do
  docker image ls --format "  {{.Repository}}:{{.Tag}}  {{.Size}}" "kafka-lab/$command:${IMAGE_TAG:-none}"
done

echo "a shell in the distroless image:"
docker run --rm --entrypoint sh "kafka-lab/$service:measure-dev" -c true 2>&1 | sed 's/^/  /' | head -2 || true
