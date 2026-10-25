#!/usr/bin/env bash
# The numbers of docs/k8s/01-docker.md, measured: image size of payments-api built by the
# naive and by the real Dockerfile, what each layer of both holds, how long each takes to
# rebuild after one .go file changed — and the real Dockerfile with its two caching
# techniques taken out one at a time, so each is priced on its own rather than together with
# the change of base image. The edits are made in a copy of the repository, never in the
# working tree.
set -euo pipefail

cd "$(dirname "$0")/../.."
readonly service=payments-api
readonly log="deploy/docker/results/measure-$(date +%Y%m%d-%H%M%S).log"
mkdir -p deploy/docker/results
exec > >(tee "$log") 2>&1

context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
rsync -a --exclude .git ./ "$context/"
dockerfiles="$context/deploy/docker"

# The real Dockerfile's two caching techniques, removed one at a time. Without the split the
# modules are fetched by the build's own RUN, after COPY . ., so an edit invalidates the step
# that holds them — they are then downloaded again unless the module cache mount still has
# them, which it does in no-split. Without the mounts the module and build caches live only
# in layers: a layer that survives the edit, or none. Removing a mount leaves an empty
# continuation line, which the last expression drops.
readonly unmount='s/--mount=type=cache,target=[^ ]* //g'
sed -e '/^COPY go.mod go.sum/d' -e '/go mod download$/d' "$dockerfiles/Dockerfile" > "$dockerfiles/Dockerfile.no-split"
sed -e "$unmount" -e '/^ *\\$/d' "$dockerfiles/Dockerfile" > "$dockerfiles/Dockerfile.no-mounts"
sed -e "$unmount" -e '/^ *\\$/d' "$dockerfiles/Dockerfile.no-split" > "$dockerfiles/Dockerfile.neither"

# build prints the wall-clock seconds and how many steps BuildKit reused, and keeps the plain
# progress output, so the log can name the layer that was or was not taken from cache.
build() {
  local dockerfile=$1 tag=$2 started output
  output="$context/build-$tag.out"
  started=$(python3 -c 'import time; print(time.time())')
  # Checked by hand: errexit does not reach into the $(build …) that reports a time, and a
  # failed build would print the time it took to fail.
  if ! docker build --progress=plain -f "$dockerfiles/$dockerfile" --build-arg SERVICE=$service \
    -t "kafka-lab/$service:measure-$tag" "$context" >"$output" 2>&1; then
    echo "build of $dockerfile failed:" >&2
    tail -20 "$output" >&2
    exit 1
  fi
  python3 -c "import time; print(f'{time.time() - $started:.1f}s', end='')"
  printf ', %s steps CACHED' "$(grep -c ' CACHED$' "$output")"
  if grep -q 'go mod download' "$output"; then
    grep -E '^#[0-9]+ \[build [0-9]+/[0-9]+\] RUN .*go mod download' -A1 "$output" | grep -q CACHED \
      && printf ', module download CACHED' || printf ', module download RAN'
  fi
  echo
}

edit() {
  echo "// measure.sh: edit $1" >> "$context/internal/interfaces/http/server.go"
}

echo "measured $(date -u +%FT%TZ) on $(uname -m), docker $(docker version --format '{{.Server.Version}}'), $(go version | awk '{print $3}') on the host"
echo
echo "rebuild after editing one .go file — each Dockerfile warmed up first, the warm-up not reported:"
for variant in naive:Dockerfile.naive real:Dockerfile no-split:Dockerfile.no-split no-mounts:Dockerfile.no-mounts neither:Dockerfile.neither; do
  tag=${variant%%:*} file=${variant#*:}
  build "$file" "$tag" >/dev/null
  edit "$tag"
  printf '  %-10s %s\n' "$tag" "$(build "$file" "$tag")"
done
echo "  (real = go.mod split + cache mounts; no-split and no-mounts take one away; neither, both)"
echo

echo "image sizes (docker image ls, unpacked) and the same in bytes (docker image inspect .Size):"
for tag in naive real; do
  printf '  %-6s %s  %s bytes\n' "$tag" \
    "$(docker image ls --format '{{.Size}}' "kafka-lab/$service:measure-$tag")" \
    "$(docker image inspect --format '{{.Size}}' "kafka-lab/$service:measure-$tag")"
done
base=$(sed -n 's/^FROM \(gcr.io\/distroless[^ ]*\)$/\1/p' deploy/docker/Dockerfile)
# BuildKit keeps the base in its own store, not among the images, so it is pulled by its digest.
docker pull -q "$base" >/dev/null
echo "  the real image's base alone, $base: $(docker image inspect --format '{{.Size}}' "$base") bytes"
echo

for tag in naive real; do
  echo "layers of the $tag image (docker history):"
  docker history --no-trunc --format '  {{.Size}}\t{{.CreatedBy}}' "kafka-lab/$service:measure-$tag" \
    | cut -c1-150
  echo
done

echo "the same binary with and without -s -w, both -trimpath, linux/$(go env GOARCH), host $(go version | awk '{print $3}') (bytes):"
GOOS=linux CGO_ENABLED=0 go build -trimpath -o "$context/plain" ./cmd/$service
GOOS=linux CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "$context/stripped" ./cmd/$service
echo "  plain $(wc -c < "$context/plain" | tr -d ' ')  stripped $(wc -c < "$context/stripped" | tr -d ' ')"
echo

echo "every command's image at IMAGE_TAG=${IMAGE_TAG:-unset} (make images):"
for command in payments-api outbox-relay payments-consumer migrate dlq-replayer prune; do
  docker image ls --format "  {{.Repository}}:{{.Tag}}  {{.Size}}" "kafka-lab/$command:${IMAGE_TAG:-none}"
done
echo

echo "who the real image runs as, and a shell in it:"
echo "  User: $(docker image inspect --format '{{.Config.User}}' "kafka-lab/$service:measure-real")"
docker run --rm --entrypoint sh "kafka-lab/$service:measure-real" -c true 2>&1 | sed 's/^/  /' | head -2 || true
echo "  the naive image runs as: '$(docker image inspect --format '{{.Config.User}}' "kafka-lab/$service:measure-naive")' (empty is root)"
