#!/usr/bin/env bash
# The whole evidence set in one run: platform applied, images built and deployed, the Docker
# measurement, every drill, and the resource log that the resource figures cite. Runs
# under caffeinate, because a Mac that sleeps mid-drill (it did, on battery, every ~15 min —
# pmset's log, seen in the session) leaves a log whose tail measures the sleep. Keep the lid open; on battery caffeinate -i
# holds off idle sleep only.
set -euo pipefail
cd "$(dirname "$0")/../../.."

if [ -z "${CAFFEINATED:-}" ]; then
  exec env CAFFEINATED=1 caffeinate -dims "$0" "$@"
fi

# One tag for the whole session: the tag hashes the working tree, and a drill that restores
# "the current tag" must mean the tag that was built, not one computed after a file changed.
IMAGE_TAG=$(deploy/docker/image-tag.sh)
export IMAGE_TAG
readonly next="$IMAGE_TAG-next"
readonly started=$(date +%s)
echo "evidence session $(date -u +%FT%TZ), images $IMAGE_TAG"

# The Docker measurement first, with the kind nodes stopped: next to a running stand, three
# rebuilds of the same Dockerfile took 302, 490 and 1159 s on 26.10, and the builds filled the
# VM's swap and set off the kernel's OOM killer before drill 01 began (seen in the session;
# those logs were deleted). It needs only Docker. Stopped containers keep their state, so the
# nodes come back as they were.
nodes=$(kind get nodes --name kafka-lab 2>/dev/null || true)
[ -z "$nodes" ] || docker stop $nodes >/dev/null
make images          # the six images measure.sh lists; k8s-images below reuses them
make image-measure
if [ -n "$nodes" ]; then
  docker start $nodes >/dev/null
  until kubectl get --raw /readyz >/dev/null 2>&1; do sleep 5; done
  kubectl wait node --all --for=condition=Ready --timeout=10m >/dev/null
fi

make k8s-up
make k8s-images
make k8s-load-image
make k8s-deploy
# Drill 01 needs a second tag to roll to. It is the same build under another name, so the
# drill measures the rollout mechanics, not a change of code (runbook.md says so).
for command in payments-api outbox-relay payments-consumer migrate dlq-replayer prune; do
  docker tag "kafka-lab/$command:$IMAGE_TAG" "kafka-lab/$command:$next"
  kind load docker-image "kafka-lab/$command:$next" --name kafka-lab >/dev/null
done

# Each drill starts on a settled stand: the API server answering and every Deployment in lab
# at its desired count. After drill 03's load the control plane took minutes to answer again
# on 25.10 (seen in the session; that attempt's logs were deleted), and the next drill failed
# on its first helm call to a cluster that was not there.
settle() {
  until kubectl get --raw /readyz >/dev/null 2>&1; do sleep 5; done
  kubectl wait deployment --all -n lab --for=condition=Available --timeout=10m >/dev/null
}

settle; IMAGE_TAG_NEXT=$next make k8s-drill-01
settle; make k8s-drill-02
# 800, not the 1 200 of the 28.09 sessions. On 25.10, sent from the host, drill 03's load was
# running when two attempts at this session ended; in the one whose Docker logs survive (800
# req/s, 18:40 UTC) Docker Desktop's port forward on 127.0.0.1:8800 shut down 11 s into the
# load, and the VM stopped a minute later. Drills 03 and 06 now send from a pod in the cluster
# through Traefik (lib.sh).
settle; RATE=800 make k8s-drill-03
settle; make k8s-drill-04
settle; make k8s-drill-04f
settle; make k8s-drill-05
settle; make k8s-drill-06
settle; make k8s-drill-07

minutes=$(( ($(date +%s) - started) / 60 + 5 ))
WINDOW="${minutes}m" deploy/k8s/resources.sh
echo "evidence session done $(date -u +%FT%TZ)"
