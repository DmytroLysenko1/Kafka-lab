#!/usr/bin/env bash
# The whole evidence set in one run: platform applied, images built and deployed, the Docker
# measurement, every drill, and the resource log that the "Figures:" comments cite. Runs
# under caffeinate, because a Mac that sleeps mid-drill (it did, on battery, every ~15 min)
# leaves a log whose tail measures the sleep. Keep the lid open; on battery caffeinate -i
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

make k8s-up
make k8s-images
make k8s-deploy
# Drill 01 needs a second tag to roll to. It is the same build under another name, so the
# drill measures the rollout mechanics, not a change of code (runbook.md says so).
for command in payments-api outbox-relay payments-consumer migrate dlq-replayer prune; do
  docker tag "kafka-lab/$command:$IMAGE_TAG" "kafka-lab/$command:$next"
  kind load docker-image "kafka-lab/$command:$next" --name kafka-lab >/dev/null
done

# Each drill starts on a settled stand: the API server answering and every Deployment in lab
# at its desired count. After drill 03's load the control plane took minutes to answer again
# on 25.10, and the next drill failed on its first helm call to a cluster that was not there.
settle() {
  until kubectl get --raw /readyz >/dev/null 2>&1; do sleep 5; done
  kubectl wait deployment --all -n lab --for=condition=Available --timeout=10m >/dev/null
}

make image-measure
settle; IMAGE_TAG_NEXT=$next make k8s-drill-01
settle; make k8s-drill-02
# 800, not the 1 200 of the 28.09 sessions: on 25.10, 1 200 req/s took Docker Desktop's
# daemon down twice in a row on this 8 GB VM, ending the session. 800 still drives the API's
# two pods well past the HPA's 70% (drill 03's own log shows by how much).
settle; RATE=800 make k8s-drill-03
settle; make k8s-drill-04
settle; make k8s-drill-04f
settle; make k8s-drill-05
settle; make k8s-drill-06
settle; make k8s-drill-07

minutes=$(( ($(date +%s) - started) / 60 + 5 ))
WINDOW="${minutes}m" deploy/k8s/resources.sh
echo "evidence session done $(date -u +%FT%TZ)"
