#!/usr/bin/env bash
# Drill 04 — seven ways a release breaks (or needs looking into), what each looks like, and the commands that find it.
# Each case upgrades the release with one bad value, records what kubectl shows, then rolls
# forward to the chart in git (see restore). Recorded output is the evidence for docs/k8s/runbook.md.
set -euo pipefail
cd "$(dirname "$0")"   # the chart path and lib.sh are relative to this directory
source ./lib.sh

readonly log="$results_dir/04-broken-deployments-$(date +%Y%m%d-%H%M%S).log"
readonly chart=../../helm/payments
# The tag every case restores to: the one deployed when the drill starts, read from the
# release rather than computed from the working tree — a tree that changed since the images
# were built (a new drill log is enough) names a tag that was never loaded into kind.
good_tag=$(deployed_tag)
readonly good_tag
# Checked before anything is broken: a tag that cannot be restored must stop the drill here,
# not after case a1 has left the release on a broken one.
require_loaded "$good_tag"
mkdir -p "$results_dir"
exec > >(tee "$log") 2>&1

section() { printf '\n==== %s — %s\n' "$1" "$(stamp)"; }

break_with() {
  # --wait would block until the timeout on a release that can never become ready; the
  # drill wants to watch it fail, so it gives it a fixed 45 s instead.
  helm upgrade payments "$chart" -n "$namespace" --reset-then-reuse-values "$@" >/dev/null
  sleep 45
}

# Restore rolls forward to the chart as git has it, not back to a revision number. Both
# obvious alternatives failed here: a bare `helm rollback` goes to revision current−1, which
# after two failed upgrades in a row is a failed one (the second run rolled the broken tag
# out that way, past the migration hook), and a revision recorded at the start was pruned by --history-max 10 before the
# drill came back to it (the fourth run: "release has no 13 version").
restore() {
  local tag=$good_tag
  require_loaded "$tag"
  # --force-conflicts: case a2 edits the Deployment with kubectl, which makes kubectl the
  # owner of the image field under server-side apply; without taking it back, the first
  # upgrade that changes the image fails with a field-manager conflict.
  helm upgrade payments "$chart" -n "$namespace" --reset-values --set image.tag="$tag" \
    --force-conflicts --wait --timeout 5m >/dev/null
  echo "restored: the chart from git, image tag $tag"
}

api_pods() {
  kubectl get pods -n "$namespace" -l app.kubernetes.io/name=payments-api \
    -o custom-columns=POD:.metadata.name,READY:.status.containerStatuses[0].ready,STATE:.status.containerStatuses[0].state,RESTARTS:.status.containerStatuses[0].restartCount,PHASE:.status.phase
}

newest_api_pod() {
  kubectl get pods -n "$namespace" -l app.kubernetes.io/name=payments-api \
    --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1:].metadata.name}'
}

still_serving() {
  printf 'API through the ingress while broken: '
  curl -s -o /dev/null -w '%{http_code}\n' -H "Host: $api_host" "$api/readyz"
}

section "a1. A tag that was never built, through helm — stopped at the migration hook"
# The migration Job runs the same tag first, as a pre-upgrade hook: it cannot pull, the hook
# never completes, and the release fails before a single Deployment is touched.
helm upgrade payments "$chart" -n "$namespace" --reset-then-reuse-values --set image.tag=does-not-exist \
  --timeout 60s 2>&1 | grep -m1 -E 'Error|failed' || true
kubectl get pods -n "$namespace" -l app.kubernetes.io/name=migrate \
  -o custom-columns=POD:.metadata.name,WAITING:.status.containerStatuses[0].state.waiting.reason
api_pods
still_serving
restore

section "a2. The same tag set on the Deployment directly — ImagePullBackOff"
# Out of band, past the hook: what a kubectl set image or another tool would do.
kubectl set image deploy/payments-api -n "$namespace" payments-api=kafka-lab/payments-api:does-not-exist >/dev/null
sleep 45
api_pods
kubectl describe pod -n "$namespace" "$(newest_api_pod)" | sed -n '/^Events:/,$p' | tail -4
still_serving
kubectl rollout undo deploy/payments-api -n "$namespace" >/dev/null
kubectl rollout status deploy/payments-api -n "$namespace" --timeout=3m >/dev/null
echo "rolled back with kubectl rollout undo; field managers now:"
kubectl get deploy payments-api -n "$namespace" --show-managed-fields -o json |
  jq -r '.metadata.managedFields[] | "  \(.manager) \(.operation)"'
# kubectl now co-owns the image field with Helm (same value), and --force-conflicts on the
# restore would not change that: a force only moves fields whose values conflict. Resetting
# managedFields to [{}] does not help either — the fields pass to "before-first-apply", which
# conflicts the same way (tried on a scratch Deployment, 27.09.2026). Removing kubectl's own
# entries does: Helm is left the only owner. Descending indices, so each removal leaves the
# next index where it was.
drop_kubectl=$(kubectl get deploy payments-api -n "$namespace" --show-managed-fields -o json |
  jq -c '[.metadata.managedFields | to_entries[] | select(.value.manager | startswith("kubectl")) | .key]
         | sort | reverse | map({op: "remove", path: "/metadata/managedFields/\(.)"})')
kubectl patch deploy payments-api -n "$namespace" --type=json -p "$drop_kubectl" >/dev/null
restore
echo "field managers after dropping kubectl's entries and the restore:"
kubectl get deploy payments-api -n "$namespace" --show-managed-fields -o json |
  jq -r '.metadata.managedFields[] | "  \(.manager) \(.operation)"'

section "b. CrashLoopBackOff — the process starts and exits"
# An address the API cannot listen on: the process starts, fails to bind and exits 1. Only
# the API reads it, so the migration hook passes and the failure lands on the Deployment.
break_with --set 'components.payments-api.env.PAYMENTS_API_ADDR=0.0.0.0:99999'
api_pods
kubectl logs -n "$namespace" "$(newest_api_pod)" --previous --tail=1 || true
still_serving
restore

section "c. CreateContainerConfigError — the Secret has no such key"
break_with --set secrets.apiKey.key=NO_SUCH_KEY
api_pods
kubectl describe pod -n "$namespace" "$(newest_api_pod)" | sed -n '/^Events:/,$p' | tail -3
still_serving
restore

section "d. OOMKilled — a memory limit below what the process needs"
break_with --set 'components.payments-api.resources.limits.memory=8Mi' --set 'components.payments-api.resources.requests.memory=8Mi'
api_pods
kubectl get pod -n "$namespace" "$(newest_api_pod)" \
  -o jsonpath='last state: {.status.containerStatuses[0].lastState.terminated.reason} exit {.status.containerStatuses[0].lastState.terminated.exitCode}{"\n"}'
still_serving
restore

section "e. Pending — requests no node can satisfy"
break_with --set 'components.payments-api.resources.requests.cpu=64'
api_pods
kubectl get events -n "$namespace" --field-selector reason=FailedScheduling --sort-by=.lastTimestamp | tail -2
still_serving
restore

section "f. Not ready — the database is down, the pods are fine"
kubectl annotate cluster payments-db -n "$namespace" --overwrite cnpg.io/hibernation=on >/dev/null
# Hibernation shuts Postgres down in smart mode, which waits for clients to disconnect, and
# the services' pools never do: the instance goes only when the smart shutdown times out
# (180 s by default), and 40 s after the annotation the API was still ready. Readiness then
# needs two failed probes 5 s apart. Waiting on both, not on a guessed pause.
kubectl wait --for=delete pod -l cnpg.io/cluster=payments-db -n "$namespace" --timeout=8m >/dev/null
echo "database pod gone $(stamp)"
sleep 15
api_pods
kubectl get endpointslices -n "$namespace" -l kubernetes.io/service-name=payments-api \
  -o jsonpath='{range .items[*].endpoints[*]}{.targetRef.name} ready={.conditions.ready}{"\n"}{end}'
still_serving
kubectl annotate cluster payments-db -n "$namespace" --overwrite cnpg.io/hibernation=off >/dev/null
kubectl wait --for=condition=Ready cluster/payments-db -n "$namespace" --timeout=5m >/dev/null
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=payments-api -n "$namespace" --timeout=2m >/dev/null
echo "database back, API ready again"

section "g. Debugging a distroless container — no shell, so an ephemeral one"
pod=$(newest_api_pod)
kubectl exec -n "$namespace" "$pod" -- sh -c true 2>&1 | tail -1 || true
# An ephemeral container joins the pod and, with --target, the process namespace of the
# service. `kubectl exec` cannot enter an ephemeral container, so it gets its command up
# front and the output is read from its log.
debugger=debugger-$(date +%s)   # ephemeral containers stay in the pod, so a name is used once
kubectl debug -n "$namespace" "$pod" --image=busybox:1.37@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e --target=payments-api -c "$debugger" --profile=general \
  -- sh -c 'ps -o pid,user,args; echo "--- the service image, seen from the debugger:"; ls /proc/1/root/' >/dev/null
kubectl wait --for=jsonpath='{.status.ephemeralContainerStatuses[0].state.terminated.reason}'=Completed \
  pod/"$pod" -n "$namespace" --timeout=90s >/dev/null
kubectl logs -n "$namespace" "$pod" -c "$debugger"
