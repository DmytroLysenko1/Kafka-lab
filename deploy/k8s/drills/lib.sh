# Shared by the drills: where the API is, how to reach it, and how to put load on it.
# Sourced, never run.

readonly namespace=lab
# The address, not the name: payments.localtest.me also resolves to ::1, where nothing is
# published, and under load vegeta dials it now and then (drill 03's first 1200 req/s run
# lost 0.66% of its requests to "connect: connection refused" on [::1]:8800). The Host
# header carries the name the Ingress rule matches.
readonly api=http://127.0.0.1:8800
readonly api_host=payments.localtest.me
readonly results_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/results"

api_key() {
  kubectl get secret payments-api-key -n "$namespace" -o jsonpath='{.data.PAYMENTS_API_KEY}' | base64 -d
}

# vegeta targets in its JSON format, one POST /payments per line, each with an idempotency
# key of its own: a load of replays would measure the idempotency lookup, not the write path.
payment_targets() {
  local count=$1 run_id=$2 key
  key=$(api_key)
  local body
  body=$(printf '{"merchant_id":"m-drill-%s","amount_minor":100,"currency":"EUR"}' "$run_id" | base64)
  for i in $(seq 1 "$count"); do
    printf '{"method":"POST","url":"%s/payments","header":{"Host":["%s"],"X-API-Key":["%s"],"Idempotency-Key":["drill-%s-%d"],"Content-Type":["application/json"]},"body":"%s"}\n' \
      "$api" "$api_host" "$key" "$run_id" "$i" "$body"
  done
}

stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# The heavy loads (drills 03 and 06) are sent from a pod in the cluster, not from the host
# through 127.0.0.1:8800: on 25.10 Docker Desktop's port forward shut down 11 s into 800 req/s
# from the host and took the VM with it (Docker's own logs, not committed; runbook.md, Scale).
# The pod sends to Traefik's Service with the Ingress's Host, so the load takes the same
# Ingress path as before and Traefik spreads it per request. Sent to the API's Service
# instead, vegeta's keep-alive connections were each pinned by kube-proxy to one pod when
# opened: one API pod ran at 191–237% of its request while others idled
# (results/api-cpu-per-pod-20261025-220313.log), and the HPA's average measured that skew.
# The light loads (drills 01, 02, 05 and 07, 20 req/s each) still go through the forward.
readonly api_in_cluster=http://traefik.traefik.svc.cluster.local
readonly load_image=kafka-lab/vegeta:12.13.0   # VEGETA_VERSION in the Makefile

# load_name is unique to one run of one drill, so two runs cannot delete each other's pod.
load_name() { echo "load-$1-$$"; }

# load_start starts one attack in a pod of its own and returns once the attack has begun:
# the pod waits for the targets (stdin, vegeta's format given as $4), then attacks at $2 req/s
# for $3 s and prints vegeta's report of type $5. The key reaches the targets from the host;
# the pod spec holds none. The pod ends by itself: it waits at most 5 min for its targets and
# lives at most the attack plus 10 min (activeDeadlineSeconds), so a drill interrupted before
# its trap ran cannot leave it running. CPU from resources-20261025-224859.log: p95 289m at
# 800 req/s (load-03); 308m through Traefik in resources-20261026-044517.log. Memory is not that log's 109Mi: that load went straight to the Service
# and was answered fast. Through Traefik on 26.10 the API saturated, vegeta — which keeps
# its rate by adding workers while answers are slow — reached 220Mi and the kernel killed it
# at its 256Mi limit 64 s into drill 03 (the node's dmesg, seen in the session). Workers are
# capped so a saturated API slows the attack, which the report shows, instead of growing it
# without bound. 768Mi is a margin, not a measurement of the cap: the logged peaks are 66Mi
# (load-03) and 125Mi (load-06) in resources-20261026-044517.log. The results go to an emptyDir on disk.
load_start() {
  local name=$1 load_rate=$2 load_seconds=$3 format=$4 report=$5
  require_image "$load_image" "make k8s-load-image"
  kubectl apply -n "$namespace" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
  labels: { app: load-generator }
spec:
  restartPolicy: Never
  # sh as PID 1 passes SIGTERM on to no one, so a deleted pod would keep attacking for the
  # default 30 s grace period.
  terminationGracePeriodSeconds: 1
  activeDeadlineSeconds: $((load_seconds + 600))
  automountServiceAccountToken: false
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: vegeta
      image: $load_image
      imagePullPolicy: Never
      command: [sh, -c]
      args:
        - waited=0; until [ -f /work/ready ]; do sleep 1; waited=\$((waited + 1)); [ \$waited -lt 300 ] || exit 1; done;
          vegeta attack -format=$format -targets=/work/targets -rate=$load_rate -duration=${load_seconds}s -max-workers=1000 > /work/attack.bin &&
          vegeta report -type=$report < /work/attack.bin
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: { drop: [ALL] }
      resources:
        requests: { cpu: 300m, memory: 128Mi }
        limits: { memory: 768Mi }
      volumeMounts: [{ name: work, mountPath: /work }]
  volumes: [{ name: work, emptyDir: {} }]
EOF
  kubectl wait pod "$name" -n "$namespace" --for=condition=Ready --timeout=2m >/dev/null
  kubectl exec -i "$name" -n "$namespace" -- sh -c 'cat > /work/targets && touch /work/ready'
}

# load_report waits for the attack to end and prints its report; a failed attack prints what
# vegeta said and fails. A status the API server did not answer is asked again, so a slow
# control plane under load cannot end the drill — but a pod that is gone, or an attack that
# outlives its deadline, does.
load_report() {
  local name=$1 load_seconds=$2 phase="" started
  started=$(date +%s)
  until [ "$phase" = Succeeded ] || [ "$phase" = Failed ]; do
    sleep 2
    if ! phase=$(kubectl get pod "$name" -n "$namespace" -o jsonpath='{.status.phase}' 2>&1); then
      case $phase in *NotFound*) echo "load pod $name is gone" >&2; return 1 ;; esac
      phase=""
    fi
    if [ $(( $(date +%s) - started )) -gt $((load_seconds + 600)) ]; then
      echo "load pod $name did not finish" >&2
      return 1
    fi
  done
  kubectl logs "$name" -n "$namespace"
  # Waited for: the next step of a drill starts a pod of the same name.
  kubectl delete pod "$name" -n "$namespace" --wait >/dev/null
  [ "$phase" = Succeeded ]
}

# load_stop is the drills' exit trap: the load ends with the drill, however the drill ends.
load_stop() {
  kubectl delete pod "$1" -n "$namespace" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}

# A local image loaded into kind, or the command that loads it: without it the pod waits two
# minutes in ErrImageNeverPull and the drill fails with only "timed out".
require_image() {
  local image=$1 fix=$2
  if ! docker exec kafka-lab-worker crictl images -o json |
    jq -e --arg ref "docker.io/$image" '.images[].repoTags[] | select(. == $ref)' >/dev/null; then
    echo "$image is not loaded into kind: $fix" >&2
    exit 1
  fi
}

deployed_tag() {
  helm get values payments -n "$namespace" -o json | jq -r '.image.tag'
}

# The images are loaded into the kind nodes, never pulled: a tag that is not there is a
# release that can only fail with ErrImageNeverPull/ErrImagePull.
require_loaded() {
  local tag=$1
  if ! docker exec kafka-lab-worker crictl images -o json |
    jq -e --arg ref "docker.io/kafka-lab/payments-api:$tag" '.images[].repoTags[] | select(. == $ref)' >/dev/null; then
    echo "kafka-lab/payments-api:$tag is not loaded into kind: make k8s-images IMAGE_TAG=$tag" >&2
    exit 1
  fi
}
