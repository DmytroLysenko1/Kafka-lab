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

# The heavy loads (drills 03 and 06) are sent from a pod in the cluster straight to the API's
# Service, not from the host through 127.0.0.1:8800: Docker Desktop's port forward shut down
# 11 s into 800 req/s on 25.10, taking the VM with it (session.sh), and a load that measures
# the forward measures nothing about the API. These loads skip the Ingress; the light
# ones (drills 02 and 07) still go through it. Port 8081 is payments-api's http.port in values.yaml.
readonly api_in_cluster=http://payments-api.$namespace.svc.cluster.local:8081
readonly load_image=kafka-lab/vegeta:12.13.0

# load_start starts one attack in a pod of its own and returns once the attack has begun:
# the pod waits for the targets (stdin, vegeta's format given as $4), then attacks at $2 req/s
# for $3 s and prints vegeta's report of type $5. The key reaches the targets from the host;
# the pod spec holds none. CPU is requested so the scheduler counts the load generator, whose
# own CPU the HPA does not see. The results go to an emptyDir on disk, not into memory; the
# memory limit is a ceiling for vegeta itself, not a measured figure.
load_start() {
  local name=$1 load_rate=$2 load_seconds=$3 format=$4 report=$5
  kubectl delete pod "$name" -n "$namespace" --ignore-not-found --wait >/dev/null
  kubectl apply -n "$namespace" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
  labels: { app: load-generator }
spec:
  restartPolicy: Never
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
        - until [ -f /work/ready ]; do sleep 0.2; done;
          vegeta attack -format=$format -targets=/work/targets -rate=$load_rate -duration=${load_seconds}s > /work/attack.bin &&
          vegeta report -type=$report < /work/attack.bin
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: { drop: [ALL] }
      resources:
        requests: { cpu: 500m, memory: 128Mi }
        limits: { memory: 512Mi }
      volumeMounts: [{ name: work, mountPath: /work }]
  volumes: [{ name: work, emptyDir: {} }]
EOF
  kubectl wait pod "$name" -n "$namespace" --for=condition=Ready --timeout=2m >/dev/null
  kubectl exec -i "$name" -n "$namespace" -- sh -c 'cat > /work/targets && touch /work/ready'
}

# load_report waits for the attack to end and prints its report. A status the API server did
# not answer is asked again, so a slow control plane under load cannot end the drill.
load_report() {
  local name=$1 phase=""
  until [ "$phase" = Succeeded ] || [ "$phase" = Failed ]; do
    sleep 5
    phase=$(kubectl get pod "$name" -n "$namespace" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  done
  kubectl logs "$name" -n "$namespace"
  kubectl delete pod "$name" -n "$namespace" --wait=false >/dev/null
  [ "$phase" = Succeeded ]
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
