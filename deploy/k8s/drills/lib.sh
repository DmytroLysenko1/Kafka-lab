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
