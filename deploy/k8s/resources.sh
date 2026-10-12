#!/usr/bin/env bash
# Writes what every container on the stand requested, was allowed and actually used over a
# window (default: the last 3 h), from the cAdvisor and kube-state-metrics series in
# Prometheus, to a new deploy/k8s/drills/results/resources-<timestamp>.log. The "Figures:"
# comments next to requests and limits in deploy/ cite such a log by name; run it right after
# a drill session (make k8s-evidence does) so the window holds the drills' peaks.
# WINDOW=90m ./resources.sh narrows it.
set -euo pipefail
cd "$(dirname "$0")"

readonly window=${WINDOW:-3h}
readonly log="drills/results/resources-$(date +%Y%m%d-%H%M%S).log"
readonly port=19090
mkdir -p drills/results

kubectl port-forward -n monitoring svc/monitoring-kube-prometheus-prometheus "$port:9090" >/dev/null 2>&1 &
forward=$!
trap 'kill "$forward" 2>/dev/null || true' EXIT
until curl -sf "localhost:$port/-/ready" >/dev/null; do sleep 1; done

# One instant query, printed as "pod/container value" lines.
query() {
  curl -sfG "localhost:$port/api/v1/query" --data-urlencode "query=$1" |
    jq -r '.data.result[] | "\(.metric.namespace // "-")/\(.metric.pod // "-")/\(.metric.container // "-") \(.value[1])"'
}

# Joins the per-container series into one table, memory in Mi and CPU in millicores.
table() {
  python3 - "$@" <<'PY'
import sys, collections
names = ["req_cpu", "req_mem", "lim_mem", "avg_mem", "max_mem", "p95_cpu", "max_cpu", "oom", "restarts"]
rows = collections.defaultdict(dict)
for name, path in zip(names, sys.argv[1:]):
    for line in open(path):
        key, value = line.split()
        rows[key][name] = float(value)
def mem(v): return "-" if v is None else f"{v/1048576:.0f}Mi"
def cpu(v): return "-" if v is None else f"{v*1000:.0f}m"
print(f"{'namespace/pod/container':72} {'req cpu':>7} {'req mem':>8} {'lim mem':>8} {'avg mem':>8} {'max mem':>8} {'p95 cpu':>8} {'max cpu':>8} {'oom':>4} {'restarts':>8}")
for key in sorted(rows):
    r = rows[key]
    if "avg_mem" not in r: continue
    print(f"{key:72} {cpu(r.get('req_cpu')):>7} {mem(r.get('req_mem')):>8} {mem(r.get('lim_mem')):>8} "
          f"{mem(r.get('avg_mem')):>8} {mem(r.get('max_mem')):>8} {cpu(r.get('p95_cpu')):>8} {cpu(r.get('max_cpu')):>8} "
          f"{int(r.get('oom', 0)):>4} {int(r.get('restarts', 0)):>8}")
PY
}

scratch=$(mktemp -d)
trap 'kill "$forward" 2>/dev/null || true; rm -rf "$scratch"' EXIT
cpu_rate='rate(container_cpu_usage_seconds_total{container!="",container!="POD"}[1m])'
query 'max by (namespace,pod,container) (kube_pod_container_resource_requests{resource="cpu"})' > "$scratch/1"
query 'max by (namespace,pod,container) (kube_pod_container_resource_requests{resource="memory"})' > "$scratch/2"
query 'max by (namespace,pod,container) (kube_pod_container_resource_limits{resource="memory"})' > "$scratch/3"
query "max by (namespace,pod,container) (avg_over_time(container_memory_working_set_bytes{container!=\"\",container!=\"POD\"}[$window]))" > "$scratch/4"
query "max by (namespace,pod,container) (max_over_time(container_memory_working_set_bytes{container!=\"\",container!=\"POD\"}[$window]))" > "$scratch/5"
query "max by (namespace,pod,container) (quantile_over_time(0.95, ${cpu_rate}[$window:30s]))" > "$scratch/6"
query "max by (namespace,pod,container) (max_over_time(${cpu_rate}[$window:30s]))" > "$scratch/7"
query "max by (namespace,pod,container) (max_over_time(kube_pod_container_status_last_terminated_reason{reason=\"OOMKilled\"}[$window]))" > "$scratch/8"
query "max by (namespace,pod,container) (increase(kube_pod_container_status_restarts_total[$window]))" > "$scratch/9"

{
  echo "resources over the last $window, $(date -u +%FT%TZ)"
  echo "docker VM: $(docker info --format '{{.MemTotal}}' | awk '{printf "%.0fMi", $1/1048576}'), $(docker info --format '{{.NCPU}}') CPU"
  echo "oom = an OOMKilled termination was seen in the window; restarts = restarts in the window"
  echo
  table "$scratch"/1 "$scratch"/2 "$scratch"/3 "$scratch"/4 "$scratch"/5 "$scratch"/6 "$scratch"/7 "$scratch"/8 "$scratch"/9
  echo
  echo "capacity (whole cluster):"
  printf '  memory requests   %s\n' "$(query 'sum(kube_pod_container_resource_requests{resource="memory"})' | awk '{printf "%.0fMi", $2/1048576}')"
  printf '  memory limits     %s\n' "$(query 'sum(kube_pod_container_resource_limits{resource="memory"})' | awk '{printf "%.0fMi", $2/1048576}')"
  printf '  cpu requests      %s\n' "$(query 'sum(kube_pod_container_resource_requests{resource="cpu"})' | awk '{printf "%.0fm", $2*1000}')"
  printf '  working set now   %s\n' "$(query 'sum(container_memory_working_set_bytes{container!="",container!="POD"})' | awk '{printf "%.0fMi", $2/1048576}')"
  peak_query="max_over_time(sum(container_memory_working_set_bytes{container!=\"\",container!=\"POD\"})[${window}:30s])"
  printf '  working set peak  %s\n' "$(query "$peak_query" | awk '{printf "%.0fMi", $2/1048576}')"
  echo "  node usage now (kubectl top, includes what cAdvisor does not attribute to containers):"
  kubectl top nodes --no-headers | awk '{print "    " $1 " " $4}'
} | tee "$log"
