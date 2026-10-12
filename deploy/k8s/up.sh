#!/usr/bin/env bash
# Brings up the Kubernetes stand: a kind cluster, the platform charts at pinned versions,
# then Kafka, Postgres and the schema registry through their operators. Every step is an
# upgrade-or-install or an apply, so a second run converges instead of failing.
set -euo pipefail

cd "$(dirname "$0")"
readonly cluster=kafka-lab
readonly repo_root=../..

# Pinned: a chart that moves under the stand is an experiment that cannot be rerun.
readonly traefik_chart=41.6.0
readonly metrics_server_chart=3.14.0
readonly kube_prometheus_chart=91.7.0
readonly loki_chart=7.3.0
readonly alloy_chart=1.13.0
readonly strimzi_chart=1.2.0
readonly cnpg_chart=0.29.1

# Both stands at once do not fit in Docker's memory, and the compose brokers would be the
# ones the OOM killer picks while an experiment is running on them.
if docker ps --format '{{.Names}}' | grep -q '^kafka-lab-kafka'; then
  echo "the compose stand is running: make stop first, the two stands do not fit together" >&2
  exit 1
fi

if ! kind get clusters | grep -qx "$cluster"; then
  kind create cluster --config kind.yaml
fi
kubectl config use-context "kind-$cluster" >/dev/null

helm_install() {
  local release=$1 chart=$2 version=$3 namespace=$4 values=$5
  helm upgrade --install "$release" "$chart" --version "$version" \
    --namespace "$namespace" --create-namespace \
    --values "platform/values/$values" --wait --timeout 10m
}

helm repo add traefik https://traefik.github.io/charts --force-update >/dev/null
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ --force-update >/dev/null
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts --force-update >/dev/null
helm repo add grafana https://grafana.github.io/helm-charts --force-update >/dev/null
helm repo add strimzi https://strimzi.io/charts/ --force-update >/dev/null
helm repo add cnpg https://cloudnative-pg.github.io/charts --force-update >/dev/null
helm repo update >/dev/null

kubectl apply -f platform/namespace.yaml

helm_install traefik traefik/traefik "$traefik_chart" traefik traefik.yaml
helm_install metrics-server metrics-server/metrics-server "$metrics_server_chart" kube-system metrics-server.yaml
# First among the rest: it brings the PodMonitor/ServiceMonitor CRDs that monitors.yaml
# and the payments chart create objects of.
helm_install monitoring prometheus-community/kube-prometheus-stack "$kube_prometheus_chart" monitoring kube-prometheus-stack.yaml
helm_install loki grafana/loki "$loki_chart" monitoring loki.yaml
helm_install alloy grafana/alloy "$alloy_chart" monitoring alloy.yaml
helm_install strimzi strimzi/strimzi-kafka-operator "$strimzi_chart" strimzi strimzi.yaml
helm_install cnpg cnpg/cloudnative-pg "$cnpg_chart" cnpg-system cnpg.yaml

# The dashboard is the compose stand's file, not a copy: Grafana's sidecar loads every
# ConfigMap labelled grafana_dashboard.
kubectl create configmap payments-dashboard --namespace monitoring \
  --from-file=payments.json="$repo_root/deploy/grafana/dashboards/payments.json" \
  --dry-run=client -o yaml |
  kubectl label --local -f - grafana_dashboard=1 -o yaml |
  kubectl apply -f -

kubectl apply -f platform/postgres.yaml -f platform/kafka.yaml -f platform/monitors.yaml
kubectl wait --namespace lab --for=condition=Ready cluster.postgresql.cnpg.io/payments-db --timeout=10m
kubectl apply -f platform/apicurio.yaml
kubectl wait --namespace lab --for=condition=Ready kafka/kafka-lab --timeout=15m
kubectl apply -f platform/topics.yaml
kubectl wait --namespace lab --for=condition=Ready kafkatopic --all --timeout=5m
kubectl wait --namespace lab --for=condition=Available deployment/apicurio --timeout=10m

echo "stand is up: grafana at http://grafana.localtest.me:8800"
