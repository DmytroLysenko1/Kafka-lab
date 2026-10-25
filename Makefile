COMPOSE := docker compose -f deploy/docker-compose.yml

TOPICCTL := go run github.com/segmentio/topicctl/cmd/topicctl@v1.23.1
CLUSTER_CONFIG := deploy/topicctl/cluster.yaml
CATALOG := deploy/topics/*.yaml
TOPIC_YAMLS := $(wildcard deploy/topics/*.yaml experiments/*/*/topics/*.yaml)

# Override when an experiment killed this broker: make lag GROUP=g KAFKA_CONTAINER=kafka-lab-kafka2
KAFKA_CONTAINER ?= kafka-lab-kafka1
KAFKA_BIN := docker exec $(KAFKA_CONTAINER) /opt/kafka/bin
BOOTSTRAP := --bootstrap-server localhost:9092

DATABASE_URL ?= postgres://lab:lab@localhost:5432/lab?sslmode=disable
KAFKA_BROKERS ?= localhost:19092,localhost:29092,localhost:39092
SCHEMA_REGISTRY_URL ?= http://localhost:8080/apis/ccompat/v7
# Tempo on the stand (make up); set it empty to run a service without exporting spans.
OTEL_EXPORTER_OTLP_ENDPOINT ?= http://localhost:4318
export DATABASE_URL KAFKA_BROKERS SCHEMA_REGISTRY_URL OTEL_EXPORTER_OTLP_ENDPOINT

TOPICS ?=
TOPIC ?=
GROUP ?=
EXP ?=

SERVICES := payments-api outbox-relay payments-consumer migrate dlq-replayer prune
# The commit the images are built from, plus a hash of the uncommitted changes when there
# are any (deploy/docker/image-tag.sh): a tag that is rebuilt in place changes no pod
# template, so nothing rolls (deploy/helm/payments).
IMAGE_TAG ?= $(shell deploy/docker/image-tag.sh)
export IMAGE_TAG
DOCKER_BUILD := DOCKER_BUILDKIT=1 docker build -f deploy/docker/Dockerfile

.PHONY: up stop down logs proto topics exp-topics check exp-% exp-04c topic-lint reset-topic elect-preferred describe lag migrate prune build vet lint test test-race test-integration verify verify-askdocs ask ask-sources askdocs-binary ask-eval ask-log tidy images image-measure app-up app-down k8s-up k8s-down k8s-images k8s-secret k8s-verify k8s-deploy k8s-replay k8s-drill-% k8s-evidence k8s-resources

up:
	$(COMPOSE) up -d --wait

stop:
	$(COMPOSE) stop

# Destructive on purpose: -v drops the brokers' volumes, so the next `up` starts from an
# empty log. Use `make stop` to pause an experiment without shredding its state.
down:
	$(COMPOSE) down -v

# The services as containers next to the stand (the compose `app` profile). Needs `make up`
# and `make topics` first, and the host-run services stopped: both bind the same ports.
app-up:
	$(if $(PAYMENTS_API_KEY),,$(error PAYMENTS_API_KEY is required: the API refuses to start without it))
	$(COMPOSE) --profile app up -d --build --wait payments-api outbox-relay payments-consumer

app-down:
	$(COMPOSE) --profile app rm -sf migrate payments-api outbox-relay payments-consumer

logs:
	$(COMPOSE) logs -f

# apply also elects the preferred leader on every existing topic it touches, so it must not
# run in the middle of an experiment that measures leadership (exp-04, exp-13).
topics:
	$(TOPICCTL) apply --cluster-config $(CLUSTER_CONFIG) --skip-confirm $(CATALOG)

# Experiment topics live in experiments/<group>/<exp>/topics/, outside the catalog that
# `make topics` creates for good and `make check` keeps checking. EXP is the path under
# experiments/, group included, because the same experiment number can only be found once.
exp-topics: topic-lint
	$(if $(EXP),,$(error EXP is required, e.g. make exp-topics EXP=transaction_guarantee/exp-08-acks))
	@echo "note: apply elects the preferred leader on the topics it touches"
	$(TOPICCTL) apply --cluster-config $(CLUSTER_CONFIG) --skip-confirm experiments/$(EXP)/topics/*.yaml

# Non-zero on drift from the YAML in either direction, and on a cluster too unhealthy to
# measure on: replicas out of sync, leftover throttles, leaders off their preferred replica.
check: topic-lint
	$(TOPICCTL) check --cluster-config $(CLUSTER_CONFIG) --check-leaders $(CATALOG)

# With eligible leader replicas (KIP-966, on by default in Kafka 4.x) the controller keeps a
# cluster-wide dynamic min.insync.replicas, and topicctl reads it as the topic's own
# setting: a YAML that leaves min.insync.replicas out would report drift forever.
topic-lint:
	@missing="$$(grep -L 'min.insync.replicas:' $(TOPIC_YAMLS))"; \
	if [ -n "$$missing" ]; then echo "min.insync.replicas must be declared explicitly in:"; echo "$$missing"; exit 1; fi

# apply only warns about a setting the YAML never declared, so a reshaped topic is dropped
# and recreated from its own YAML, found before anything is deleted. Only that one YAML is
# applied, so no other topic gets its leaders moved.
reset-topic:
	$(if $(TOPIC),,$(error TOPIC is required, e.g. make reset-topic TOPIC=payments-consumer.dlq))
	$(eval TOPIC_YAML := $(firstword $(wildcard deploy/topics/$(TOPIC).yaml experiments/*/*/topics/$(TOPIC).yaml)))
	$(if $(TOPIC_YAML),,$(error no YAML declares $(TOPIC), refusing to delete a topic that cannot be recreated))
	$(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --delete --topic '$(TOPIC)'
	@until ! $(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --list | grep -qx '$(TOPIC)'; do sleep 1; done
	$(TOPICCTL) apply --cluster-config $(CLUSTER_CONFIG) --skip-confirm $(TOPIC_YAML)

# The explicit replacement for the auto leader rebalance that the compose file switches off.
elect-preferred:
	$(KAFKA_BIN)/kafka-leader-election.sh $(BOOTSTRAP) --election-type PREFERRED --all-topic-partitions

# exp-04c is not a plain experiment directory: it recreates the brokers with a raised
# broker.session.timeout.ms and puts the default back afterwards. An explicit rule wins over
# the pattern rule below, which would look for experiments/*/exp-04c-* and find nothing.
exp-04c: check
	experiments/kafka_internals/exp-04-isr-leader-election/run-session-timeout.sh

# make exp-01 finds experiments/<group>/exp-01-*/run.sh whichever group it was filed under,
# and runs it only after check has agreed that the cluster still matches the catalog — a
# measurement on a drifted stand is a wrong number.
exp-%: check
	$(eval EXP_DIR := $(firstword $(wildcard experiments/*/exp-$*-*)))
	$(if $(EXP_DIR),,$(error no experiment matches experiments/*/exp-$*-*))
	$(EXP_DIR)/run.sh

describe:
	$(TOPICCTL) get partitions --cluster-config $(CLUSTER_CONFIG) $(TOPICS)

# Not topicctl: its lag reads one topic at a time through classic DescribeGroups, which reports
# a KIP-848 group as Dead. This lists every topic of the group, on either protocol.
lag:
	$(if $(GROUP),,$(error GROUP is required, e.g. make lag GROUP=payments-consumer))
	$(KAFKA_BIN)/kafka-consumer-groups.sh $(BOOTSTRAP) --describe --group '$(GROUP)'

# Regenerates gen/ from proto/. The same .proto text is embedded and registered with the
# schema registry at runtime, so the schema consumers resolve is the one this generated.
proto:
	buf lint
	buf generate

# Applied by a command of its own, never by a service on boot: two replicas coming up
# together would race through the same schema change.
migrate:
	go run ./cmd/migrate

# Deletes inbox and outbox rows past their horizons (cmd/prune); on Kubernetes an hourly CronJob.
prune:
	go run ./cmd/prune

build:
	go build ./...

# The integration tag is vetted too, which compiles the suite: it cannot run without the stand,
# but it can stop compiling without one, and once did unnoticed.
vet:
	go vet -tags integration ./...

lint:
	golangci-lint run

test:
	go test ./...

test-race:
	go test -race ./...

# What a fake cannot prove: ON CONFLICT under a real race, the CHECK constraints, that
# FOR UPDATE SKIP LOCKED hands each outbox record to exactly one relay, and that a consumer
# can decode what the publisher wrote by the schema id inside it. Needs `make up`.
# The packages share one Postgres and run in parallel; they stay hermetic by owning
# disjoint tables — the payments side truncates payments and outbox, the consumer side
# inbox and merchant_currency_totals.
test-integration:
	go test -race -count=1 -tags=integration ./internal/...

# go vet and golangci-lint fail on a module with no packages, so verify says so instead of
# reporting a red build for code that is absent.
verify:
	@packages="$$(go list ./... 2>/dev/null)"; status=$$?; \
	if [ $$status -ne 0 ]; then go list ./...; exit $$status; \
	elif [ -z "$$packages" ]; then echo "no Go packages yet: nothing to build, vet, lint or test"; \
	else $(MAKE) build vet lint test-race verify-askdocs; fi

# askdocs is its own module (the Anthropic SDK stays out of the payments service's go.mod), so
# ./... above does not reach it.
verify-askdocs:
	cd askdocs && go build ./... && go vet ./... && golangci-lint run ./... && go test -race ./...

# Answers a question from the lab's documentation with Claude; needs ANTHROPIC_API_KEY.
#   make ask Q="What happens to the outbox while Kafka is down?"
# make reports any failure as 2; the exit codes in askdocs/README.md are the binary's, so a
# script runs askdocs/bin/askdocs itself. The question is read from the environment, so quotes
# inside it reach the binary intact.
ask: askdocs-binary
	@test -n "$$Q" || { echo 'usage: make ask Q="your question"'; exit 64; }
	cd askdocs && ./bin/askdocs -root .. "$$Q"

# The sections the same question would be answered from, without calling the model.
ask-sources: askdocs-binary
	@test -n "$$Q" || { echo 'usage: make ask-sources Q="your question"'; exit 64; }
	cd askdocs && ./bin/askdocs -root .. -retrieve-only "$$Q"

askdocs-binary:
	cd askdocs && go build -o bin/askdocs ./cmd/askdocs

# Every question in askdocs/eval/questions.tsv through retrieval alone, with whether it found
# the document labelled for it; no key needed. The log is askdocs/results/retrieval.log.
ask-eval: askdocs-binary
	askdocs/eval/run.sh retrieve > askdocs/results/retrieval.log
	@tail -1 askdocs/results/retrieval.log

# The same questions answered by the model, logged with tokens, stop reason and time. Run it
# in your own terminal with ANTHROPIC_API_KEY set: only the SDK reads the key, and it never
# reaches the log, so the log can be committed.
ask-log: askdocs-binary
	@test -n "$$ANTHROPIC_API_KEY" || { echo 'ask-log needs ANTHROPIC_API_KEY in the environment'; exit 78; }
	askdocs/eval/run.sh answer > askdocs/results/answers-$$(date -u +%Y%m%d-%H%M%S).log

tidy:
	go mod tidy

# Sizes and rebuild times of docs/k8s/01-docker.md, with the log kept in deploy/docker/results/.
image-measure:
	deploy/docker/measure.sh

images:
	@for service in $(SERVICES); do \
		$(DOCKER_BUILD) --build-arg SERVICE=$$service -t kafka-lab/$$service:$(IMAGE_TAG) . || exit 1; \
	done

# The Kubernetes stand (docs/k8s/): kind, the platform charts, Kafka through Strimzi,
# Postgres through CloudNativePG. Refuses to start next to the compose stand.
k8s-up:
	deploy/k8s/up.sh

k8s-down:
	kind delete cluster --name kafka-lab

CHART := deploy/helm/payments
K8S_NAMESPACE := lab
# The tag running in the cluster, read from the release. IMAGE_TAG is computed from the working
# tree, which drifts from what was built and loaded as soon as a file changes; a one-off job
# run beside the release must use the image the release runs. Recursive (=), so helm is
# asked only by a target that uses it.
DEPLOYED_TAG = $(shell helm get values payments -n $(K8S_NAMESPACE) -o json 2>/dev/null | jq -r .image.tag)
CLASS ?= exhausted
# Schemas for the CRDs the chart uses (ServiceMonitor), from the community catalog at a fixed
# commit — its main branch would change what `make k8s-verify` accepts without a change here.
KUBECONFORM := kubeconform -strict -summary -schema-location default \
	-schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/ad3b08c5045129d7bb1eeffd8e61719b2c8dd1e2/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'

# Nothing is pulled from a registry: the images are copied into the kind nodes.
k8s-images: images
	@for service in $(SERVICES); do \
		kind load docker-image kafka-lab/$$service:$(IMAGE_TAG) --name kafka-lab || exit 1; \
	done

# Generated once and never printed; an existing key is left alone, so a rerun does not
# lock out the callers that hold it.
k8s-secret:
	@kubectl get secret payments-api-key -n $(K8S_NAMESPACE) >/dev/null 2>&1 || \
		kubectl create secret generic payments-api-key -n $(K8S_NAMESPACE) \
			--from-literal=PAYMENTS_API_KEY="$$(openssl rand -hex 24)"   # 192 random bits

# The chart and the platform's own manifests (Strimzi, CloudNativePG, prometheus-operator
# objects included, through the pinned CRD catalog).
k8s-verify:
	helm lint $(CHART) --set image.tag=verify
	helm template payments $(CHART) --namespace $(K8S_NAMESPACE) --set image.tag=verify | $(KUBECONFORM)
	$(KUBECONFORM) deploy/k8s/platform/*.yaml

# The chart's version and appVersion never change, so helm history tells revisions apart only
# by the description: every upgrade names the image tag it rolled out, which is what the
# runbook picks a rollback target by.
k8s-deploy: k8s-verify k8s-secret
	helm upgrade --install payments $(CHART) --namespace $(K8S_NAMESPACE) \
		--set image.tag=$(IMAGE_TAG) --description "image $(IMAGE_TAG)" --wait --timeout 5m

# The restricted profile and a memory limit, as for the chart's pods; kubectl run has no flags
# for either, so they go in as an override of the generated pod. runAsUser is numeric on
# purpose: the image says USER nonroot, and the kubelet cannot prove a user name is not
# root, so runAsNonRoot alone refuses to start the container.
REPLAYER_OVERRIDES = {"spec":{"automountServiceAccountToken":false,"securityContext":{"runAsNonRoot":true,"runAsUser":65532,"runAsGroup":65532,"seccompProfile":{"type":"RuntimeDefault"}},"containers":[{"name":"replayer","image":"kafka-lab/dlq-replayer:$(DEPLOYED_TAG)","args":["-class=$(CLASS)"],"env":[{"name":"KAFKA_BROKERS","value":"kafka-lab-kafka-bootstrap:9092"}],"securityContext":{"allowPrivilegeEscalation":false,"readOnlyRootFilesystem":true,"capabilities":{"drop":["ALL"]}},"resources":{"requests":{"cpu":"10m","memory":"32Mi"},"limits":{"memory":"64Mi"}}}]}}

# The replayer is a command run by hand, not a workload: one pod, attached, removed after.
k8s-replay:
	@test -n "$(DEPLOYED_TAG)" -a "$(DEPLOYED_TAG)" != null || { echo "no payments release in $(K8S_NAMESPACE) to take the image tag from" >&2; exit 1; }
	kubectl run dlq-replayer-$$(date +%s) -n $(K8S_NAMESPACE) --rm -i --restart=Never \
		--image=kafka-lab/dlq-replayer:$(DEPLOYED_TAG) --image-pull-policy=IfNotPresent \
		--overrides='$(REPLAYER_OVERRIDES)' -- -class=$(CLASS)

# make k8s-drill-01 runs deploy/k8s/drills/01-*.sh; each writes its evidence under results/.
k8s-drill-%:
	$(eval DRILL := $(firstword $(wildcard deploy/k8s/drills/$*-*.sh)))
	$(if $(DRILL),,$(error no drill matches deploy/k8s/drills/$*-*.sh))
	cd deploy/k8s/drills && ./$(notdir $(DRILL))

# Every log the docs cite, in one run (its length is in the resource log's header): see
# deploy/k8s/drills/session.sh.
k8s-evidence:
	deploy/k8s/drills/session.sh

# What every container requested and used over WINDOW (default 3h), into drills/results/.
k8s-resources:
	deploy/k8s/resources.sh
