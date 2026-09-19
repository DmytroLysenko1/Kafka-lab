COMPOSE := docker compose -f deploy/docker-compose.yml

TOPICCTL := go run github.com/segmentio/topicctl/cmd/topicctl@v1.23.1
CLUSTER_CONFIG := deploy/topicctl/cluster.yaml
CATALOG := deploy/topics/*.yaml
TOPIC_YAMLS := $(wildcard deploy/topics/*.yaml experiments/*/topics/*.yaml)

# Override when an experiment killed this broker: make lag GROUP=g KAFKA_CONTAINER=kafka-lab-kafka2
KAFKA_CONTAINER ?= kafka-lab-kafka1
KAFKA_BIN := docker exec $(KAFKA_CONTAINER) /opt/kafka/bin
BOOTSTRAP := --bootstrap-server localhost:9092

TOPICS ?=
TOPIC ?=
GROUP ?=
EXP ?=

.PHONY: up stop down logs topics exp-topics check topic-lint reset-topic elect-preferred describe lag build vet lint test test-race verify tidy

up:
	$(COMPOSE) up -d --wait

stop:
	$(COMPOSE) stop

# Destructive on purpose: -v drops the brokers' volumes, so the next `up` starts from an
# empty log. Use `make stop` to pause an experiment without shredding its state.
down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

# apply also elects the preferred leader on every existing topic it touches, so it must not
# run in the middle of an experiment that measures leadership (exp-04, exp-13).
topics:
	$(TOPICCTL) apply --cluster-config $(CLUSTER_CONFIG) --skip-confirm $(CATALOG)

# Experiment topics live in experiments/<exp>/topics/, outside the catalog that
# `make topics` creates for good and `make check` keeps checking.
exp-topics: topic-lint
	$(if $(EXP),,$(error EXP is required, e.g. make exp-topics EXP=exp-08-acks))
	@echo "note: apply elects the preferred leader on the topics it touches
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
	$(eval TOPIC_YAML := $(firstword $(wildcard deploy/topics/$(TOPIC).yaml experiments/*/topics/$(TOPIC).yaml)))
	$(if $(TOPIC_YAML),,$(error no YAML declares $(TOPIC), refusing to delete a topic that cannot be recreated))
	$(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --delete --topic '$(TOPIC)'
	@until ! $(KAFKA_BIN)/kafka-topics.sh $(BOOTSTRAP) --list | grep -qx '$(TOPIC)'; do sleep 1; done
	$(TOPICCTL) apply --cluster-config $(CLUSTER_CONFIG) --skip-confirm $(TOPIC_YAML)

# The explicit replacement for the auto leader rebalance that the compose file switches off.
elect-preferred:
	$(KAFKA_BIN)/kafka-leader-election.sh $(BOOTSTRAP) --election-type PREFERRED --all-topic-partitions

describe:
	$(TOPICCTL) get partitions --cluster-config $(CLUSTER_CONFIG) $(TOPICS)

# Not topicctl: its lag reads one topic at a time through classic DescribeGroups, which reports
# a KIP-848 group as Dead. This lists every topic of the group, on either protocol.
lag:
	$(if $(GROUP),,$(error GROUP is required, e.g. make lag GROUP=payments-consumer))
	$(KAFKA_BIN)/kafka-consumer-groups.sh $(BOOTSTRAP) --describe --group '$(GROUP)'

build:
	go build ./...

vet:
	go vet ./...

lint:
	golangci-lint run

test:
	go test ./...

test-race:
	go test -race ./...

# The module has no packages until the service stage; go vet and golangci-lint fail on an
# empty module, so verify says so instead of reporting a red build for code that is absent.
verify:
	@packages="$$(go list ./... 2>/dev/null)"; status=$$?; \
	if [ $$status -ne 0 ]; then go list ./...; exit $$status; \
	elif [ -z "$$packages" ]; then echo "no Go packages yet: nothing to build, vet, lint or test"; \
	else $(MAKE) build vet lint test-race; fi

tidy:
	go mod tidy
