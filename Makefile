KAFKA_BROKERS ?= localhost:19092,localhost:29092,localhost:39092
export KAFKA_BROKERS
TOPICS ?=
COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: up stop down logs topics describe build vet lint test test-race verify tidy

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

topics:
	go run ./cmd/labctl topics

describe:
	go run ./cmd/labctl describe $(TOPICS)

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

verify: build vet lint test-race

tidy:
	go mod tidy
