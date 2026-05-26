SHELL := /bin/bash

GO            ?= go
DOCKER_COMPOSE ?= docker compose -f deployments/docker-compose.yml

BIN_DIR := bin
TRENDSD_BIN := $(BIN_DIR)/trendsd
LOADGEN_BIN := $(BIN_DIR)/loadgen

RPS ?= 5000
D   ?= 60s

.PHONY: help tidy build run test test-race bench cover lint vet up down logs load clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

tidy: ## go mod tidy
	$(GO) mod tidy

build: $(TRENDSD_BIN) $(LOADGEN_BIN) ## build all binaries

$(TRENDSD_BIN):
	mkdir -p $(BIN_DIR)
	$(GO) build -trimpath -o $(TRENDSD_BIN) ./cmd/trendsd

$(LOADGEN_BIN):
	mkdir -p $(BIN_DIR)
	$(GO) build -trimpath -o $(LOADGEN_BIN) ./cmd/loadgen

run: $(TRENDSD_BIN) ## run service against local Kafka
	$(TRENDSD_BIN)

test: ## go test
	$(GO) test ./...

test-race: ## go test with -race
	$(GO) test -race ./...

bench: ## run ranker benchmarks
	$(GO) test -run=^$$ -bench=. -benchmem ./internal/ranker/...

cover: ## generate coverage report
	$(GO) test -coverprofile=coverage.txt ./...
	$(GO) tool cover -func=coverage.txt | tail -1

vet: ## go vet
	$(GO) vet ./...

up: ## docker compose up
	$(DOCKER_COMPOSE) up -d --build kafka trendsd

down: ## docker compose down
	$(DOCKER_COMPOSE) down -v

logs: ## tail compose logs
	$(DOCKER_COMPOSE) logs -f trendsd

load: ## fire loadgen against running stack
	$(DOCKER_COMPOSE) --profile load run --rm loadgen \
		-brokers=kafka:9092 -topic=search.events \
		-rps=$(RPS) -d=$(D) -users=50000 -queries=2000 -workers=8

load-local: $(LOADGEN_BIN) ## run loadgen from host
	$(LOADGEN_BIN) -brokers=localhost:9094 -rps=$(RPS) -d=$(D)

clean: ## remove build artifacts
	rm -rf $(BIN_DIR) coverage.txt
