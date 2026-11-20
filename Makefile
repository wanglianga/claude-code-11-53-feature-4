SHELL := /bin/bash
GO ?= go
BASE ?= http://127.0.0.1:8080

.PHONY: help run build test vet fmt samples replay sequence-replay verify clean

help: ## show targets
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-10s %s\n", $$1, $$2}'

build: ## compile the server binary into ./bin/agrisync
	$(GO) build -trimpath -o bin/agrisync ./cmd/server

run: ## run the server locally (needs PostgreSQL)
	$(GO) run ./cmd/server

vet: ## go vet
	$(GO) vet ./...

fmt: ## gofmt check
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "unformatted: $$out"; exit 1; fi

test: ## unit + integration tests
	$(GO) test ./... -count=1

samples: ## regenerate sample payloads
	python3 scripts/gen_samples.py
	python3 scripts/gen_sequence_samples.py

replay: ## run the acceptance replay against BASE
	./scripts/replay.sh $(BASE)

sequence-replay: ## run the sequence-integrity acceptance replay against BASE
	./scripts/sequence_replay.sh $(BASE)

verify: ## full verification (build, vet, tests, both replays against a throwaway server)
	./scripts/verify.sh

clean:
	rm -rf bin
