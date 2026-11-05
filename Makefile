SHELL := /bin/bash
PWD := $(shell pwd)

up:
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

setup-hooks:
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit
.PHONY: setup-hooks

install-tools:
	go install golang.org/x/tools/cmd/goimports@latest
.PHONY: install-tools

MODULE := TA050-sistemas-distribuidos-wikipedia-article-analysis

fmt:
	@if command -v goimports >/dev/null 2>&1; then \
		echo "Formateando proyecto con goimports..."; \
		goimports -local $(MODULE) -w .; \
	elif [ -x "$$HOME/go/bin/goimports" ]; then \
		echo "Formateando proyecto con goimports..."; \
		"$$HOME/go/bin/goimports" -local $(MODULE) -w .; \
	elif command -v gofmt >/dev/null 2>&1; then \
		echo "Formateando proyecto con gofmt..."; \
		gofmt -s -w .; \
	else \
		echo "Error: no se encontró ni goimports ni gofmt." >&2; \
		exit 1; \
	fi
.PHONY: fmt

ROWS ?= 50
CLIENTS ?= 3
VOLUME ?=
ORACLE ?= auto
INPUT_MODE ?= volume
COMPOSE_FILE ?= docker-compose.yaml
TIMEOUT ?= 1200
EXTRA_ARGS ?=

benchmark:
	@if [ -n "$(VOLUME)" ]; then \
		python3 scripts/benchmark_runner.py --volume $(VOLUME) --clients $(CLIENTS) --oracle $(ORACLE) --input-mode $(INPUT_MODE) --compose-file $(COMPOSE_FILE) --timeout $(TIMEOUT) $(EXTRA_ARGS); \
	else \
		python3 scripts/benchmark_runner.py --rows $(ROWS) --clients $(CLIENTS) --oracle $(ORACLE) --input-mode $(INPUT_MODE) --compose-file $(COMPOSE_FILE) --timeout $(TIMEOUT) $(EXTRA_ARGS); \
	fi
.PHONY: benchmark

prepare-expected:
	@if [ -n "$(VOLUME)" ]; then \
		python3 scripts/benchmark_runner.py --volume $(VOLUME) --clients $(CLIENTS) --oracle generate --input-mode $(INPUT_MODE) --compose-file $(COMPOSE_FILE) $(EXTRA_ARGS) --skip-docker; \
	else \
		python3 scripts/benchmark_runner.py --rows $(ROWS) --clients $(CLIENTS) --oracle generate --input-mode $(INPUT_MODE) --compose-file $(COMPOSE_FILE) $(EXTRA_ARGS) --skip-docker; \
	fi
.PHONY: prepare-expected



