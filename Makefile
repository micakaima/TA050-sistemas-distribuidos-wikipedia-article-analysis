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

fmt:
	@if command -v goimports >/dev/null 2>&1; then \
		echo "Formateando proyecto con goimports..."; \
		goimports -w .; \
	elif [ -x "$$HOME/go/bin/goimports" ]; then \
		echo "Formateando proyecto con goimports..."; \
		"$$HOME/go/bin/goimports" -w .; \
	elif command -v gofmt >/dev/null 2>&1; then \
		echo "Formateando proyecto con gofmt..."; \
		gofmt -s -w .; \
	else \
		echo "Error: no se encontró ni goimports ni gofmt." >&2; \
		exit 1; \
	fi
.PHONY: fmt
