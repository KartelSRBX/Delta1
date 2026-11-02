.DEFAULT_GOAL := help
COMPOSE ?= docker compose
PY ?= python3

help: ## Show targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Build and start the whole stack
	$(COMPOSE) up -d --build
	@echo "Grafana:    http://localhost:3000  (dashboards: Delta1 folder)"
	@echo "Prometheus: http://localhost:9090"
	@echo "ClickHouse: http://localhost:8123/play  (user delta1 / delta1)"

down: ## Stop the stack, keep data
	$(COMPOSE) down

clean: ## Stop the stack and delete all data
	$(COMPOSE) down -v

ps: ## Service status
	$(COMPOSE) ps

logs: ## Follow generator and processor logs
	$(COMPOSE) logs -f generator processor

dev: ## Create a virtualenv with dev dependencies
	$(PY) -m venv .venv && .venv/bin/pip install -e '.[dev]'

test: ## Unit tests
	pytest

lint: ## Ruff lint + format check
	ruff check .
	ruff format --check .

fmt: ## Auto-format
	ruff format .
	ruff check --fix .

dashboards: ## Regenerate Grafana dashboards from grafana/build_dashboards.py
	$(PY) grafana/build_dashboards.py

smoke: ## End-to-end checks against a running stack
	$(PY) scripts/smoke_test.py

ch: ## Open a ClickHouse client
	$(COMPOSE) exec clickhouse clickhouse-client -u delta1 --password delta1 -d delta1

outage: ## Stop ClickHouse for 30s to watch lag build up and drain
	$(COMPOSE) stop clickhouse && sleep 30 && $(COMPOSE) start clickhouse

.PHONY: help up down clean ps logs dev test lint fmt dashboards smoke ch outage
