.PHONY: install install-e2e demo up down logs test test-unit test-integration test-e2e test-compose lint typecheck docker-test docker-lint docker-typecheck db-migrate dbt-run dbt-test train evaluate shell-api shell-db

install:
	uv sync --locked --extra dev

install-e2e:
	uv sync --locked --extra dev --extra e2e
	uv run --locked --extra dev --extra e2e playwright install chromium

demo:
	bash scripts/demo.sh

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

test:
	uv run --locked pytest tests/ -m "not compose" -v --cov-branch --cov-fail-under=70

test-unit:
	uv run --locked pytest tests/unit/ -v

test-integration:
	uv run --locked pytest tests/integration/ -m "not compose" -v

test-e2e:
	uv run --locked --extra dev --extra e2e pytest tests/e2e/ -v --no-cov

test-compose:
	RUN_COMPOSE_E2E=1 uv run --locked --extra dev --extra e2e pytest tests/integration/test_compose_e2e_recovery.py -v --no-cov

lint:
	uv run --locked ruff check .
	uv run --locked ruff format --check .

typecheck:
	uv run --locked mypy .

docker-test:
	docker compose exec api pytest tests/unit/ tests/integration/ -m "not compose" -v --cov-branch --cov-fail-under=70

docker-lint:
	docker compose exec api ruff check .
	docker compose exec api ruff format --check .

docker-typecheck:
	docker compose exec api mypy .

db-migrate:
	docker compose exec -T postgres sh -ec 'for f in /migrations/*.sql; do psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f "$$f"; done'

dbt-run:
	docker compose exec dbt-runner dbt run

dbt-test:
	docker compose exec dbt-runner dbt test

train:
	test -n "$(RUN_ID)" || (echo "Usage: make train RUN_ID=<completed-run>"; exit 1)
	docker compose run --rm --no-deps -v ./model:/app/model -v ./reports:/app/reports api python -m model.train --run-id "$(RUN_ID)"

evaluate:
	uv run --locked python -m model.evaluate --csv data/fixtures/smoke_1m_v1.csv --manifest data/manifests/fixture_smoke_v1.json --output-dir reports/evaluation --bundle-dir model/artefacts

shell-api:
	docker compose exec api /bin/bash

shell-db:
	docker compose exec postgres psql -U marketpulse_user -d marketpulse
