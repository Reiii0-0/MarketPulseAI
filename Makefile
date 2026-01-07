.PHONY: up down logs test test-unit test-integration lint typecheck dbt-run dbt-test train shell-api shell-db

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

test:
	docker compose exec api pytest tests/ -v --cov=api --cov=consumer --cov=producer --cov=model --cov-fail-under=85 --cov-branch

test-unit:
	docker compose exec api pytest tests/unit/ -v

test-integration:
	docker compose exec api pytest tests/integration/ -v

lint:
	docker compose exec api ruff check .
	docker compose exec api ruff format --check .

typecheck:
	docker compose exec api mypy .

dbt-run:
	docker compose exec dbt-runner dbt run

dbt-test:
	docker compose exec dbt-runner dbt test

train:
	docker compose exec api python model/train.py

shell-api:
	docker compose exec api /bin/bash

shell-db:
	docker compose exec postgres psql -U marketpulse_user -d marketpulse
