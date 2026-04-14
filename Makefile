.PHONY: test lint run setup-db deploy migrate sync check fmt fmt-check

test:
	pytest tests/ -v

lint:
	uv run ruff check . && uv run ruff format --check .

run:
	uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Provisions Lakebase for the app: grants schema access to the app service
# principal, then runs Alembic migrations to create/update tables.
# Run once as a developer with workspace CLI auth before deploying the app.
setup-db:
	uv run python scripts/migrate.py

# Full deployment: build + validate + bundle deploy + migrate + start app.
deploy:
	bash scripts/deploy.sh full

# Run Alembic migrations only (no SP grant, no bundle deploy).
migrate:
	uv run python -m alembic upgrade head

# Manually trigger the sync job via Databricks CLI.
sync:
	databricks bundle run uc_catalog_sync

# CI gate: lint + format check.
check:
	make lint

# Auto-fix formatting.
fmt:
	uv run ruff format app/ tests/

# Check formatting without fixing (used by CI).
fmt-check:
	uv run ruff format --check app/ tests/
