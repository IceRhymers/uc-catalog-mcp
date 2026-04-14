.PHONY: test lint run setup-db

test:
	pytest tests/ -v

lint:
	ruff check . && ruff format --check .

run:
	uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Provisions Lakebase for the app: grants schema access to the app service
# principal, then runs Alembic migrations to create/update tables.
# Run once as a developer with workspace CLI auth before deploying the app.
setup-db:
	uv run python scripts/migrate.py
