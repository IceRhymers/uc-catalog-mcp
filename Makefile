.PHONY: test lint run migrate

test:
	pytest tests/ -v

lint:
	ruff check . && ruff format --check .

run:
	uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

migrate:
	uv run python scripts/migrate.py
