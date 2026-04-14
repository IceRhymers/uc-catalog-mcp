.PHONY: test lint run

test:
	pytest tests/ -v

lint:
	ruff check . && ruff format --check .

run:
	uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
