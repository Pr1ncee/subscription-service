.PHONY: format lint typecheck test check

format:
	uv run black . && uv run ruff check --fix .

lint:
	uv run black --check . && uv run ruff check .

typecheck:
	uv run mypy && uv run lint-imports

test:
	uv run pytest -m "not e2e" --cov --cov-report=term-missing --cov-report=xml

check: lint typecheck test
