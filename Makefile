MODULE := beancount_tools_collection
COV_MIN := 30

.PHONY: install check lint format test build audit

install:
	uv sync --locked

check: lint
	uv run ruff format --check .
	uv lock --check

lint:
	uv run ruff check .

format:
	uv run ruff format .
	uv run ruff check --fix .

test:
	uv run pytest --cov=$(MODULE) --cov-report=term-missing --cov-fail-under=$(COV_MIN)

build:
	uv build --no-sources

audit:
	uvx zizmor@1 .github/workflows/
