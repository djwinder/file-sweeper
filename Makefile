.PHONY: setup test lint type fmt tf-validate ci
setup:
	uv venv || true
	uv pip install -e ".[dev]"
	pre-commit install

test:
	uv run pytest

lint:
	uv run ruff check .

type:
	uv run mypy src

fmt:
	uv run ruff format .

tf-validate:
	cd infra/envs/dev && terraform init -backend=false && terraform validate && tflint

ci: fmt lint type test tf-validate
