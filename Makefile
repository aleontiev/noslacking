.PHONY: install dev lint typecheck test check extract map-users validate migrate status sync clean build pypi pypi-test

# ─── Development ──────────────────────────────────────────────────────────────

install:  ## Install dependencies
	uv sync

dev:  ## Install with dev dependencies
	uv sync --extra dev

lint:  ## Run linter
	uv run ruff check src/ tests/

lint-fix:  ## Run linter with auto-fix
	uv run ruff check --fix src/ tests/

typecheck:  ## Run type checker
	uv run mypy src/

test:  ## Run tests
	uv run pytest

check: lint typecheck test  ## Run all checks (lint + typecheck + tests)

# ─── Migration Commands ───────────────────────────────────────────────────────

setup:  ## Interactive setup wizard
	uv run noslacking setup

extract:  ## Extract all Slack data
	uv run noslacking extract

map-users:  ## Map Slack users to Google Workspace users
	uv run noslacking map-users

validate:  ## Pre-flight validation
	uv run noslacking validate

migrate:  ## Execute migration to Google Chat
	uv run noslacking migrate

migrate-dry:  ## Dry-run migration (no writes)
	uv run noslacking migrate --dry-run

status:  ## Show migration progress
	uv run noslacking status --detail

sync:  ## Incremental sync of new messages
	uv run noslacking sync

# ─── Distribution ─────────────────────────────────────────────────────────────

clean:  ## Remove build artifacts
	rm -rf dist/ build/ src/*.egg-info

build: clean  ## Build sdist and wheel
	uv build

pypi-test: build  ## Upload to TestPyPI
	uv publish --publish-url https://test.pypi.org/legacy/

pypi: build  ## Upload to PyPI
	uv publish

# ─── Help ─────────────────────────────────────────────────────────────────────

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
