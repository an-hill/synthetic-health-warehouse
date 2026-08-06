# Everything runs through `uv run`, which syncs the environment first, so there is no separate install step.

UV ?= uv
RUN := $(UV) run
# Absolute because dbt resolves a relative path against the directory dbt was
# invoked from, and DuckDB silently creates whatever file that lands on.
# Overridable so a check can point at a copy rather than the real warehouse.
DBT_WAREHOUSE_PATH ?= $(CURDIR)/warehouse.duckdb
export DBT_WAREHOUSE_PATH

.PHONY: help test format lint lint-fix typecheck freshness build check-all
.DEFAULT_GOAL := help

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-11s\033[0m %s\n", $$1, $$2}'

test:  ## Run the test suite
	$(RUN) pytest

format:  ## Format in place
	$(RUN) ruff format .

# Local only - never CI/CD
lint-fix:  ## Apply lint autofixes, then reformat
	$(RUN) ruff check --fix .
	$(RUN) ruff format .

lint:  ## Check style and formatting
	$(RUN) ruff check .
	$(RUN) ruff format --check .

typecheck:  ## Run ty
	$(RUN) ty check

# Out of check-all deliberately: these need a loaded warehouse and the dbt group,
# and check-all has to keep running without either.
freshness:  ## Check how recently the loader last wrote each raw table
	$(RUN) --group dbt dbt source freshness --project-dir transform --profiles-dir transform

build:  ## Build the models and run their tests
	$(RUN) --group dbt dbt build --project-dir transform --profiles-dir transform

check-all: lint typecheck test  ## Lint, typecheck, and tests, in the order they fail fastest
