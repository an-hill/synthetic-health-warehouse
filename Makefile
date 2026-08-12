# Everything runs through `uv run`, which syncs the environment first, so there is no separate install step.

UV ?= uv
RUN := $(UV) run
DBT := $(RUN) --group dbt dbt
# Kept apart from DBT because dbt rejects these ahead of the subcommand.
DBT_DIRS := --project-dir transform --profiles-dir transform

# Absolute because dbt resolves a relative path against the directory dbt was
# invoked from, and DuckDB silently creates whatever file that lands on.
# Overridable so a check can point at a copy rather than the real warehouse.
DBT_WAREHOUSE_PATH ?= $(CURDIR)/warehouse.duckdb
export DBT_WAREHOUSE_PATH

.PHONY: help test test-dags format lint lint-fix typecheck deps parse freshness build docs check-windows check-all
.DEFAULT_GOAL := help

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

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

# The airflow group is only needed so ty can resolve the DAG's imports.
typecheck:  ## Run ty
	$(RUN) --group airflow ty check

deps:  ## Install the packages named in transform/packages.yml
	$(DBT) deps $(DBT_DIRS)

# A prerequisite of everything that compiles the project rather than a step to
# remember, since a fresh clone has no dbt_packages and dbt then refuses to run.
parse: deps  ## Build the dbt manifest the DAGs render from
	$(DBT) parse $(DBT_DIRS)

# Out of check-all because it needs both optional groups: dbt to build the
# manifest Cosmos renders from, and airflow to import the DAGs at all.
# AIRFLOW_HOME is pinned so that importing airflow does not create one at ~.
test-dags: parse  ## Parse both DAGs, and check the selector splitting them still covers the project and matches the README
	AIRFLOW_HOME=$(CURDIR)/.airflow $(RUN) --group airflow pytest tests/dags

# Out of check-all deliberately: these need a loaded warehouse and the dbt group,
# and check-all has to keep running without either.
freshness: deps  ## Check how recently the loader last wrote each raw table
	$(DBT) source freshness $(DBT_DIRS)

build: deps  ## Build the models and run their tests
	$(DBT) build $(DBT_DIRS)

# Out of check-all for the same reason as freshness, and additionally because
# these hold only once a second window has been landed and built.
check-windows:  ## Assert what two landed windows should have produced
	$(RUN) python scripts/check_warehouse.py merge-reached-an-earlier-build
	$(RUN) python scripts/check_warehouse.py snapshot-captured-the-payer-changes
	$(RUN) python scripts/check_warehouse.py readmissions-match-the-export

# Regenerated every time because serve will otherwise hand back an older graph
# without saying so.
docs: deps  ## Serve the model documentation and lineage graph on localhost:8080
	$(DBT) docs generate $(DBT_DIRS)
	$(DBT) docs serve $(DBT_DIRS)

check-all: lint typecheck test  ## Lint, typecheck, and tests, in the order they fail fastest
