# Everything runs through `uv run`, which syncs the environment first, so there is no separate install step.

UV ?= uv
RUN := $(UV) run

.PHONY: help test format lint lint-fix typecheck check-all
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

check-all: lint typecheck test  ## Everything CI runs, in the order it fails fastest
