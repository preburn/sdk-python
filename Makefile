.DEFAULT_GOAL := help

include versions.env

PYTHON ?= $(PYTHON_VERSION)
DOCKER ?=
REVISIONS ?=

UV_IMAGE := ghcr.io/astral-sh/uv:$(UV_VERSION)-python$(PYTHON)-trixie-slim
DOCKER_RUN := docker run --rm --user "$$(id -u):$$(id -g)" \
	--volume "$(CURDIR):/workspace" --workdir /workspace \
	--env UV_CACHE_DIR=/workspace/.cache/uv \
	--env UV_PROJECT_ENVIRONMENT=/workspace/.cache/venv-$(PYTHON) \
	--env PREBURN_OPENAPI_PATH --env PREBURN_BASE_URL --env PREBURN_API_KEY \
	$(UV_IMAGE)
UV_RUN := $(if $(DOCKER),$(DOCKER_RUN)) uv run --locked --all-extras --python $(PYTHON)

.PHONY: help lint format typecheck test test-contract test-integration check-private-names install-hooks

help: ## List targets. PYTHON=3.x picks the interpreter, DOCKER=1 runs in the uv image
	@awk 'BEGIN { FS = ":.*## " } /^[a-z][a-z0-9-]*:.*## / { printf "  %-20s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

lint: ## ruff format check and ruff lint
	$(UV_RUN) ruff format --check
	$(UV_RUN) ruff check

format: ## ruff format and ruff lint fixes
	$(UV_RUN) ruff format
	$(UV_RUN) ruff check --fix

typecheck: ## mypy strict over src, tests and examples
	$(UV_RUN) mypy

test: ## Unit tests
	$(UV_RUN) pytest tests/unit

test-contract: ## Contract tests against PREBURN_OPENAPI_PATH, else the pinned server release
	$(UV_RUN) pytest tests/contract

test-integration: ## Integration tests against PREBURN_BASE_URL with PREBURN_API_KEY
	$(UV_RUN) pytest tests/integration -m integration

check-private-names: ## Scan the working tree, or REVISIONS (git rev-list arguments), for the names in PREBURN_PRIVATE_NAMES_FILE
ifeq ($(PREBURN_PRIVATE_NAMES_FILE),)
	@echo "skip check-private-names: PREBURN_PRIVATE_NAMES_FILE is not set"
else
	scripts/check-private-names.sh $(if $(REVISIONS),--revisions $(REVISIONS))
endif

install-hooks: ## Set core.hooksPath to .githooks
	git config core.hooksPath .githooks
