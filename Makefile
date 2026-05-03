# Globals
PYTHON ?= python3.11
COMPOSE = docker compose

VENV_NAME = venv
VENV_BIN = $(VENV_NAME)/bin
VENV_PYTHON = $(VENV_BIN)/python

# Commands
.PHONY: create-env
create-env:
	@echo "Creating Python Virtual Environment"
	@$(PYTHON) -m venv --clear $(VENV_NAME)
	@$(VENV_PYTHON) -m ensurepip --upgrade
	@$(VENV_PYTHON) -m pip install --upgrade pip setuptools wheel

.PHONY: install-requirements
install-requirements:
	@$(VENV_PYTHON) -m ensurepip --upgrade
	@$(VENV_PYTHON) -m pip install --upgrade setuptools wheel
	@$(VENV_PYTHON) -m pip install -e .

.PHONY: project_init
project_init: create-env install-requirements


# Scripts
.PHONY: download_dataset
download_dataset:
	$(VENV_PYTHON) -m src.ingestion.download_dataset

.PHONY: upload_dataset
upload_dataset:
	$(VENV_PYTHON) -m src.ingestion.upload_dataset

.PHONY: ingest_all
ingest_all: download_dataset upload_dataset

.PHONY: build_sot
build_sot:
	$(VENV_PYTHON) -m src.processing.build_sot

.PHONY: upload_sot
upload_sot:
	$(VENV_PYTHON) -m src.processing.upload_sot

.PHONY: process_all
process_all: build_sot upload_sot

.PHONY: build_spec
build_spec:
	$(VENV_PYTHON) -m src.feature_engineering.build_spec

.PHONY: upload_spec
upload_spec:
	$(VENV_PYTHON) -m src.feature_engineering.upload_spec

.PHONY: features_all
features_all: build_spec upload_spec


# Conteiners
.PHONY: stack_up
stack_up:
	@$(COMPOSE) up --build --detach

.PHONY: stack_down
stack_down:
	@$(COMPOSE) down --remove-orphans

.PHONY: stack_logs
stack_logs:
	@$(COMPOSE) logs --follow

minio-up:
	$(COMPOSE) up --build --detach minio

minio-down:
	$(COMPOSE) down --remove-orphans minio

minio-logs:
	$(COMPOSE) logs -f minio


# Housekeeping
.PHONY: lint
lint:
	@$(VENV_PYTHON) -m ruff check . --exclude notebooks
	@$(VENV_PYTHON) -m ruff format --check . --exclude notebooks

.PHONY: format
format:
	@$(VENV_PYTHON) -m ruff check --fix . --exclude notebooks
	@$(VENV_PYTHON) -m ruff format . --exclude notebooks

.PHONY: clean
clean:
	@echo "Cleaning Python cache files..."
	@find . -type f -name "*.py[co]" -delete
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type d -name ".ruff_cache" -exec rm -rf {} +
	@echo "Cleaning test cache files..."
	@find . -type d -name ".pytest_cache" -exec rm -rf {} +
	@echo "Cleaning build and distribution files..."
	@find . -type d -name "*.egg-info" -exec rm -rf {} +
	@find . -type d -name "build" -exec rm -rf {} +
	@find . -type d -name "dist" -exec rm -rf {} +
	@find . -type d -name ".cache" -exec rm -rf {} +
	@echo "Cleaning Jupyter notebook cache..."
	@find . -type d -name ".ipynb_checkpoints" -exec rm -rf {} +
	@echo "Clean complete!"
