PYTHON ?= python3
ALEMBIC_CONFIG ?= alembic.ini

.PHONY: install-dev build migrate migrate-down

install-dev:
	$(PYTHON) -m pip install -e '.[dev]'

build:
	$(PYTHON) -m build
