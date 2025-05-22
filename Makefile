DOCKER_COMPOSE = docker compose -f docker-compose.dev.yml
PYTHON_RUN = $(DOCKER_COMPOSE) run --rm python

# Default target when running `make`
.PHONY: all
all: docker-up

# Create .env file to set the UID/GID for the docker containers to run as to the current user
.env:
	echo "DOCKER_LOCAL_USER=$(shell id -u):$(shell id -g)" >> .env

# Builds and starts all docker containers
.PHONY: docker-up
docker-up: docker-build
	$(DOCKER_COMPOSE) up $(SERVICE)

# Tear down all containers and delete all volumes
.PHONY: docker-purge
docker-purge: .env
	$(DOCKER_COMPOSE) down --remove-orphans --volumes

# Build the Docker image for the python service
.PHONY: docker-build
docker-build: .env
	$(DOCKER_COMPOSE) build python

# Start a shell (bash) in the python docker container
.PHONY: docker-shell
docker-shell:
	$(DOCKER_COMPOSE) run --rm python bash

# Run tests
.PHONY: test
test:
	$(DOCKER_COMPOSE) run --rm python python -m pytest tests
	$(DOCKER_COMPOSE) down

# Run formatter and linter
.PHONY: lint-fix
lint-fix:
	$(PYTHON_RUN) ruff check --fix ./pipeline ./tests
	$(PYTHON_RUN) ruff format ./pipeline ./tests
	# mypy has no fix mode, we run it anyway to report (unfixable) errors
	$(PYTHON_RUN) mypy ./pipeline

# Check with formatter and linter
.PHONY: lint-check
lint-check:
	$(PYTHON_RUN) ruff check ${MAIN_MODULE}
	$(PYTHON_RUN) ruff format ./pipeline ./tests
	$(PYTHON_RUN) mypy ./pipeline
