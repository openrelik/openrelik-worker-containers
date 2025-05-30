# Use the official Docker Hub Ubuntu base image
FROM --platform=linux/amd64 ubuntu:24.04 AS ubuntu_2404_amd64

ARG PPA_TRACK=stable

# Prevent needing to configure debian packages, stopping the setup of
# the docker container.
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    python3-poetry \
    wget \
    sudo \
    fdisk \
    qemu-utils \
    ntfs-3g \
  && rm -rf /var/lib/apt/lists/*

# Install latest release of container explorer
COPY ./setup.sh ./setup.sh
RUN chmod +x ./setup.sh
RUN ./setup.sh install

# Configure poetry
ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Configure debugging
ARG OPENRELIK_PYDEBUG
ENV OPENRELIK_PYDEBUG=${OPENRELIK_PYDEBUG:-0}
ARG OPENRELIK_PYDEBUG_PORT
ENV OPENRELIK_PYDEBUG_PORT=${OPENRELIK_PYDEBUG_PORT:-5678}

# Set working directory
WORKDIR /openrelik

# Enable system-site-packages
RUN poetry config virtualenvs.options.system-site-packages true

# Copy poetry toml and install dependencies
COPY ./pyproject.toml ./poetry.lock ./
RUN poetry install --no-interaction --no-ansi

# Copy all worker files
COPY . ./

# Install the worker and set environment to use the correct python interpreter.
RUN poetry install && rm -rf $POETRY_CACHE_DIR
ENV VIRTUAL_ENV=/app/.venv PATH="/openrelik/.venv/bin:$PATH"

# Default command if not run from docker-compose (and command being overidden)
CMD ["celery", "--app=src.tasks", "worker", "--task-events", "--concurrency=1", "--loglevel=INFO"]
