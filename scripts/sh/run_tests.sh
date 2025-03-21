#!/bin/bash

# Always resolve to project root (assumes this script is in scripts/sh)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

# Move to project root
cd "$PROJECT_ROOT" || exit 1

SERVICE_NAME="pyspark-app-test"

# All additional args passed to pytest
EXTRA_ARGS="$*"

# Run from /app where tests/ is located in the container
docker-compose run --rm "$SERVICE_NAME" -c "$EXTRA_ARGS"