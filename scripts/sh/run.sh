#!/bin/bash

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")/.."

# Navigate to the project root (so paths work correctly)
cd "$PROJECT_ROOT" || exit 1

# Check if a job name is provided
if [ $# -lt 1 ]; then
  echo "❌ Usage: $0 <JobName>"
  exit 1
fi

JOB_NAME="$1"
shift  # Shift arguments so any extras can be passed

echo "🚀 Running Spark Job: $JOB_NAME ..."
docker-compose run --rm pyspark-app "$JOB_NAME" "$@"