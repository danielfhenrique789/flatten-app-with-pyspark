#!/bin/bash

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")/.."

# Move to project root directory (e.g., /app)
cd "$ROOT_DIR" || exit 1

# Step 1: Create data folder if it doesn't exist
if [ ! -d "data" ]; then
  echo "Creating data/ directory..."
  mkdir -p data
else
  echo "data/ directory already exists."
fi

# Step 2: Start PostgreSQL container in detached mode
echo "Starting PostgreSQL container..."
docker-compose -f compose-postgres.yml up -d
if [ $? -ne 0 ]; then
  echo "Failed to start PostgreSQL container."
  exit 1
fi

# Step 3: Run tests
echo "Running tests..."
"$SCRIPT_DIR/run_tests.sh"
TEST_EXIT_CODE=$?

# Optional: Exit with test result code
if [ $TEST_EXIT_CODE -eq 0 ]; then
  echo "Setup complete. Tests passed!"
else
  echo "Setup finished, but tests failed."
fi

exit $TEST_EXIT_CODE