#!/bin/bash

# Define the script and SQL file paths
SCRIPT_DIR="$(dirname "$0")"   # Gets the directory of the current script
EXEC_SCRIPT="$SCRIPT_DIR/exec_db.sh"
SQL_FILE="$SCRIPT_DIR/../sql/init.sql"  # Navigate up one level to access sql folder

# Check if exec_db.sh exists
if [[ ! -f "$EXEC_SCRIPT" ]]; then
    echo "ERROR: exec_db.sh not found in $SCRIPT_DIR"
    exit 1
fi

# Check if init.sql exists
if [[ ! -f "$SQL_FILE" ]]; then
    echo "ERROR: init.sql not found in $SCRIPT_DIR/../sql/"
    exit 1
fi

# Run exec_db.sh and pass init.sql as input
echo "🚀 Running exec_db.sh with init.sql..."
bash "$EXEC_SCRIPT" "$SQL_FILE"

# Define a list of values
VALUES=("LoadRawData" "FlatteningAddress" "FlatteningTransactions" "SaveClient" "SaveAddress" "SaveTransactions")

# Loop through each value
for VALUE in "${VALUES[@]}"; do
    echo "Running $VALUE ..."

    # Run the command and exit immediately if it fails
    if ! docker-compose run pyspark-app "$VALUE"; then
        echo "ERROR: Execution failed for $VALUE. Stopping script."
        exit 1
    fi
done

echo "All jobs completed successfully!"