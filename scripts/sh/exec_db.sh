#!/bin/bash

# Default command to run PostgreSQL inside the container
DB_CONTAINER="postgres-db"
DB_USER="postgresuser"
DB_NAME="mydatabase"

# Check if an argument (SQL file) is provided
if [[ "$#" -gt 0 ]]; then
    SQL_FILE="$1"

    # Check if the provided argument is a valid file
    if [[ -f "$SQL_FILE" ]]; then
        echo "🚀 Running SQL file: $SQL_FILE..."
        docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" < "$SQL_FILE"
    else
        echo "🚀 Running command inside PostgreSQL: $@"
        docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" "$@"
    fi
else
    echo "🚀 Connecting to PostgreSQL interactive shell..."
    docker exec -it "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME"
fi