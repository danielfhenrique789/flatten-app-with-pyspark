#!/bin/bash

# Define the scripts directory
SCRIPTS_DIR="scripts/sh"

# Check if a script name was provided
if [ -z "$1" ]; then
    echo "❌ ERROR: No script name provided."
    echo "Usage: ./app.sh --<script_name> [arguments]"
    exit 1
fi

# Extract script name by removing the '--' prefix
SCRIPT_NAME="${1#--}"

# Construct the full script path
SCRIPT_PATH="$SCRIPTS_DIR/$SCRIPT_NAME.sh"

# Check if the script exists
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "ERROR: Script '$SCRIPT_NAME.sh' not found in $SCRIPTS_DIR"
    exit 1
fi

# Execute the script, passing any additional arguments
bash "$SCRIPT_PATH" "${@:2}"