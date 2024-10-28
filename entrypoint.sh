#!/usr/bin/env bash

# entrypoint.sh

# Navigate to the working directory
cd /airbyte

# Execute the main Python script with any passed arguments
exec python main.py "$@"
