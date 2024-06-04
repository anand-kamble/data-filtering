#!/bin/bash

SRC_DIR="src"
ENTRY_FILE="main.py"

# Default arguments
DEFAULT_ARGS=(
    --config "filter_configs/var_of_interest.json"
    --test_mode
    --test_rows 33000
    --drop_duplicates
    --split "merged"
)

# Check if arguments are provided
if [ $# -eq 0 ]; then
    ARGS=("${DEFAULT_ARGS[@]}")
else
    ARGS=("$@")
fi

python $SRC_DIR/$ENTRY_FILE "${ARGS[@]}"
