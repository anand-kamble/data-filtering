#!/bin/bash 
#
SRC_DIR="src"
ENTRY_FILE="main.py"

if [ "$1" == "--test" ]; then
    python tests/main.py
else
    python $SRC_DIR/$ENTRY_FILE \
    --config "filter_configs/var_of_interest.json" \
    --test_mode \
    --test_rows 33000 \
    --drop_duplicates \
    --split "merged" \
    "$@"
fi

