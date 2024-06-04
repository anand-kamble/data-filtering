#!/bin/bash 
#
SRC_DIR="src"
ENTRY_FILE="main.py"

python $SRC_DIR/$ENTRY_FILE \
    --config "filter_configs/var_of_interest.json" \
    --test_mode \
    --test_rows 33000 \
    --drop_duplicates \
    --split "merged" \
    "$@"
