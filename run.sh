#!/bin/bash 
#
SRC_DIR="src"
ENTRY_FILE="main.py"

if [ "$1" == "--test" ]; then
    python tests/main.py
else
    python $SRC_DIR/$ENTRY_FILE
fi
