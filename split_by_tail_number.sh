#!/bin/bash

# Define variables
SRC_DIR="src"
ENTRY_FILE="split_by_tail_number.py"
FILE1="copa_parquet/INV_AC_REG.parquet"
FILE2="copa_parquet/ISDP LOGBOOK REPORT.parquet"

# Check if the necessary files are present
if [ ! -f "$FILE1" ]; then
    echo "Error: $FILE1 not found. Please make sure the file exists in the copa_parquet directory."
    exit 1
fi

if [ ! -f "$FILE2" ]; then
    echo "Error: $FILE2 not found. Please make sure the file exists in the copa_parquet directory."
    exit 1
fi

# Run the Python script
echo "All necessary files are present. Running the Python script..."
python $SRC_DIR/$ENTRY_FILE
