#!/bin/bash

# Define paths to your data folders
DATA_FOLDER_1="./copa"
DATA_FOLDER_2="./TABLES_ADD_20240515"

# Function to check if a folder exists
check_folder() {
    local folder="$1"
    if [ -d "$folder" ]; then
        echo "Data folder '$folder' found."
    else
        echo "Error: Data folder '$folder' not found. Please create the folder or check the path."
        exit 1
    fi
}

# Check if both folders exist
check_folder "$DATA_FOLDER_1"
check_folder "$DATA_FOLDER_2"

# Run the Python script if both folders exist
python src/write_parquet.py
