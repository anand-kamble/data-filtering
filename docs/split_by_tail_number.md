## Overview

This repository contains a Python script and a bash script to process aircraft registration and logbook data. The Python script merges data from two Parquet files, splits the merged data by aircraft tail number (`AC_REG_CD`), and saves the resulting subsets into separate Parquet files. The bash script is provided to easily run the Python script.

## File Structure

- `src/split_by_tail_number.py`: The main Python script for merging and splitting the aircraft data.
- `split_by_tail_number.sh`: A bash script to run the Python script.
- `copa_parquet/`: Directory where the input Parquet files should be located.
- `copa_parquet/split_by_tail_number/`: Directory where the output files will be saved.

## Usage

### Running the Python Script Directly

1. **Prepare the input data**:
   - Place your `INV_AC_REG.parquet` and `ISDP LOGBOOK REPORT.parquet` files inside the `copa_parquet/` directory.

2. **Run the Python script**:

    ```bash
    python src/split_by_tail_number.py
    ```

   The script will:
   - Merge `INV_AC_REG` and `ISDP LOGBOOK REPORT` on `AC_REG_CD`.
   - Split the merged data by `AC_REG_CD`.
   - Save the split data into the `copa_parquet/split_by_tail_number/` directory.

### Running via Bash Script

1. **Ensure the bash script has execute permissions**:

    ```bash
    chmod +x split_by_tail_number.sh
    ```

2. **Run the bash script**:

    ```bash
    ./split_by_tail_number.sh
    ```

   This will execute the Python script and process the data as described above.

## Script Details

### Python Script (`split_by_tail_number.py`)

The Python script performs the following steps:

1. **Load Data**: Reads data from `INV_AC_REG.parquet` and `ISDP LOGBOOK REPORT.parquet` files.
2. **Merge Data**: Merges the two datasets on the `AC_REG_CD` column.
3. **Save Data**: Splits the merged dataset by `AC_REG_CD` and saves each subset into a separate Parquet file named according to the aircraft tail number.

**Functions:**

- `load_data()`: Loads data from the specified Parquet files.
- `merge_data(INV_AC_REG, ISDP_LOGBOOK_REPORT)`: Merges the datasets on `AC_REG_CD`.
- `save_data_by_tail_number(merged_df, output_dir)`: Saves the merged data into separate files based on `AC_REG_CD`.
- `main()`: Main function that orchestrates the loading, merging, and saving of data.


## Bash Script (`split_by_tail_number.sh`)

The bash script is designed to run the Python script after ensuring that the required Parquet files are present in the specified directory. 

**Content of `split_by_tail_number.sh`:**

```bash
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
python "$SRC_DIR/$ENTRY_FILE"
```

## Example Output

After running the scripts, the `copa_parquet/split_by_tail_number/` directory will contain multiple Parquet files, each named according to an aircraft's tail number (e.g., `N12345.parquet`, `N67890.parquet`, etc.).

## Troubleshooting

- Ensure that the Parquet files `INV_AC_REG.parquet` and `ISDP LOGBOOK REPORT.parquet` exist in the `copa_parquet/` directory.
- Verify that you have the required Python packages installed using poetry.
- Check the Python script for any errors or exceptions that may provide more information about the issue.
