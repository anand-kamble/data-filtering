import json
import os
from enum import unique

import pandas as pd

from copa_modules import _types, data_processor

BASE_PATH = "TABLES_ADD_20240515/"

config: _types.data_config | None = None
# Path of the config file relative to the run.sh file.
with open("filter_configs/var_of_interest.json") as f:
    config = json.load(f)
# Create a DataProcessor object with the configuration and base path
my_data_processor = data_processor(
    config,
    base_path=BASE_PATH,  # Path of the Directory where the dataset is located relative to run.sh.
    test_mode=True,  # Test mode which will only load a subset of the data
    test_rows=33000,  # Number of rows to load in test mode
    drop_duplicates=False,  # Drop duplicates from the dataset (Currently set true for Copa dataset)
    no_cache=False,  # Do not use cached data, i.e., data from the copa_output folder.
)

# Below Load function is returning the filtered dataframe.
my_filtered_data = my_data_processor.load()
print(f"Shape of filtered data: {my_filtered_data.shape}")


def split_dataframe_to_csv(df, column_name):
    """
    Splits a pandas DataFrame into separate CSV files based on unique values in the specified column.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    column_name (str): The name of the column to split the DataFrame by.
    """

    # Get the unique values from the specified column
    unique_values = df[column_name].unique()

    # Extract the first two letters from each ATA and store them in a set to get unique ATA
    unique_ATAs = set([x[:2] for x in unique_values])

    # Create the directory "ata_filtered" if it does not exist
    if not os.path.exists("ata_filtered"):
        os.makedirs("ata_filtered")

    # Loop through each unique prefix
    for value in unique_ATAs:
        subset_df = df[df[column_name].str.startswith(value)]
        filename = f"ata_filtered/ATA_{value[:2]}.csv"
        subset_df.to_csv(filename, index=False)
        print(f"File created: {filename}")


split_dataframe_to_csv(my_filtered_data, "ATA")
