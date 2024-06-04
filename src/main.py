import argparse
import json
import os
from ast import parse
from enum import unique
from typing import Literal

import pandas as pd

from copa_modules import _types, data_processor


def split_dataframe_to_csv(
    df, column_name, split_type: Literal["full", "merged"] = "full"
):
    """
    Splits a pandas DataFrame into separate CSV files based on unique values in the specified column.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    column_name (str): The name of the column to split the DataFrame by.
    """
    # Create the directory "ata_filtered" if it does not exist
    if not os.path.exists("ata_filtered"):
        os.makedirs("ata_filtered")

    if split_type == "merged":
        # Get the unique values from the specified column
        unique_values = df[column_name].unique()

        # Extract the first two letters from each ATA and store them in a set to get unique ATA
        unique_ATAs = set([x[:2] for x in unique_values])

        # Loop through each unique prefix
        for value in unique_ATAs:
            subset_df = df[df[column_name].str.startswith(value)]
            filename = f"ata_filtered/ATA_{value[:2]}.csv"
            subset_df.to_csv(filename, index=False)
            print(f"File created: {filename}")

    elif split_type == "full":
        unique_values = df[column_name].unique()

        for value in unique_values:
            subset_df = df[df[column_name] == value]
            filename = f"ata_filtered/ATA_{value}.csv"
            subset_df.to_csv(filename, index=False)
            print(f"File created: {filename}")


def main():

    parser = argparse.ArgumentParser(description="Copa Module")

    parser.add_argument(
        "--split",
        choices=["full", "merged"],
        required=False,
        default="full",
        help="Specify how the data is separate in CSV files.\n'full' creates separate CSV for every possible ATA\n'merged' merges the sub classes of ATA and creates separate CSV files.",
    )

    parser.add_argument("--config", required=True, help="Path to config file.")

    parser.add_argument(
        "--test_mode",
        action="store_true",
        help="Enable test mode which will only load a subset of the data",
    )

    parser.add_argument(
        "--test_rows",
        type=int,
        default=1000,
        help="Number of rows to load in test mode",
    )

    parser.add_argument(
        "--drop_duplicates",
        action="store_true",
        help="Drop duplicates from the dataset",
    )

    parser.add_argument(
        "--no_cache", action="store_true", help="Disable caching if applicable"
    )

    args = parser.parse_args()

    config: _types.data_config | None = None

    # Path of the config file relative to the run.sh file.
    with open(args.config) as f:
        config = json.load(f)
    # Create a DataProcessor object with the configuration and base path
    my_data_processor = data_processor(
        config,
        test_mode=args.test_mode,  # Test mode which will only load a subset of the data
        test_rows=args.test_rows,  # Number of rows to load in test mode
        drop_duplicates=args.drop_duplicates,  # Drop duplicates from the dataset (Currently set true for Copa dataset)
        no_cache=args.no_cache,  # Do not use cached data, i.e., data from the copa_output folder.
    )

    # Below Load function is returning the filtered dataframe.
    my_filtered_data = my_data_processor.load()
    print(f"Shape of filtered data: {my_filtered_data.shape}")
    split_dataframe_to_csv(my_filtered_data, "ATA", args.split)


if __name__ == "__main__":
    main()
