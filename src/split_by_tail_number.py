"""
This script processes aircraft registration and logbook data by merging them on a common column 'AC_REG_CD'.
The merged data is then split into separate Parquet files based on the unique aircraft registration numbers.
The resulting files are saved in a specified output directory.

Usage:
    Run the script in an environment where the necessary parquet files are accessible.
    The output files will be saved in a specified directory named 'split_by_tail_number' under the 'copa_parquet' directory.
"""

import os

import pandas as pd


def load_data():
    """
    Load data from parquet files.

    Returns:
        pd.DataFrame: DataFrame for INV_AC_REG.
        pd.DataFrame: DataFrame for ISDP_LOGBOOK_REPORT.
    """
    INV_AC_REG = pd.read_parquet("copa_parquet/INV_AC_REG.parquet")
    ISDP_LOGBOOK_REPORT = pd.read_parquet("copa_parquet/ISDP LOGBOOK REPORT.parquet")

    return INV_AC_REG, ISDP_LOGBOOK_REPORT


def merge_data(INV_AC_REG, ISDP_LOGBOOK_REPORT):
    """
    Merge INV_AC_REG and ISDP_LOGBOOK_REPORT on 'AC_REG_CD'.

    Args:
        INV_AC_REG (pd.DataFrame): DataFrame containing aircraft registration data.
        ISDP_LOGBOOK_REPORT (pd.DataFrame): DataFrame containing logbook report data.

    Returns:
        pd.DataFrame: Merged DataFrame.
    """
    merged_df = pd.merge(INV_AC_REG, ISDP_LOGBOOK_REPORT, on="AC_REG_CD", how="inner")
    return merged_df


def save_data_by_tail_number(merged_df, output_dir):
    """
    Save the merged data into separate Parquet files based on 'AC_REG_CD'.

    Args:
        merged_df (pd.DataFrame): Merged DataFrame to be split and saved.
        output_dir (str): Directory path where files will be saved.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Directory {output_dir} created.")
    else:
        print(f"Directory {output_dir} already exists.")

    for AC_REG_CD, grp in merged_df.groupby("AC_REG_CD"):
        file_path = os.path.join(output_dir, f"{AC_REG_CD}.parquet")
        grp.to_parquet(file_path)
        print(f"Saved the data for tail number {AC_REG_CD} to {file_path}")


def main():
    """
    Main function to execute the data processing steps.
    """
    INV_AC_REG, ISDP_LOGBOOK_REPORT = load_data()

    print("Shape of INV_AC_REG:", INV_AC_REG.shape)
    print("Shape of ISDP_LOGBOOK_REPORT:", ISDP_LOGBOOK_REPORT.shape)

    print("Merging INV_AC_REG and ISDP_LOGBOOK_REPORT on AC_REG_CD")
    merged_df = merge_data(INV_AC_REG, ISDP_LOGBOOK_REPORT)
    print("Shape of merged dataframe:", merged_df.shape)

    output_dir = "./copa_parquet/split_by_tail_number/"
    save_data_by_tail_number(merged_df, output_dir)


if __name__ == "__main__":
    main()
