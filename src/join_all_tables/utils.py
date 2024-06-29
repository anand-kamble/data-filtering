""" Utility functions """
import pandas as pd
from pprint import pprint
from collections import defaultdict

def create_cols_dict(df):
    """ Identify all columns labels that don't contain a digit """
    cols = list(df.columns)
    cols_no_digit = [col for col in cols if not any(c.isdigit() for c in col)]

    # For each column X in cols_no_digit, identify all columns whose names start with X and store them in a dict
    cols_dict = {}
    for col in cols_no_digit:
        cols_dict[col] = [c for c in cols if c.startswith(col)]

    return cols_dict


# ----------------------------------------------------------------------

def are_columns_equal(cols_dict, msg=""):
    """  
       For each key, check whether the first column is identical to each of the other columns. 
          Return the results in another dict
          Return the results in another dict with bool values.
    """
    cols_dict_identical = {}
    for key, value in cols_dict.items():
        first_col = value[0]
        cols_dict_identical[key] = [col == first_col for col in value]
    if msg:
        print(f"\n==== {msg} ====")
    pprint(cols_dict_identical)

# ----------------------------------------------------------------------

def calculate_fracs(df, cols_dict_, remove_equal=False, print_fracs=True):
    # For each key, print the fraction of identical values between column j and column 0 for j > 0
    dict_ = defaultdict(list, [])
    if print_fracs:
        print("===================================================")
        print("Fraction of rows equal to base column before merge")

    keep_equal = not remove_equal
    for col_label, cols in cols_dict_.items():
        for col in cols:
            if col == col_label:
                dict_[col_label].append(col_label)
                continue
            # What is the fraction of the number of rows for which df[cols_dict[col_label]] == df[col]
            frac = (df[col_label] == df[col]).sum() / df.shape[0]
            if print_fracs:
                print(f"{col_label} vs {col}: {frac=}")
                print(df[[col_label, col]].head(10))
            # if col == col_label, don't keep
            if frac > 0.999:
                if keep_equal:
                    dict_[col_label].append(col)
            else:
                dict_[col_label].append(col)
    return dict_

# ----------------------------------------------------------------------
def print_freq_nan(df, cols_dict):
    """ Print fraction of NaN and not NaN in rows """
    cols_dict_nan = {}
    cols_dict_not_nan = {}
    for key, value in cols_dict.items():
        cols_dict_nan[key] = [df[col].isna().sum() / df.shape[0] for col in value]
        cols_dict_not_nan[key] = [df[col].notna().sum() / df.shape[0] for col in value]

    print("================================================")
    print("Fraction of NaNs")
    pprint(cols_dict_nan)

    # print("================================================")
    # print("Fraction of Non-NaNs")
    # pprint(cols_dict_not_nan)
    return cols_dict_nan, cols_dict_not_nan
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
