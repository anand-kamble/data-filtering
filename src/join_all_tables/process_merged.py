# %%
import pandas as pd
from pprint import pprint
from collections import defaultdict

# %%
BASE = "parquet/"
df = pd.read_parquet(BASE + "merged_df.parquet")
cols = list(df.columns)
# %%

# df.to_csv(BASE + "merged_df.csv", index=False)

# ----------------------------------------------------------------------
# %%
# Identify all columns labels that don't contain a digit
def create_cols_dict(df):
    cols = list(df.columns)
    cols_no_digit = [col for col in cols if not any(c.isdigit() for c in col)]

    # For each column X in cols_no_digit, identify all columns whose names start with X and store them in a dict
    cols_dict = {}
    for col in cols_no_digit:
        cols_dict[col] = [c for c in cols if c.startswith(col)]

    return cols_dict

cols_dict = create_cols_dict(df)

# ----------------------------------------------------------------------
# %%
# Sort the keys of cols_dict
cols_dict = dict(sorted(cols_dict.items()))
for key, value in cols_dict.items():
    print(f"{key=}, {value=}")

# ----------------------------------------------------------------------
# %%
# For each key, check whether the first column is identical to each of the other columns. Return the results in another dict
#  Return the results in another dict with bool values.
def are_columns_equal(cols_dict, msg=""):
    cols_dict_identical = {}
    for key, value in cols_dict.items():
        first_col = value[0]
        cols_dict_identical[key] = [col == first_col for col in value]
    if msg:
        print(f"\n==== {msg} ====")
    pprint(cols_dict_identical)

are_columns_equal(cols_dict, "Check column equality")

def calculate_fracs(cols_dict_, remove_equal=False, print_fracs=True):
    # For each key, print the fraction of identical values between column j and column 0 for j > 0
    dict_ = defaultdict(list, [])
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
            # if col == col_label, don't keep
            if frac > 0.999:
                if keep_equal:
                    dict_[col_label].append(col)
            else:
                dict_[col_label].append(col)
    return dict_


print("=====================================")
print("What fraction of columns is identical?\n")
print("df[cols_dict['AC_REG_CD']].head(10)")
print(f"{cols_dict['AC_REG_CD']=}")
print(df[cols_dict["AC_REG_CD"]].head(10))

print(".................................................")
cols_dict1 = calculate_fracs(cols_dict, remove_equal=True, print_fracs=False)
cols_dict = calculate_fracs(cols_dict1, remove_equal=False, print_fracs=True)
print(".................................................")
quit()


print("=====================================")

# ----------------------------------------------------------------------
# %%
# Consider the column AC_REG_CD. Print the first 10 rows of the columns that start with AC_REG_CD
print("df[cols_dict['AC_REG_CD']].head(10)")
print(df[cols_dict["AC_REG_CD"]].head(10))
print("=====================================")


# ----------------------------------------------------------------------
# %%
# Create a new dictionary. Calculate the frequency of NaN to shape[0] for each column.
#  If the frequency is greater than 0.5, store the column in a list.
cols_dict_nan = {}
cols_dict_not_nan = {}
for key, value in cols_dict.items():
    cols_dict_nan[key] = [df[col].isna().sum() / df.shape[0] for col in value]
    cols_dict_not_nan[key] = [df[col].notna().sum() / df.shape[0] for col in value]

pprint(cols_dict_nan)
pprint(cols_dict_not_nan)

# print 10 values of the columns cols_dict['H_EVENT_ID']
# In principle, there are no NaNs for four columns
# print("H_EVENT_ID")
# print(df[cols_dict["H_EVENT_ID"]].head(10))
