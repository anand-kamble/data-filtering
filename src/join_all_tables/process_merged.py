# %%
import pandas as pd
from pprint import pprint

# %%
BASE = "parquet/"
df = pd.read_parquet(BASE + "merged_df.parquet")
cols = list(df.columns)
# %%

# df.to_csv(BASE + "merged_df.csv", index=False)

# %%
# Identify all columns labels that don't contain a digit
cols = list(df.columns)
cols_no_digit = [col for col in cols if not any(c.isdigit() for c in col)]

# For each column X in cols_no_digit, identify all columns whose names start with X and store them in a dict
cols_dict = {}
for col in cols_no_digit:
    cols_dict[col] = [c for c in cols if c.startswith(col)]

# %%
# Sort the keys of cols_dict
cols_dict = dict(sorted(cols_dict.items()))
for key, value in cols_dict.items():
    print(f"{key=}, {value=}")

# %%
# For each key, check whether the first column is identical to each of the other columns. Return the results in another dict
#  Return the results in another dict with bool values.
cols_dict_identical = {}
for key, value in cols_dict.items():
    first_col = value[0]
    cols_dict_identical[key] = [col == first_col for col in value]

pprint(cols_dict_identical)
# %%
# Consider the column AC_REG_CD. Print the first 10 rows of the columns that start with AC_REG_CD
print(df[cols_dict["AC_REG_CD"]].head(10))
