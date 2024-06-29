# %%
import pandas as pd
from pprint import pprint
from collections import defaultdict
import utils as u

# %%
BASE = "parquet/"
df = pd.read_parquet(BASE + "merged_df.parquet")
cols = list(df.columns)
# %%

# df.to_csv(BASE + "merged_df.csv", index=False)

# ----------------------------------------------------------------------
cols_dict = u.create_cols_dict(df)

# ----------------------------------------------------------------------
# %%
# Sort the keys of cols_dict
cols_dict = dict(sorted(cols_dict.items()))
for key, value in cols_dict.items():
    print(f"{key=}, {value=}")

# ----------------------------------------------------------------------
# %%
u.are_columns_equal(cols_dict, "Check column equality")

# ----------------------------------------------------------------------

print("=====================================")
print("What fraction of columns is identical?\n")
print("df[cols_dict['AC_REG_CD']].head(10)")
print(f"{cols_dict['AC_REG_CD']=}")
print(df[cols_dict["AC_REG_CD"]].head(10))

print(".................................................")
cols_dict1 = u.calculate_fracs(df, cols_dict, remove_equal=True, print_fracs=False)
cols_dict = u.calculate_fracs(df, cols_dict1, remove_equal=False, print_fracs=True)
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

u.print_freq_nan(df, cols_dict)

# cols_dict_nan = {}
# cols_dict_not_nan = {}
# for key, value in cols_dict.items():
#     cols_dict_nan[key] = [df[col].isna().sum() / df.shape[0] for col in value]
#     cols_dict_not_nan[key] = [df[col].notna().sum() / df.shape[0] for col in value]
# 
# pprint(cols_dict_nan)
# pprint(cols_dict_not_nan)

# print 10 values of the columns cols_dict['H_EVENT_ID']
# In principle, there are no NaNs for four columns
# print("H_EVENT_ID")
# print(df[cols_dict["H_EVENT_ID"]].head(10))

