"""
Testing for the joining of the dataframes by following keys:

('INV_LOC.csv', 'ALT_ID')
('REF_FAIL_CATGRY.csv', 'RSTAT_CD')
('REF_FAIL_CATGRY.csv', 'REVISION_DB_ID')
('INV_LOC.csv', 'LOC_ID')
('REF_FLIGHT_STAGE.csv', 'BITMAP_TAG')
('REF_FAIL_SEV.csv', 'FAIL_SEV_CD')
('REF_FAIL_PRIORITY.csv', 'FAIL_PRIORITY_CD')
('REF_FAULT_SOURCE.csv', 'FAULT_SOURCE_CD')
('REF_FLIGHT_STAGE.csv', 'FLIGHT_STAGE_CD')
('REF_FAIL_CATGRY.csv', 'FAIL_CATGRY_CD')
('SD_FAULT.csv', 'FAULT_ID')
('REF_FAULT_LOG_TYPE.csv', 'USER_CD')
('EVT_EVENT.csv', 'EVENT_ID')
('INV_AC_REG.csv', 'AC_REG_CD')
"""

# %%
import pandas as pd

# %%

tables = [
    "INV_LOC",
    "REF_FAIL_CATGRY",
    "REF_FAIL_CATGRY",
    "INV_LOC",
    "REF_FLIGHT_STAGE",
    "REF_FAIL_SEV",
    "REF_FAIL_PRIORITY",
    "REF_FAULT_SOURCE",
    "REF_FLIGHT_STAGE",
    "REF_FAIL_CATGRY",
    "SD_FAULT",
    "REF_FAULT_LOG_TYPE",
    "EVT_EVENT",
    "INV_AC_REG",
]

df: dict[str, pd.DataFrame] = dict()

for table in tables:
    df[table] = pd.read_parquet(f"../copa_parquet/{table}.parquet")

# %% Print the shape of the dataframes
# Here I will be prioritizing the tables with the most number of rows
print("-" * 40, "\nDataframes shape: ")
for d in df:
    print(d, df[d].shape)

# %% Select the two dataframes to join
# In this code the df1 will have priority over df2 as the join method has been set to left
df1 = df["INV_LOC"]
df2 = df["SD_FAULT"]
# %%
set(df1.columns).intersection(set(df2.columns))

# %% Check the shape of the two selected dataframes
print("-" * 40 + "\n", "Selected dataframes shape: ", df1.shape, df2.shape)
# %% Pick 1000 random samples from the dataframes
num_samples = 1000
test_df1 = df1.sample(num_samples)
test_df2 = df2.sample(num_samples)
print("-" * 40 + "\n", "Test dataframes shape: ")
print(test_df1.shape, test_df2.shape)
# %% Check the number of unique values in the columns
# We want this number to be same as the number of rows in the dataframe
# This will ensure that the column can be used as a key for joining the dataframes
# has unique values
print(
    "-" * 40 + "\n", "Number of unique values in the column ALT_ID of test df1 and df2"
)
print(test_df1["ALT_ID"].nunique(), test_df2["ALT_ID"].nunique())

# %%

merged_table = df1.merge(df2, on="ALT_ID", how="left")
print("-" * 40 + "\n", "Final merged table shape: ", merged_table.shape)
# merged_table.to_parquet("../copa_parquet/INV_LOC_SD_FAULT_merged.parquet")

# %%
df3 = df["EVT_EVENT"]

# %%
set(df1.columns).intersection(set(df3.columns))

# %%
set(df2.columns).intersection(set(df3.columns))

# %%
test_df3 = df3.sample(num_samples)

# %%
print(
    "-" * 40 + "\n",
    "Number of unique values in the column EVENT_ID of test df1 and df3",
)
print(test_df1["ALT_ID"].nunique(), test_df3["ALT_ID"].nunique())

# Output of the above code:
# Number of unique values in the column EVENT_ID of test df1 and df3
# 1000 1000
# %% Finding the common columns between the three tables
common_columns = (
    set(df1.columns).intersection(set(df2.columns)).intersection(set(df3.columns))
)
print("-" * 40 + "\n")
print("Common columns in df1, df2, and df3: ", common_columns)
# %% Let us find the number of common values in ALT_ID in the given table.
# This will help us to determine the number of rows that will be merged

for col in common_columns:
    print("-" * 40 + "\n")
    print(f"Checking the column: {col}")
    print("Number of unique values in the column in df1: ", df1[col].nunique())
    print("Number of unique values in the column in df2: ", df2[col].nunique())
    print("Number of unique values in the column in df3: ", df3[col].nunique())
    print(
        f"Number of {col} in df1 that are also in df2: ",
        df1[col].isin(df2[col]).sum(),
    )
    print(
        f"Number of {col} in df1 that are also in df3: ",
        df1[col].isin(df3[col]).sum(),
    )
    print(
        f"Number of {col} in df2 that are also in df3: ",
        df2[col].isin(df3[col]).sum(),
    )

# Output of the above code:
# Number of ALT_ID in df1 that are also in df2:  0
# Number of ALT_ID in df1 that are also in df3:  0
# Number of ALT_ID in df2 that are also in df3:  0

# %%
