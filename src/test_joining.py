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
# List of table names that we will be reading into dataframes
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

# Dictionary to store the dataframes
df: dict[str, pd.DataFrame] = dict()

# Reading each table's Parquet file into a dataframe and storing it in the dictionary
for table in tables:
    df[table] = pd.read_parquet(f"../copa_parquet/{table}.parquet")

# %% Print the shape of the dataframes
# Here I will be prioritizing the tables with the most number of rows
print("-" * 40, "\nDataframes shape: ")
for d in df:
    print(d, df[d].shape)

# %% After looking at the shapes of the dataframes, I will be joining the tables with the most number of rows
# The tables with the most number of rows are: INV_LOC, SD_FAULT, EVT_EVENT
df1 = df["INV_LOC"]
df2 = df["SD_FAULT"]
df3 = df["EVT_EVENT"]

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

# %%
"""
Conclusion:
For the tables to be merged, we need values which are common in all the tables.
For example, if we select ALT_ID as the common column, we need the values of ALT_ID to be unique in all the rows, but these
values should be common in all the tables. So that we can make one row from the three tables.

In the above code, we have checked the number of unique values in the column and the number of values for a column which are 
same in the other tables. The ideal merging column will be the one that has the same number of unique values in all the tables
and all the values are present in all the tables.

After looking at the results, we can see that 'RSTAT_CD', 'REVISION_DT', 'CREATION_DT', 'ALT_ID' columns are shared between all the three tables.
We cannot use RSTAT_CD as the merging column because it does not have unique values.
REVISION_DT and CREATION_DT are also not ideal because they are timestamps and they are not unique. (There can be multiple rows with the same date)

ALT_ID has unique values but the values are not common in all the tables. 

"""
