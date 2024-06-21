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
    "ISDP LOGBOOK REPORT",
]

# Dictionary to store the dataframes
df: dict[str, pd.DataFrame] = dict()

# Reading each table's Parquet file into a dataframe and storing it in the dictionary
for table in tables:
    df[table] = pd.read_parquet(f"../copa_parquet/{table}.parquet")


# %%
INV_AC_REG = df["INV_AC_REG"]
ISDP_LOGBOOK_REPORT = df["ISDP LOGBOOK REPORT"]

# %%
print("Shape of INV_AC_REG:", INV_AC_REG.shape)
print("Shape of ISDP_LOGBOOK_REPORT:", ISDP_LOGBOOK_REPORT.shape)

"""Expected output:
Shape of INV_AC_REG: (135, 28)
Shape of ISDP_LOGBOOK_REPORT: (1205167, 28)
"""

# %%
INV_AC_REG["AC_REG_CD"].value_counts()

# %%
"""
Expected output:
AC_REG_CD
HP-9928CMP    1
HP-1372CMP    1
HP-1371CMP    1
HP-1373CMP    1
HP-1377CMP    1
             ..
HP-1522WWP    1
HP-1523CMP    1
HP-1534CMP    1
HP-1538CMP    1
HP-1726CMP    1
Name: count, Length: 135, dtype: int64
"""


# %%
# Merge the two dataframes
merged_df = pd.merge(INV_AC_REG, ISDP_LOGBOOK_REPORT, on="AC_REG_CD", how="inner")
print("Shape of merged dataframe:", merged_df.shape)
"""
Expected output:

Shape of merged dataframe: (1205006, 55)
"""

# %%
# Save the merged dataframe to a CSV file
merged_df.to_csv("INV_AC_REG_ISDP_LOGBOOK_MERGED.csv", index=False)
