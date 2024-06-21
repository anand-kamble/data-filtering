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
ISDP_LOGBOOK_REPORT["AC_REG_CD"].value_counts()

"""
Expected output:
AC_REG_CD
HP-1532CMP    21209
HP-1536CMP    20785
HP-1523CMP    20414
HP-1537CMP    20410
HP-1711CMP    19168
              ...  
HP-9929CMP      496
HP-9928CMP      478
                161
HK-4601         101
HP-9931CMP        6
Name: count, Length: 134, dtype: int64
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
merged_df.shape, INV_AC_REG.shape, ISDP_LOGBOOK_REPORT.shape

# %%
INV_AC_REG.columns

# %%
INV_AC_REG.AC_REG_CD.value_counts().max()

# %%
merged_df['ATAg'] = merged_df['ATA'].str.split('-').str[0]

# %%
merged_df['ATAg'].value_counts().sort_values(ascending=False)

# %%
dg = merged_df.groupby(['ATAg', 'AC_REG_CD']).size()
dg = merged_df.groupby(['AC_REG_CD', 'ATAg']).size()

# %%
dg[dg.index.get_level_values('ATAg') == '21'].sort_values(ascending=False).head()
# %%
merged_df.ATA.value_counts().sort().head()
# %%
dg.head(50)
# %%
ATA_AC = merged_df.groupby(['ATAg', 'AC_REG_CD'])
AC_ATA = merged_df.groupby(['AC_REG_CD', 'ATAg'])

# %%
ATA_AC[ATA_AC['ATA']== 25]
# %%
ATA_AC.size()
# %%
