# # %%
# import matplotlib.pyplot as plt
# import pandas as pd
# import plotly.express as px

# # %%
# # List of table names that we will be reading into dataframes
# tables = [
#     "INV_LOC",
#     "REF_FAIL_CATGRY",
#     "REF_FAIL_CATGRY",
#     "INV_LOC",
#     "REF_FLIGHT_STAGE",
#     "REF_FAIL_SEV",
#     "REF_FAIL_PRIORITY",
#     "REF_FAULT_SOURCE",
#     "REF_FLIGHT_STAGE",
#     "REF_FAIL_CATGRY",
#     "SD_FAULT",
#     "REF_FAULT_LOG_TYPE",
#     "EVT_EVENT",
#     "INV_AC_REG",
#     "ISDP LOGBOOK REPORT",
# ]

# # Dictionary to store the dataframes
# df: dict[str, pd.DataFrame] = dict()

# # Reading each table's Parquet file into a dataframe and storing it in the dictionary
# for table in tables:
#     df[table] = pd.read_parquet(f"../copa_parquet/{table}.parquet")


# # %%
# INV_AC_REG = df["INV_AC_REG"]
# ISDP_LOGBOOK_REPORT = df["ISDP LOGBOOK REPORT"]

# # %%
# print("Shape of INV_AC_REG:", INV_AC_REG.shape)
# print("Shape of ISDP_LOGBOOK_REPORT:", ISDP_LOGBOOK_REPORT.shape)

# """Expected output:
# Shape of INV_AC_REG: (135, 28)
# Shape of ISDP_LOGBOOK_REPORT: (1205167, 28)
# """

# # %%
# INV_AC_REG["AC_REG_CD"].value_counts()

# # %%
# """
# Expected output:
# AC_REG_CD
# HP-9928CMP    1
# HP-1372CMP    1
# HP-1371CMP    1
# HP-1373CMP    1
# HP-1377CMP    1
#              ..
# HP-1522WWP    1
# HP-1523CMP    1
# HP-1534CMP    1
# HP-1538CMP    1
# HP-1726CMP    1
# Name: count, Length: 135, dtype: int64
# """


# # %%
# # Merge the two dataframes
# merged_df = pd.merge(INV_AC_REG, ISDP_LOGBOOK_REPORT, on="AC_REG_CD", how="inner")
# print("Shape of merged dataframe:", merged_df.shape)
# """
# Expected output:

# Shape of merged dataframe: (1205006, 55)
# """

# # %%
# merged_df.shape, INV_AC_REG.shape, ISDP_LOGBOOK_REPORT.shape

# # %%
# INV_AC_REG.columns

# # %%
# INV_AC_REG.AC_REG_CD.value_counts().max()

# # %%
# merged_df["ATAg"] = merged_df["ATA"].str.split("-").str[0]

# # %%
# merged_df["ATAg"].value_counts().sort_values(ascending=False)

# # # %%
# # dg = merged_df.groupby(["ATAg", "AC_REG_CD"]).size()
# # dg = merged_df.groupby(["AC_REG_CD", "ATAg"]).size()

# # # %%
# # dg[dg.index.get_level_values("ATAg") == "21"].sort_values(ascending=False).head()
# # # %%
# # # merged_df.ATA.value_counts().sort().head()
# # # %%
# # dg.head(50)
# # # %%
# # ATA_AC = merged_df.groupby(["ATAg", "AC_REG_CD"])
# # AC_ATA = merged_df.groupby(["AC_REG_CD", "ATAg"])

# # # %%
# # # ATA_AC[ATA_AC["ATA"] == 25]
# # # %%
# # ATA_AC.size()
# # # %%
# # pivot_table = merged_df.pivot_table(index="ATAg", columns="AC_REG_CD", aggfunc="count")

# # fig = px.imshow(pivot_table,aspect="auto",title="Heatmap of ATA vs FAULT_SEVERITY",width=1920,height=1080)
# # fig.write_image("ATA_vs_AC_REG_CD.pdf",format="pdf")

# # # %%

# # %%
# merged_df["ATAg"].value_counts().sort_values(ascending=False)


# %% ==================================================================
import matplotlib.pyplot as plt
import numpy as np
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

# """Expected output:
# Shape of INV_AC_REG: (135, 28)
# Shape of ISDP_LOGBOOK_REPORT: (1205167, 28)
# """


# %%
# Merge the two dataframes
merged_df = pd.merge(INV_AC_REG, ISDP_LOGBOOK_REPORT, on="AC_REG_CD", how="inner")
print("Shape of merged dataframe:", merged_df.shape)
# """
# Expected output:
# Shape of merged dataframe: (1205006, 55)
# """

# We can see that the merged dataframe has 1205006 rows and 55 columns.
# The number of columns is correctly calculated as 28 (INV_AC_REG) + 28 (ISDP_LOGBOOK_REPORT) - 1 (AC_REG_CD) = 55.
# Which means the merge operation was successful.


# %%
# Here we are grouping the ATA values by the Primary ATA
# Some ATA strings include the sub catorgories of the ATA, we are only interested in the primary ATA at this point.
merged_df["ATAg"] = merged_df["ATA"].str.split("-").str[0]

# %%
# We are counting the number of occurances of each ATA value
merged_df["ATAg"].value_counts().sort_values(ascending=False)

# %%
# Get which column include dates, the headers of these column generally end with 'DT'
# So we are filtering the columns that end with 'DT'
datetime_columns = list(
    filter(
        lambda c: c.endswith("DT") or c.endswith("DATE"), merged_df.columns.to_list()
    )
)
print("Columns that include datetime values: ", datetime_columns)
# %%
# Convert the datetime columns to unix timestamp
# The new columns will have the same name with a '_U' suffix
# We are also updating the original column with the Pandas datetime object
# Which will be useful for plotting and other operations
for col in datetime_columns:
    print("Converting column: ", col, " to unix timestamp")
    merged_df[col] = pd.to_datetime(merged_df[col], format="%d-%b-%y")
    merged_df[col + "_U"] = (
        pd.to_datetime(merged_df[col], format="%d-%b-%y").astype(int) / 10**9
    )

# %%
plt.figure(figsize=(20, 10))
plt.xticks(rotation=45)
merged_df["ATAg"].hist()

# ATA 23, 25, 11 ,05 ,29 are the most common ATA values
# %%
value_counts = merged_df["ATAg"].value_counts()
top_5_values = value_counts.head(5)
top_5_ATA = top_5_values.index.to_list()
print("Top 5 ATA values: ", top_5_ATA)
total_count = merged_df["ATAg"].count()
top_5_percentages = (top_5_values / total_count) * 100
print("-" * 20, "Top 5 ATA values and their percentages", "-" * 20)
print(top_5_percentages)


# %%
aircrafts = merged_df["AC_REG_CD"].unique()

df_by_aircraft = dict()

for aircraft in aircrafts:
    df_by_aircraft[aircraft] = merged_df[merged_df["AC_REG_CD"] == aircrafts[0]]
    # While doing this we will also update the index of the dataframe to be the 'CREATION_DT' column
    df_by_aircraft[aircraft].set_index("FAULT_FOUND_DATE", inplace=True)

# %%
# We also have the FLEET available in the dataframe, we can use this to focus on a specific aircraft model.
print("-" * 20, "Available Fleet values", "-" * 20)
print(merged_df["FLEET"].value_counts())
# %%
df_by_aircraft["HP-1373CMP"][df_by_aircraft["HP-1373CMP"]["ATAg"] == "23"]
# %%

plt.figure(figsize=(20, 10))
for ata in top_5_ATA:
    occurrences = (
        df_by_aircraft["HP-1373CMP"][df_by_aircraft["HP-1373CMP"]["ATAg"] == ata]
        .groupby("FAULT_FOUND_DATE")
        .size()
        .sort_index()
    )
    occurrences.plot(kind="line", marker="o", alpha=0.5, label="ATA " + ata)
plt.xlabel("Date")
plt.ylabel("Number of Occurrences")
plt.title("Number of Occurrences of Top ATAs Over Time for Aircraft HP-1373CMP")
plt.legend()
plt.grid(True)
plt.show()

# %%
