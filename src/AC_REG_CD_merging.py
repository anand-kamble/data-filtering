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
print("Number of unique aircrafts: ", len(aircrafts))
df_by_aircraft = dict()

for aircraft in aircrafts:
    df_by_aircraft[aircraft] = merged_df[merged_df["AC_REG_CD"] == aircrafts[0]]
    # While doing this we will also update the index of the dataframe to be the 'CREATION_DT' column
    df_by_aircraft[aircraft].set_index("FAULT_FOUND_DATE", inplace=False)

# %%
# We also have the FLEET available in the dataframe, we can use this to focus on a specific aircraft model.
print("-" * 20, "Available Fleet values", "-" * 20)
print(merged_df["FLEET"].value_counts())

# %%
# We will focus on the aircrafts of fleet 737-NG which is the most occuring aircraft fleet in the dataset
top_fleet_df = merged_df[merged_df["FLEET"] == "737-NG"]
top_fleet_df = top_fleet_df[
    top_fleet_df["FAULT_FOUND_DATE"] > pd.Timestamp("2019-01-01")
]
top_fleet_df = top_fleet_df[
    top_fleet_df["FAULT_FOUND_DATE"] < pd.Timestamp("2019-02-01")
]

plt.figure(figsize=(20, 10))
for ata in top_5_ATA:
    occurrences = (
        top_fleet_df[top_fleet_df["ATAg"] == ata]
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
df_by_aircraft["HP-1373CMP"][df_by_aircraft["HP-1373CMP"]["ATAg"] == "23"]
# %%

plt.figure(figsize=(20, 10))
for ata in top_5_ATA:
    df_to_plot = df_by_aircraft["HP-1373CMP"][
        df_by_aircraft["HP-1373CMP"]["ATAg"] == ata
    ]
    df_to_plot = df_to_plot[df_to_plot["FAULT_FOUND_DATE"] > pd.Timestamp("2019-01-01")]
    df_to_plot = df_to_plot[df_to_plot["FAULT_FOUND_DATE"] < pd.Timestamp("2019-02-01")]
    occurrences = df_to_plot.groupby("FAULT_FOUND_DATE").size().sort_index()
    occurrences.plot(kind="line", marker="o", alpha=0.5, label="ATA " + ata)
plt.xlabel("Date")
plt.ylabel("Number of Occurrences")
plt.title("Number of Occurrences of Top ATAs Over Time for Aircraft HP-1373CMP")
plt.legend()
plt.grid(True)
plt.show()

# %%
merged_df["AIRWORTH_CD"].unique()
# %%
merged_df.columns

# %%
# To get informattion about the part used in the maintainance, we can use the EVT_INV table
EVT_INV = pd.read_parquet("../copa_parquet/EVT_INV.parquet")

# %%
# Finding the common columns between the two dataframes
set(EVT_INV.columns).intersection(set(merged_df.columns))
# Common columns are 'CREATION_DT', 'INV_NO_DB_ID', 'INV_NO_ID', 'REVISION_DT', 'RSTAT_CD'

# %%
# We can merge the two dataframes on the common columns
# Test the merge operation on random 100 samples

# CANNOT BE MERGED AS RUNNING OUT OF MEMORY.
# final_merged_df_sample = pd.merge(
#     merged_df, EVT_INV, on="INV_NO_ID", how="inner"
# )
# final_merged_df_sample.to_parquet("final_merged_df_sample.parquet")
# print("Shape of final_merged_df_sample dataframe after merge:", final_merged_df_sample.shape)

# %%
# To save the memory, we will only consider the INV_NO_ID that are common between
# both the dataframes.

# merged_df["INV_NO_ID"].nunique(), EVT_INV["INV_NO_ID"].nunique()

# shared_INV_NO_ID = set(merged_df["INV_NO_ID"].unique()).intersection(
#     set(EVT_INV["INV_NO_ID"].unique())
# )

# merged_df_shared_INV_NO_ID = merged_df[merged_df["INV_NO_ID"].isin(shared_INV_NO_ID)]
# EVT_INV_shared_INV_NO_ID = EVT_INV[EVT_INV["INV_NO_ID"].isin(shared_INV_NO_ID)]

# print("Shape of merged_df_shared_INV_NO_ID:", merged_df_shared_INV_NO_ID.shape)
# print("Shape of EVT_INV_shared_INV_NO_ID:", EVT_INV_shared_INV_NO_ID.shape)
# print(
#     "Compared to original shapes of merged_df and EVT_INV:",
#     merged_df.shape,
#     EVT_INV.shape,
# )
# print(
#     "We have reduced the size of the dataframes by considering only the common INV_NO_IDs"
# )
# print(
#     "EVT_INV has been reduced by a factor of",
#     EVT_INV_shared_INV_NO_ID.shape[0] / EVT_INV.shape[0],
# )

#  Expected output:
# Shape of merged_df_shared_INV_NO_ID: (1146241, 62)
# Shape of EVT_INV_shared_INV_NO_ID: (479508, 24)
# Compared to original shapes of merged_df and EVT_INV: (1205006, 62) (8982850, 24)
# We have reduced the size of the dataframes by considering only the common INV_NO_IDs
# EVT_INV has been reduced by a factor of 0.05338038595768604

# %%
# final_merged_df_sample = pd.merge(
# merged_df_shared_INV_NO_ID, EVT_INV_shared_INV_NO_ID, on="INV_NO_ID", how="inner"
# )

# STILL RUNNING OUT OF MEMORY

# %%
import seaborn as sns

merged_df.columns
# %%

sns.heatmap(pd.crosstab(merged_df["ATAg"], merged_df["AC_REG_CD"]))
plt.show()

# %%
# Dropping ATA values that are not in the top 5
without_top_5 = merged_df[~merged_df["ATAg"].isin(top_5_ATA)]

sns.heatmap(pd.crosstab(without_top_5["ATAg"], without_top_5["AC_REG_CD"]))
plt.show()

# %%
merged_df["month"] = merged_df["FAULT_FOUND_DATE"].dt.month

# %%
# ATA_to_analyse = "09"
aircraft_to_analyse = "HP-1373CMP"

# interested_rows = merged_df[merged_df["ATAg"] == ATA_to_analyse]
interested_rows = merged_df[merged_df["AC_REG_CD"] == aircraft_to_analyse]

interested_rows["month"].plot(
    kind="hist", bins=12, rwidth=0.8, alpha=0.7, color="skyblue"
)
plt.title("Occurrences by Month of Aircraft " + aircraft_to_analyse)
plt.xlabel("Month")
plt.ylabel("Number of Occurrences")
plt.xticks(range(1, 13))
plt.grid(axis="y", alpha=0.75)
plt.show()

# %%
grouped = merged_df.groupby(["AC_REG_CD", "month"]).size().unstack(fill_value=0)

# Plot stacked histogram
grouped.T.plot(kind="bar", stacked=True, alpha=0.7, width=0.8)

# plt.figure(figsize=(20, 10))
plt.title("Occurrences by Month for Multiple Aircraft")
plt.xlabel("Month")
plt.ylabel("Number of Occurrences")
plt.xticks(range(12), [str(i + 1) for i in range(12)], rotation=0)
plt.grid(axis="y", alpha=0.75)
plt.legend(title="Aircraft", loc="upper left")
plt.tight_layout()  # Adjust layout to prevent clipping of ylabel/xticks
plt.show()


# %%
aircraft_counts = merged_df["ATA"].value_counts()

# Get top 5 aircraft
top_5_aircraft = aircraft_counts.nlargest(5).index

# Filter the DataFrame to include only the top 5 aircraft
filtered_df = merged_df[merged_df["ATAg"].isin(top_5_aircraft)]

# Group by aircraft and month, count occurrences
grouped = filtered_df.groupby(["ATAg", "month"]).size().unstack(fill_value=0)

# Plot stacked histogram
grouped.T.plot(kind="bar", alpha=0.7, width=0.8, figsize=(10, 6))

plt.title("Occurrences by Month for Top 5 Aircraft")
plt.xlabel("Month")
plt.ylabel("Number of Occurrences")
plt.xticks(range(12), [str(i + 1) for i in range(12)], rotation=0)
plt.grid(axis="y", alpha=0.75)
plt.legend(title="Aircraft", bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()  # Adjust layout to prevent clipping of ylabel/xticks
plt.show()

# %%
# Distribution of maintenance delay times
plt.figure(figsize=(10, 6))
sns.histplot(merged_df["MAINT_DELAY_TIME_QT"], bins=30, kde=True)
plt.title("Distribution of Maintenance Delay Times")
plt.show()

from sklearn.linear_model import LinearRegression

# %%
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
