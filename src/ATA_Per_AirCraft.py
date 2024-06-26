# %%
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# %%
df = pd.read_parquet("./copa_parquet/ISDP LOGBOOK REPORT.parquet")

# Print information about the dataframe
print("Loaded dataframe shape:", df.shape)
print("Columns in dataframe:", df.columns)
# %%
Aircrafts = df["AC_REG_CD"].value_counts()

# Print the top 5 aircrafts
print("\nTop 5 Aircrafts:")
print(Aircrafts.head(5))

# %% Top 5 Aircrafts
top_5_aircrafts = Aircrafts.head(5)

# %% We know that the ATA 00 is not common in the aircrafts,
# so we will focus on it to get observe the outliers.

# Focus on only one aircraft
df_of_aircraft = df[df["AC_REG_CD"] == top_5_aircrafts.index[0]]

# Print information about the filtered aircraft dataframe
print("\nFiltered dataframe for aircraft", top_5_aircrafts.index[0])
print("Shape of filtered dataframe:", df_of_aircraft.shape)

# %% Get the ATA 00 events from the aircraft
df_of_aircraft_filtered_by_ATA = df_of_aircraft[df_of_aircraft["ATA"] == "00"]

# Print information about the filtered ATA 00 events dataframe
print("\nFiltered dataframe for ATA 00 events:")
print(
    "Shape of filtered ATA 00 events dataframe:", df_of_aircraft_filtered_by_ATA.shape
)


# %% Convert the FAULT_FOUND_DATE to datetime
df_of_aircraft_filtered_by_ATA["FAULT_FOUND_DATE"] = pd.to_datetime(
    df_of_aircraft_filtered_by_ATA["FAULT_FOUND_DATE"], format="%d-%b-%y"
)

# Print the head of the dataframe after conversion
print("\nHead of dataframe after datetime conversion:")
print(df_of_aircraft_filtered_by_ATA.head())


# %% Interger timestamp of the date
df_of_aircraft_filtered_by_ATA["FAULT_FOUND_DATE_INT"] = df_of_aircraft_filtered_by_ATA[
    "FAULT_FOUND_DATE"
].astype(int, errors="ignore")

# Print the head of the dataframe after adding integer timestamp
print("\nHead of dataframe after adding integer timestamp:")
print(df_of_aircraft_filtered_by_ATA.head())

# %% Group the entries by week
# Ref: https://stackoverflow.com/a/59246890/22647897
df_by_week: pd.DataFrame = (
    df_of_aircraft_filtered_by_ATA.groupby(
        pd.Grouper(key="FAULT_FOUND_DATE", freq="W")
    )["ATA"]
    .count()
    .to_frame()
)

# Print the grouped dataframe
print("\nGrouped dataframe by week:")
print(df_by_week)


# %% Plot the number of occurences of ATA 00 events over time
plt.figure(figsize=(20, 10), dpi=300)
sns.lineplot(data=df_by_week)
plt.xticks(rotation=90)
plt.title(
    "Number of ATA 00 Events Over Time by Week for Aircraft " + top_5_aircrafts.index[0]
)
plt.show()
plt.waitforbuttonpress()

# %%
