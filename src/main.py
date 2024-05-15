import json
import os
from enum import unique

import pandas as pd

from copa_modules import _types, data_processor
from copa_modules.utils import extract_ATA

BASE_PATH = "TABLES_ADD_20240515/"

config: _types.data_config | None = None
# Path of the config file relative to the run.sh file.
with open("filter_configs/ata_filter.json") as f:
    config = json.load(f)
# Create a DataProcessor object with the configuration and base path
my_data_processor = data_processor(
    config,
    base_path=BASE_PATH,  # Path of the Directory where the dataset is located relative to run.sh.
    test_mode=False,  # Test mode which will only load a subset of the data
    test_rows=33000,  # Number of rows to load in test mode
    drop_duplicates=False,  # Drop duplicates from the dataset (Currently set true for Copa dataset)
    no_cache=False,  # Do not use cached data, i.e., data from the copa_output folder.
)

# Below Load function is returning the filtered dataframe.
my_filtered_data = my_data_processor.load()
print(f"Shape of filtered data: {my_filtered_data.shape}")

# Creating a list to store the extracted ATA codes and event descriptions
ata_table_data = []
ata_not_found = []

# Extracting ATA codes from the event descriptions
for i in range(my_filtered_data["EVENT_SDESC"].shape[0]):
    extracted = extract_ATA(my_filtered_data["EVENT_SDESC"][i])
    if type(extracted) == tuple:
        ATA, event_description = extracted
        ata_table_data.append(
            [ATA, event_description, my_filtered_data["EVENT_SDESC"][i]]
        )
    else:
        ata_not_found.append(my_filtered_data["EVENT_SDESC"][i])


# Here we are creating a dataframe from ata_table_data and ata_not_found
ata_table = pd.DataFrame(
    data=ata_table_data, columns=["ATA", "EVENT_DESCRIPTION", "original_description"]
)
ata_not_found_df = pd.DataFrame(data=ata_not_found, columns=["original_description"])


print(f"Shape of ata_table: {ata_table.shape}")
print(f"Shape of ata_not_found_df: {ata_not_found_df.shape}")

# Unique ATA codes found
unique_ata = ata_table["ATA"].unique()
print(f"Unique ATA codes found: \n{unique_ata}")

# Creating a directory to store the filtered ata tables
if not os.path.exists("ata_filtered"):
    os.makedirs("ata_filtered")

# Saving the ata tables to csv files
for ata in unique_ata:
    ata_table[ata_table["ATA"] == ata].to_csv(
        f"ata_filtered/ATA-{ata}.csv", index=False
    )

ata_not_found_df.to_csv("ata_filtered/ATA-not-found.csv", index=False)


print(
    f"\nPercentage of ATA codes found: {round(ata_table.shape[0] / my_filtered_data.shape[0] * 100,2)}%\n"
)
