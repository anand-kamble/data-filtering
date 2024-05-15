import json
import os
from enum import unique

import pandas as pd

from copa_modules import _types, data_processor
from copa_modules.utils import extract_ATA

BASE_PATH = "copa/"
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
    drop_duplicates=True,  # Drop duplicates from the dataset (Currently set true for Copa dataset)
    no_cache=False,  # Do not use cached data, i.e., data from the copa_output folder.
)

# Below Load function is returning the filtered dataframe.
my_filtered_data = my_data_processor.load()
print(f"Shape of filtered data: {my_filtered_data.shape}")


ata_table_data = []

for desc in my_filtered_data["EVENT_SDESC"]:
    extracted = extract_ATA(desc)
    if type(extracted) == tuple:
        ATA, event_description = extracted
        ata_table_data.append([ATA, event_description])


ata_table = pd.DataFrame(data=ata_table_data, columns=["ATA", "EVENT_DESCRIPTION"])
my_filtered_data = pd.concat([my_filtered_data, ata_table], axis=1)

del ata_table_data
del ata_table

if not os.path.exists("ata_filtered"):
    os.makedirs("ata_filtered")


unique_ata = my_filtered_data["ATA"].unique()

print(f"Unique ATA codes: {unique_ata}")


for ata in unique_ata:
    my_filtered_data[my_filtered_data["ATA"] == ata].to_csv(
        f"ata_filtered/ATA-{ata}.csv", index=False
    )
