import json

import pandas as pd

from copa_modules import _types, data_processor

BASE_PATH = "copa/"

config: _types.data_config | None = None
with open("data_config.json") as f:
    config = json.load(f)

# Create a DataProcessor object with the configuration and base path
my_data_processor = data_processor(
    config,
    base_path=BASE_PATH,  # Path of the Directory where the dataset is located.
    test_mode=True,  # Test mode which will only load a subset of the data
    test_rows=33000,  # Number of rows to load in test mode
    drop_duplicates=True,  # Drop duplicates from the dataset (Currently set true for Copa dataset)
    no_cache=False,  # Do not use cached data, i.e., data from the copa_output folder.
)

# Below Load function is returning the filtered dataframe.
my_filtered_data = my_data_processor.load()

print(f"Shape of filtered data: {my_filtered_data.shape}")
