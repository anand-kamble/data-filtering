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
    base_path=BASE_PATH,
    test_mode=True,
    test_rows=25000,
    drop_duplicates=True,
    no_cache=True,
)

my_filtered_data = my_data_processor.load()

print(f"Shape of filtered data: {my_filtered_data.shape}")
