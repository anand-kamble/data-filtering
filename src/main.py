import json
import pandas as pd
from copa_modules import data_processor, _types


BASE_PATH = "copa/"

config: _types.data_config | None = None
with open('data_config.json') as f:
    config = json.load(f)

# Create a DataProcessor object with the configuration and base path
myDataProcessor = data_processor(config, base_path=BASE_PATH)

# Load the files specified in the configuration
myDataProcessor.load_files(fast_load=True)

# Filter the data according to the configuration
FilteredData = myDataProcessor.filter_data(drop_duplicates=True)

myDataProcessor.save_filtered_data("filtered_data.parquet","parquet")

print(FilteredData.columns)
