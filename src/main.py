import json
import pandas as pd
from copa_modules import DataProcessor, _types


BASE_PATH = "copa/"

config: _types.DataConfig | None = None
with open('data_config.json') as f:
    config = json.load(f)

# Create a DataProcessor object with the configuration and base path
myDataProcessor = DataProcessor(config, base_path=BASE_PATH)

# Load the files specified in the configuration
myDataProcessor.loadFiles()

# Filter the data according to the configuration
FilteredData = myDataProcessor.filterData()

print(FilteredData.columns)