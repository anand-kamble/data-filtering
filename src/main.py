import json
import pandas as pd
from copa_modules import DataProcessor, _types


BASE_PATH = "copa/"

config: _types.DataConfig | None = None
with open('data_config.json') as f:
    config = json.load(f)

myDataProcessor = DataProcessor(config,base_path=BASE_PATH)

myDataProcessor.loadFiles()