import json
import sys

import pandas as pd

# Adding the required paths to the sys.path
sys.path.append("src")
sys.path.append("tests")

from test import Test

from copa_modules import _types, data_processor

config: _types.data_config | None = None
with open("filter_configs/var_of_interest.json") as f:
    config = json.load(f)

# Create a DataProcessor object with the configuration and base path
my_data_processor = data_processor(
    config,
    test_mode=True,
    test_rows=33000,
    drop_duplicates=True,
    no_cache=False,
)

####### Testing the DataProcessor object #######

# Create a Test object with the name of the module to test
dp_test = Test("DataProcessor", mode="soft")

# Test the DataProcessor object has the necessary attributes
dp_test.expect(my_data_processor).has_type(data_processor).has_attribute(
    [
        "config",
        "base_path",
        "test_mode",
        "drop_dupiiblicates",
        "no_cache",
        "load",
    ]  # Intentional Typo in one of the attribute name
)

# Testing the test_mode attribute is set to True
dp_test.expect(my_data_processor.test_mode).to_be(True)

# Loading the data
my_data = my_data_processor.load()

# Testing the data is a pandas DataFrame with custom message
dp_test.expect(my_data).has_type(pd.DataFrame, "Expected to be a pandas DataFrame")

# Testing the shape of the data is tuple
dp_test.expect(my_data.shape).has_type(tuple)

# Testing the number of rows loaded is 33000
dp_test.expect(my_data.shape[0]).to_be(33000)

dp_test.export_results()
