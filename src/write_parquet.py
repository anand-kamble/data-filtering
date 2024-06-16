# Convert all csv files to parquet format

# %%
import glob
import os

import pandas as pd

# %% Get all files in the specified directory
files_orig = glob.glob("../copa/*.csv")
files = files_orig + glob.glob("../TABLES_ADD_20240515/*.csv")
print(f"Found {len(files)} files.")

# %% For each file, identify whether they use "," or ";" as the delimiter
delimiters = dict()
for f in files:
    with open(f, "r", encoding="latin-1") as file:
        first_line = file.readline()
        if ";" in first_line:
            delimiters[f] = ";"
        else:
            delimiters[f] = ","

#  %% Convert all files to Parquet format
# Create destination folder for Parquet files
parquet_root = "../copa_parquet"
if not os.path.exists(parquet_root):
    os.makedirs(parquet_root)

for f in files:
    print("==> f: ", f)
    df = pd.read_csv(f, encoding="latin-1", sep=delimiters[f], low_memory=False)
    print("  - read df")
    out_file = os.path.join(
        parquet_root, os.path.basename(f).replace(".csv", ".parquet")
    )
    print("  - out_file: ", out_file)
    df.to_parquet(out_file, index=False)
    print("  - saved to parquet file.")

# %%
