# %% Import necessary libraries
import glob
import json
import os
from functools import reduce
from pprint import pprint

import pandas as pd

# Set configuration flags
print_common_cols_flag = True
verbose_flag = True

# %% Set pandas display option to show all columns
pd.set_option("display.max_columns", None)

# %% Get all files in the specified directory
files_orig = glob.glob("../copa/*.csv")
files = files_orig + glob.glob("../TABLES_ADD_20240515/*.csv")
print(f"Found {len(files)} files.")

# %% Create a dictionary to store data from each file
data = dict()
for f in files:
    print(f"Reading file:{f}")
    # Read the first 20 lines of each file, skipping bad lines, and store in the dictionary
    read_df = pd.read_csv(f, nrows=1, on_bad_lines="skip", encoding="latin-1")
    base_filename = os.path.basename(f)
    data[base_filename] = set(list(read_df.columns))

# pprint(data)

comms = dict()
keys = list(data.keys())

for i, key in enumerate(keys):
    for j in range(i + 1, len(keys)):
        comms[(key, keys[j])] = data[key].intersection(data[keys[j]])

for k, v in comms.items():
    if len(v) > 0:
        print(f"key: {k} ==> Common keys: {v}")
# %% Create a dictionary to store common columns between each pair of files
comms = dict()
for f in files:
    comms[f] = dict()
    for ff in files:
        if ff != f:
            # Find the intersection of columns between the two dataframes
            comms[f][ff] = reduce(
                set.intersection, map(set, [data[f].columns, data[ff].columns])
            )

# %%


if print_common_cols_flag:

    class SetEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, set):
                return list(obj)
            if isinstance(obj, dict):
                return "CustomSomethingRepresentation"
            return json.JSONEncoder.default(self, obj)


print(json.dumps(comms, cls=SetEncoder, sort_keys=True, indent=8))
pprint(comms)

# %% Print the common columns between two specific files
print(
    "Common columns between ISDP LOGBOOK REPORT.csv and SCHED_ACTION.csv are:",
    comms["../TABLES_ADD_20240515/ISDP LOGBOOK REPORT.csv"][
        "../TABLES_ADD_20240515/SCHED_ACTION.csv"
    ],
)


if verbose_flag is True:
    # %% Read the full data from the two specific files
    df1 = pd.read_csv(
        "../TABLES_ADD_20240515/ISDP LOGBOOK REPORT.csv", encoding="latin-1"
    )
    df2 = pd.read_csv("../TABLES_ADD_20240515/SCHED_ACTION.csv", encoding="latin-1")

    # %% Convert the 'ACTION_DT' column in both dataframes to Unix timestamps
    df1["ACTION_DT"] = (
        pd.to_datetime(df1["ACTION_DT"], errors="coerce").astype(int) / 10**9
    )
    df2["ACTION_DT"] = (
        pd.to_datetime(df2["ACTION_DT"], errors="coerce").astype(int) / 10**9
    )

    # %% Join the two dataframes on the 'ACTION_DT' column
    joint_df = df1.join(df2, on="ACTION_DT", lsuffix="_df1", rsuffix="_df2")
    print(joint_df)
    # %%
