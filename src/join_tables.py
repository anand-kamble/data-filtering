# %% Import necessary libraries
import glob
import json
import os
import re
from collections import defaultdict
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

# %% For each file, identify whether they use "," or ";" as the delimiter
delimiters = dict()
for f in files:
    with open(f, "r", encoding="latin-1") as file:
        first_line = file.readline()
        if ";" in first_line:
            delimiters[f] = ";"
        else:
            delimiters[f] = ","

# sve the delimeters dict to the file delimiters.json
with open("delimiters.json", "w") as f:
    json.dump(delimiters, f)

pprint(delimiters)


# %% Create a dictionary to store data from each file
data = dict()
base_to_full = dict()
for f in files:
    print(f"==> Reading file:{f}")
    sep = delimiters[f]
    # Read the first line of each file and extract the column names
    read_df = pd.read_csv(f, nrows=1, on_bad_lines="skip", encoding="latin-1", sep=sep)
    print("  - nb columns: ", len(read_df.columns))
    base_filename = os.path.basename(f)
    base_to_full[base_filename] = f
    data[base_filename] = set(list(read_df.columns))

# %%
comms = dict()
keys = list(data.keys())

for i, key in enumerate(keys):
    for j in range(i + 1, len(keys)):
        comms[(key, keys[j])] = data[key].intersection(data[keys[j]])

for k, v in comms.items():
    if len(v) > 0:
        print(f"key: {k} ==> Common keys: {v}")

# %% Generate the list of joinable columns across databases into a set
# and keep unique elements

cols = set()
for k, v in comms.items():
    # Skip empty sets
    if len(v) == 0:
        continue
    key1, key2 = k
    cols = cols.union(data[key1].union(data[key2]))

print(f"Joinable Columns:\n{cols}")

#### END Gordon Mods PROGRAM ####

# %% ## More Gordon Mods
# Diagnostic checks. For each pair of csv files with common columns, check which columns are formed from unique entries.
# Only unique columns can be used for merging/joining the two dataframes.
# Example: key: ('REQ_PART.csv', 'SD_FAULT.csv') ==> Common keys: {'CREATION_DT', 'REVISION_DT', 'RSTAT_CD', 'ALT_ID'}

# %%

# Read and cache the files
cache = dict()

# key: (file, col), value: nb_non_unique values
unique_keys_dict = defaultdict(list)

# Key is a pair of files
for key, cols in comms.items():
    if len(cols) == 0:
        continue
    # key = ("REQ_PART.csv", "SD_FAULT.csv")
    print("=====================================")
    print("key: ", key)
    print(
        "Read from parquet files (.parquet): ",
        key[0].replace(".csv", ".parquet"),
        key[1].replace(".csv", ".parquet"),
    )
    try:
        if key[0] in cache:
            df1 = cache[key[0]]
            print("cached 0")
        else:
            df1 = pd.read_parquet(
                "../copa_parquet/" + key[0].replace(".csv", ".parquet")
            )
            cache[key[0]] = df1
    except Exception as e:
        print("Error reading file ", base_to_full[key[0]])
        continue

    try:
        if key[1] in cache:
            df2 = cache[key[1]]
            print("cached 1")
        else:
            df2 = pd.read_parquet(
                "../copa_parquet/" + key[1].replace(".csv", ".parquet")
            )
            cache[key[1]] = df2
    except Exception as e:
        print("Error reading file ", base_to_full[key[1]])
        continue

    # Store key=(filenm, cols), value=nb_non_unique values
    for col in cols:
        # Check that key[0] is a key of unique_keys_dict and whether col is in the list of unique columns for that key.
        if0: bool = (key[0], col) in unique_keys_dict
        if1: bool = (key[1], col) in unique_keys_dict

        if if0:
            non_non_unique_0 = unique_keys_dict[(key[0], col)]
        else:
            nb_non_unique_0 = df1[col].shape[0] - df1[col].nunique()
            ukey = (key[0], col)
            unique_keys_dict[ukey].append((nb_non_unique_0))

        if if1:
            non_non_unique_1 = unique_keys_dict[(key[1], col)]
        else:
            nb_non_unique_1 = df2[col].shape[0] - df1[col].nunique()
            ukey = (key[1], col)
            unique_keys_dict[ukey].append((nb_non_unique_1))

        if nb_non_unique_0 == 0 and nb_non_unique_1 == 0:
            print("- Unique col: ", col)
            unique_keys_dict[key[0]].append((col, nb_non_unique_0))
            unique_keys_dict[key[1]].append((col, nb_non_unique_1))

print("=====================================")
print("unique_keys_dict: ")
pprint(unique_keys_dict)

# Wrap tuple arguments of unique_keys_dict.keys() into a str
unique_keys_dict = {str(k): v for k, v in unique_keys_dict.items()}

# save unique_keys_dict to json:
with open("unique_keys_dict.json", "w", encoding="latin-1") as f:
    json.dump(unique_keys_dict, f)

# print(f"Column {col} has {nb_non_unique_1} non-unique values in {key[0]}")
# print(f"Column {col} has {nb_non_unique_2} non-unique values in {key[1]}")

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
