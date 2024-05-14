"""
File Name: trials.py
Date Created: 14th May 2024
Description: This file is used for conducting simple trials of the code. It serves as an interactive notebook for 
testing and debugging. It contains various functions and methods to test the functionality of the main codebase.
"""

# %%
import pandas as pd

# %%
file_name = "copa/SD_FAULT.csv"
# %%
sd_fault = pd.read_csv(
    file_name,
    sep=",",
    encoding="latin1",
    low_memory=False,
    on_bad_lines="warn",
)

# %%


# %%
sd_fault["FLIGHT_STAGE_DB_ID"].unique()

# %%
sd_fault["FLIGHT_STAGE_DB_ID"].value_counts(dropna=False)

# %%
sd_fault["FLIGHT_STAGE_CD"].value_counts(dropna=False)
# %%
print(sd_fault["CREATION_DT"].value_counts(dropna=False).to_string())

# %%
evt_event = pd.read_csv(
    "copa/EVT_EVENT.csv",
    sep=",",
    encoding="latin1",
    low_memory=False,
    on_bad_lines="warn",
    nrows=1000,
)

# %%
evt_event.head()
