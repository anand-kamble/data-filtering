# %%
import pandas as pd

# %%
DIR_PATH = "../copa/"

dataConfig = {
    "SD_FAULT_REFERENCE.csv": ["NOTES", "CREATION_DT"],
    'FAIL_DEFER_REF.csv': ['PERF_PENALTIES_LDESC'],
    'EVT_EVENT.csv': ['EVENT_LDESC'],
    'INV_LOC.csv': ['--'],
    "REQ_PART.csv": ["REQ_QT", "REQ_BY_DT", "EST_ARRIVAL_DT", "REQ_NOTE"],
    'SCHED_STASK.csv': ['TASK_PRIORITY_CD', 'MAIN_INV_NO_ID', 'ROUTINE_BOOL', 'INSTRUCTION_LDESC', 'EST_DURATION_QT'],
    'REF_FAIL_SEV.csv': ['FAIL_SEV_ORD'],
    'FL_LEG.csv': ['--']
}

# %%
dataset = {}
"""
Here I was trying to make a loop to load all the files,
but some of the CSV files are using ";" (semicolon) as a seperator rather than "," (comma)
So the loop didn't work, I tried using regex but it was still giving errors.
"""

# for k in dataConfig.keys():
#     tempDf = pd.read_csv(
#         DIR_PATH + k,
#         sep="\r[,;]",
#         encoding="latin1",
#         on_bad_lines="warn",
#     )
#     print(tempDf.columns.shape)

dataset["SD_FAULT_REFERENCE.csv"] = pd.read_csv(DIR_PATH + "SD_FAULT_REFERENCE.csv",sep=";",encoding="latin1",on_bad_lines="warn",low_memory=False)
dataset["FAIL_DEFER_REF.csv"] = pd.read_csv(DIR_PATH + "FAIL_DEFER_REF.csv",sep=";",encoding="latin1",on_bad_lines="warn",low_memory=False)
dataset["EVT_EVENT.csv"] = pd.read_csv(DIR_PATH + "EVT_EVENT.csv",sep=",",encoding="latin1",on_bad_lines="warn",low_memory=False)
dataset["INV_LOC.csv"] = pd.read_csv(DIR_PATH + "INV_LOC.csv",sep=";",encoding="latin1",on_bad_lines="warn",low_memory=False)
dataset["REQ_PART.csv"] = pd.read_csv(DIR_PATH + "REQ_PART.csv", sep=",", encoding="latin1", on_bad_lines="warn",low_memory=False)
dataset["SCHED_STASK.csv"] = pd.read_csv(DIR_PATH + "SCHED_STASK.csv",sep=",",encoding="latin1",on_bad_lines="warn",low_memory=False)
dataset["REF_FAIL_SEV.csv"] = pd.read_csv(DIR_PATH + "REF_FAIL_SEV.csv",sep=";",encoding="latin1",on_bad_lines="warn",low_memory=False)
dataset["FL_LEG.csv"] = pd.read_csv(DIR_PATH + "FL_LEG.csv",sep=";",encoding="latin1",on_bad_lines="warn",low_memory=False)



# %%
filteredData = pd.DataFrame()
for k in dataConfig.keys():
    print(f"Processing file: {k}")
    if dataConfig[k] != ["--"]:
        filteredData = pd.concat([filteredData, dataset[k][dataConfig[k]]], axis=1)
    elif dataConfig[k] == ["--"]: # If no Variable is mentioned load all the columns from that table.
        filteredData = pd.concat([filteredData, dataset[k][:]], axis=1)
# %%
filteredData
# %%
