# %%
import pandas as pd

# %%
DIR_PATH = "../copa/"

dataConfig = {
    "SD_FAULT_REFERENCE.csv": ["NOTES", "CREATION_DT"],
    # 'FAIL_DEFER_REF.csv': ['PERF_PENALTIES_LDESC'],
    # 'EVT_EVENT.csv': ['EVENT_LDESC'],
    # 'INV_LOC.csv': ['--'],
    "REQ_PART.csv": ["REQ_QT", "REQ_BY_DT", "EST_ARRIVAL_DT", "REQ_NOTE"],
    # 'SCHED_STASK.csv': ['TASK_PRIORITY_CD', 'MAIN_INV_NO_ID', 'ROUTINE_BOOL', 'INSTRUCTION_LDESC', 'EST_DURATION_QT'],
    # 'REF_FAIL_SEV.csv': ['FAIL_SEV_ORD'],
    # 'FL_LEG.csv': ['--']
}

# %%
dataset = {}
# for k in dataConfig.keys():
#     tempDf = pd.read_csv(
#         DIR_PATH + k,
#         sep="\r[,;]",
#         encoding="latin1",
#         on_bad_lines="warn",
#     )
#     print(tempDf.columns.shape)

dataset["SD_FAULT_REFERENCE.csv"] = pd.read_csv(
    DIR_PATH + "SD_FAULT_REFERENCE.csv",
    sep=";",
    encoding="latin1",
    on_bad_lines="warn",
)

dataset["FAIL_DEFER_REF.csv"] = pd.read_csv(
    DIR_PATH + "FAIL_DEFER_REF.csv",
    sep=";",
    encoding="latin1",
    on_bad_lines="warn",
)

dataset["EVT_EVENT.csv"] = pd.read_csv(
    DIR_PATH + "EVT_EVENT.csv",
    sep=",",
    encoding="latin1",
    on_bad_lines="warn",
)

dataset["INV_LOC.csv"] = pd.read_csv(    
    DIR_PATH + "INV_LOC.csv",
    sep=";",
    encoding="latin1",
    on_bad_lines="warn",
)

dataset["REQ_PART.csv"] = pd.read_csv(
    DIR_PATH + "REQ_PART.csv",
    sep=";",
    encoding="latin1",
    on_bad_lines="warn",
)

dataset["SCHED_STASK.csv"] = pd.read_csv(
    DIR_PATH + "SCHED_STASK.csv",
    sep=";",
    encoding="latin1",
    on_bad_lines="warn",
)

dataset["REF_FAIL_SEV.csv"] = pd.read_csv(
    DIR_PATH + "REF_FAIL_SEV.csv",
    sep=";",
    encoding="latin1",  
    on_bad_lines="warn",
)

dataset["FL_LEG.csv"] = pd.read_csv(
    DIR_PATH + "FL_LEG.csv",
    sep=";",
    encoding="latin1",
    on_bad_lines="warn",
)


print(dataset["SD_FAULT_REFERENCE.csv"].columns)


#%%
filteredData = pd.DataFrame()
for k in dataConfig.keys():
    print(dataConfig[k])
    print(dataset[k].columns)
    if dataConfig[k] != ["--"]:
            filteredData = pd.concat(
                [filteredData, dataset[k][dataConfig[k]]], axis=1
            )
# %%
