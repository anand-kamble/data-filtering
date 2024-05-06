#%%
import pandas as pd
import os
from enum import Enum
from glob import glob
from Data import Data

#%%
DIR_PATH = "copa/"

data = {
    'SD_FAULT_REFERENCE.csv': ['NOTES', 'CREATION_DT'],
    'FAIL_DEFER_REF.csv': ['PERF_PENALTIES_LDESC'],
    'EVT_EVENT.csv': ['EVENT_LDESC'],
    'INV_LOC.csv': ['--'],
    'REQ_PART.csv': ['REQ_QT', 'REQ_BY_DT', 'EST_ARRIVAL_DT', 'REQ_NOTE'],
    'SCHED_STASK.csv': ['TASK_PRIORITY_CD', 'MAIN_INV_NO_ID', 'ROUTINE_BOOL', 'INSTRUCTION_LDESC', 'EST_DURATION_QT'],
    'REF_FAIL_SEV.csv': ['FAIL_SEV_ORD'],
    'FL_LEG.csv': ['--']
}

#%%

Dataset = Data(data, base_path=DIR_PATH)
# %%
available_files = glob(f"{DIR_PATH}*.csv")

#%%
FilteredDataset = Dataset.readCsv().filterData()

print(FilteredDataset.columns.tolist())