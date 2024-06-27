#%%

# !pip install pandasql <-- Uncomment this line to install the package

# I was not able to add this package using poetry.
# Below is the error message I got when I tried to add it using poetry.
# failed to query /usr/bin/python3 with code 1 err: '  File "/usr/local/poetry/venv/lib64/python3.11/site-packages/virtualenv/discovery/py_info.py", line 7\n    from __future__ import annotations\n    ^\nSyntaxError: future feature annotations is not defined\n'
# %%
import pandas as pd
from pandasql import sqldf


# %%
tables = [
    "INV_LOC",
    "REF_FAIL_CATGRY",
    "INV_LOC",
    "REF_FLIGHT_STAGE",
    "REF_FAIL_SEV",
    "REF_FAIL_PRIORITY",
    "REF_FAULT_SOURCE",
    "REF_FLIGHT_STAGE",
    "SD_FAULT",
    "REF_FAULT_LOG_TYPE",
    "EVT_EVENT",
    "INV_AC_REG",
    "ISDP LOGBOOK REPORT",
]


#%%
BASE_PATH = "./TABLES_ADD_20240515/"
OLDER_BASE_PATH = "./copa/"

EQP_ASSMBL_BOM = pd.read_csv(BASE_PATH + "EQP_ASSMBL_BOM.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
EQP_ASSMBL = pd.read_csv(BASE_PATH + "EQP_ASSMBL.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
EVT_EVENT_REL = pd.read_csv(BASE_PATH + "EVT_EVENT_REL.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
EVT_INV = pd.read_csv(BASE_PATH + "EVT_INV.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
EVT_LOC = pd.read_csv(BASE_PATH + "EVT_LOC.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
FL_LEG_DISRUPT = pd.read_csv(BASE_PATH + "FL_LEG_DISRUPT.csv",nrows=100,encoding="latin1",on_bad_lines="warn",sep=";")
INV_AC_REG = pd.read_csv(BASE_PATH + "INV_AC_REG.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
SCHED_ACTION = pd.read_csv(BASE_PATH + "SCHED_ACTION.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
ISDP_LOGBOOK_REPORT = pd.read_csv(BASE_PATH + "ISDP LOGBOOK REPORT.csv",nrows=100,encoding="latin1",on_bad_lines="warn")

# Tables required from older data
SD_FAULT = pd.read_csv(OLDER_BASE_PATH + "SD_FAULT.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
EVT_EVENT = pd.read_csv(OLDER_BASE_PATH + "EVT_EVENT.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
SCHED_STASK = pd.read_csv(OLDER_BASE_PATH + "SCHED_STASK.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
REF_EVENT_STATUS = pd.read_csv(OLDER_BASE_PATH + "REF_EVENT_STATUS.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
FL_LEG = pd.read_csv(OLDER_BASE_PATH + "FL_LEG.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
INV_LOC = pd.read_csv(OLDER_BASE_PATH + "INV_LOC.csv",nrows=100,encoding="latin1",on_bad_lines="warn",sep=";")
REF_FAIL_CATGRY = pd.read_csv(OLDER_BASE_PATH + "REF_FAIL_CATGRY.csv",nrows=100,encoding="latin1",on_bad_lines="warn")


# %%
sql_query = None
with open("./src/pandasql_joining/ISDP LOGBOOK REPORT_query.sql", 'r') as file:
    sql_query = file.read()


# %%
# %%
pysqldf = lambda q: sqldf(q, globals())
merged = pysqldf(sql_query)

print("="*50)
print("Merged Data:")

print(merged.head())