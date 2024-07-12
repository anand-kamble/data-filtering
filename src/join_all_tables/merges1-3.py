# %%

# Save on memory

from pprint import pprint
import pandas as pd
import utils as u

BASE = "../../copa_parquet/"
BASE = "parquet/"

# Load all the tables from parquet files
sd_fault = pd.read_parquet(BASE + "sd_fault.parquet")
evt_event = pd.read_parquet(BASE + "evt_event.parquet")
sched_stask = pd.read_parquet(BASE + "sched_stask.parquet")

# Remove some columns
# sd_fault = sd_fault[["FAULT_DB_ID", "FAULT_ID"]]
# evt_event = evt_event[["EVENT_DB_ID", "EVENT_ID"]]
# sched_stask = sched_stask[["FAULT_DB_ID", "FAULT_ID", "SCHED_DB_ID", "SCHED_ID"]]

# evt_event['EVENT_ID'] = evt_event['EVENT_ID'].astype(int)
evt_event = evt_event.query("EVENT_ID > 3555260 and EVENT_ID < 3555270")
sd_fault = sd_fault.query("FAULT_ID > 3555260 and FAULT_ID < 3555270")
sched_stask = sched_stask.query("FAULT_ID > 3555260 and FAULT_ID < 3555270")

print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print("evt_event")
print(evt_event)
print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print("sd_fault")
print(sd_fault)
print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print("sched_stask")
print(sched_stask)
print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

# ----------------------------------------------------------------------
# %%
df = sd_fault

# ----------------------------------------------------------------------
# %%
# MERGE 1
"""
      SD_FAULT
      INNER JOIN EVT_EVENT FAULT_EVENT ON
        SD_FAULT.FAULT_DB_ID = FAULT_EVENT.EVENT_DB_ID AND
        SD_FAULT.FAULT_ID    = FAULT_EVENT.EVENT_ID
      --FIXED

sd_fault:  "FAULT_DB_ID", "FAULT_ID"
evt_event: "EVENT_DB_ID", "EVENT_ID"
"""

fault_event = evt_event
df = df.merge(
    fault_event,
    left_on=["FAULT_DB_ID", "FAULT_ID"],
    right_on=["EVENT_DB_ID", "EVENT_ID"],
    how="inner",
    suffixes=("", "_fault_event_1"),
)

print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print(f"Merge 1 done, {df.shape=}")
print(df)
print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

# ----------------------------------------------------------------------
# %%
# MERGE 2
"""
      INNER JOIN SCHED_STASK CORR_TASK_SS ON
        CORR_TASK_SS.FAULT_DB_ID = SD_FAULT.FAULT_DB_ID AND
        CORR_TASK_SS.FAULT_ID = SD_FAULT.FAULT_ID

df:          "FAULT_DB_ID", "FAULT_ID"
sched_stask: "FAULT_DB_ID", "FAULT_ID"
"""

corr_task_ss = sched_stask
df = df.merge(
    corr_task_ss,
    left_on=["FAULT_DB_ID", "FAULT_ID"],
    right_on=["FAULT_DB_ID", "FAULT_ID"],
    how="inner",
    suffixes=("", "_corr_task_ss_2"),
)

print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print(f"Merge 2 done, {df.shape=}")
print(df)
print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

# ----------------------------------------------------------------------
# MERGE 3
"""
      INNER JOIN EVT_EVENT TASK_EVENT ON
        TASK_EVENT.EVENT_DB_ID = CORR_TASK_SS.SCHED_DB_ID AND
        TASK_EVENT.EVENT_ID    = CORR_TASK_SS.SCHED_ID
      --FIXED 

df:        "SCHED_DB_ID", "SCHED_ID"
evt_event: "EVENT_DB_ID", "EVENT_ID"
"""
task_event = evt_event
df = df.merge(
    task_event,
    left_on=["SCHED_DB_ID", "SCHED_ID"],
    right_on=["EVENT_DB_ID", "EVENT_ID"],
    how="inner",
    suffixes=("", "_task_event_3"),
)

print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
print(f"Merge 3 done, {df.shape=}")
pd.options.display.max_columns = None
print(df)
print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

# ----------------------------------------------------------------------
# %%
u.analyze_columns(df)

quit()


# The discrepency between EVENT_ID and EVENT_ID_3 is due to the first merge.
# If the first merge is Removed, there are no issues.
