# %%
# Break down merge into individual pieces
# From Logbook_report.csv
# "FLEET","AC_REG_CD","SERIAL_NO_OEM","CORR_BARCODE","FAULT_FOUND_DATE","FAULT_SOURCE","DEPARTURE_LOCATION","LOGBOOK_TYPE","WRK_PKG_LOC","FAULT_NAME","FAULT_SDESC","CORRECTIVE_ACTION","FLIGHT","FLIGHT_UP_DT","MAINT_DELAY_TIME_QT","TASK_NAME","TASK_BARCODE","COMPLETION_DT","ARRIVAL_LOCATION","FAULT_STATUS","ACTION_DT","Dt Corrective Action","Corrective Action Time","ATA","LOGPAGE","WORK_PKG_BARCODE","FAULT_SEVERITY","DISRUPTION_ID"

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
# evt_event_rel = pd.read_parquet(BASE + "evt_event_rel.parquet")

nrows = 100000000
sd_fault = sd_fault.iloc[:nrows] if sd_fault.shape[0] > nrows else sd_fault
evt_event = evt_event.iloc[:nrows] if evt_event.shape[0] > nrows else evt_event
sched_stask = sched_stask.iloc[:nrows] if sched_stask.shape[0] > nrows else sched_stask
# evt_event_rel = (
# evt_event_rel.iloc[:nrows] if evt_event_rel.shape[0] > nrows else evt_event_rel
# )

# %%

# sched_action = pd.read_parquet(BASE + "sched_action.parquet")
# evt_inv = pd.read_parquet(BASE + "evt_inv.parquet")
# # inv_inv = pd.read_parquet(BASE+'inv_inv.parquet')  # does not exist
# inv_ac_reg = pd.read_parquet(BASE + "inv_ac_reg.parquet")
# evt_loc = pd.read_parquet(BASE + "evt_loc.parquet")
# inv_loc = pd.read_parquet(BASE + "inv_loc.parquet")
# eqp_assmbl = pd.read_parquet(BASE + "eqp_assmbl.parquet")
# eqp_assmbl_bom = pd.read_parquet(BASE + "eqp_assmbl_bom.parquet")
# ref_event_status = pd.read_parquet(BASE + "ref_event_status.parquet")
# fl_leg = pd.read_parquet(BASE + "fl_leg.parquet")
# fl_leg_disrupt = pd.read_parquet(BASE + "fl_leg_disrupt.parquet")

# %%

# ----------------------------------------------------------------------
df = sd_fault

# ----------------------------------------------------------------------
# MERGE 1
"""
      SD_FAULT
      INNER JOIN EVT_EVENT FAULT_EVENT ON
        SD_FAULT.FAULT_DB_ID = FAULT_EVENT.EVENT_DB_ID AND
        SD_FAULT.FAULT_ID    = FAULT_EVENT.EVENT_ID
      --FIXED
"""

fault_event = evt_event
df = df.merge(
    fault_event,
    left_on=["FAULT_DB_ID", "FAULT_ID"],
    right_on=["EVENT_DB_ID", "EVENT_ID"],
    how="inner",
    suffixes=("", "_fault_event_1"),
)

print(f"Merge 1 done, {df.shape=}")
# Which are the common columns?

# ----------------------------------------------------------------------
# MERGE 2
"""
      INNER JOIN SCHED_STASK CORR_TASK_SS ON
        CORR_TASK_SS.FAULT_DB_ID = SD_FAULT.FAULT_DB_ID AND
        CORR_TASK_SS.FAULT_ID = SD_FAULT.FAULT_ID
"""

corr_task_ss = sched_stask
df = df.merge(
    corr_task_ss,
    left_on=["FAULT_DB_ID", "FAULT_ID"],
    right_on=["FAULT_DB_ID", "FAULT_ID"],
    how="inner",
    suffixes=("", "_corr_task_ss_2"),
)

print(f"Merge 2 done, {df.shape=}")
# Which are the common columns?

# ----------------------------------------------------------------------
# MERGE 3
"""
      INNER JOIN EVT_EVENT TASK_EVENT ON
        TASK_EVENT.EVENT_DB_ID = CORR_TASK_SS.SCHED_DB_ID AND
        TASK_EVENT.EVENT_ID    = CORR_TASK_SS.SCHED_ID
      --FIXED 
"""
task_event = evt_event
df = df.merge(
    task_event,
    left_on=["SCHED_DB_ID", "SCHED_ID"],
    right_on=["EVENT_DB_ID", "EVENT_ID"],
    how="inner",
    suffixes=("", "_task_event_3"),
)

print(f"Merge 3 done, {df.shape=}")
# Which are the common columns?
# ----------------------------------------------------------------------

# Analyze additional with suffix
cols_dict = u.create_cols_dict(df)
u.are_columns_equal(cols_dict, "Check column equality")
cols_dict1 = u.calculate_fracs(df, cols_dict, remove_equal=True, print_fracs=False)
cols_dict = u.calculate_fracs(df, cols_dict1, remove_equal=False, print_fracs=True)
print("===========================================")
print("cols_dict, u.calculate_fracs")
pprint(cols_dict)

print("===========================================")
u.print_freq_nan(df, cols_dict)
