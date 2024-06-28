# %%
# Break down merge into individual pieces
# From Logbook_report.csv
# "FLEET","AC_REG_CD","SERIAL_NO_OEM","CORR_BARCODE","FAULT_FOUND_DATE","FAULT_SOURCE","DEPARTURE_LOCATION","LOGBOOK_TYPE","WRK_PKG_LOC","FAULT_NAME","FAULT_SDESC","CORRECTIVE_ACTION","FLIGHT","FLIGHT_UP_DT","MAINT_DELAY_TIME_QT","TASK_NAME","TASK_BARCODE","COMPLETION_DT","ARRIVAL_LOCATION","FAULT_STATUS","ACTION_DT","Dt Corrective Action","Corrective Action Time","ATA","LOGPAGE","WORK_PKG_BARCODE","FAULT_SEVERITY","DISRUPTION_ID"

# Save on memory

import pandas as pd

BASE = "../../copa_parquet/"
BASE = "parquet/"

# Load all the tables from parquet files
sd_fault = pd.read_parquet(BASE + "sd_fault.parquet")
evt_event = pd.read_parquet(BASE + "evt_event.parquet")
sched_stask = pd.read_parquet(BASE + "sched_stask.parquet")
evt_event_rel = pd.read_parquet(BASE + "evt_event_rel.parquet")

nrows = 100000000
sd_fault = sd_fault.iloc[:nrows] if sd_fault.shape[0] > nrows else sd_fault
evt_event = evt_event.iloc[:nrows] if evt_event.shape[0] > nrows else evt_event
sched_stask = sched_stask.iloc[:nrows] if sched_stask.shape[0] > nrows else sched_stask
evt_event_rel = (
    evt_event_rel.iloc[:nrows] if evt_event_rel.shape[0] > nrows else evt_event_rel
)

# %%

sched_action = pd.read_parquet(BASE + "sched_action.parquet")
evt_inv = pd.read_parquet(BASE + "evt_inv.parquet")
# # inv_inv = pd.read_parquet(BASE+'inv_inv.parquet')  # does not exist
inv_ac_reg = pd.read_parquet(BASE + "inv_ac_reg.parquet")
evt_loc = pd.read_parquet(BASE + "evt_loc.parquet")
inv_loc = pd.read_parquet(BASE + "inv_loc.parquet")
eqp_assmbl = pd.read_parquet(BASE + "eqp_assmbl.parquet")
eqp_assmbl_bom = pd.read_parquet(BASE + "eqp_assmbl_bom.parquet")
ref_event_status = pd.read_parquet(BASE + "ref_event_status.parquet")
fl_leg = pd.read_parquet(BASE + "fl_leg.parquet")
fl_leg_disrupt = pd.read_parquet(BASE + "fl_leg_disrupt.parquet")

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

print(f"Merge 4 done, {df.shape=}")
# Which are the common columns?
# ----------------------------------------------------------------------

# MERGE 4
"""
     LEFT JOIN EVT_EVENT_REL ON
        FAULT_EVENT.EVENT_DB_ID = EVT_EVENT_REL.REL_EVENT_DB_ID AND
        FAULT_EVENT.EVENT_ID    = EVT_EVENT_REL.REL_EVENT_ID AND
        REL_TYPE_CD = 'DISCF'
     -- #### This join does nothing.
"""
# df4 = df3.merge(
df = df.merge(
    evt_event_rel[evt_event_rel["REL_TYPE_CD"] == "DISCF"],
    left_on=["EVENT_DB_ID", "EVENT_ID"],
    right_on=["REL_EVENT_DB_ID", "REL_EVENT_ID"],
    how="left",
    suffixes=("", "_evt_event_rel_4"),
)

print(f"Merge 4 done, {df.shape=}")
# Which are the common columns?
# ----------------------------------------------------------------------
# %%
# MERGE 5
"""
      LEFT JOIN SCHED_STASK FOUND_ON_TASK ON
        EVT_EVENT_REL.EVENT_DB_ID = FOUND_ON_TASK.SCHED_DB_ID AND
        EVT_EVENT_REL.EVENT_ID    = FOUND_ON_TASK.SCHED_ID
"""
# df5 = df4.merge(
found_on_task = sched_stask
df = df.merge(
    found_on_task,
    left_on=["EVENT_DB_ID", "EVENT_ID"],
    right_on=["SCHED_DB_ID", "SCHED_ID"],
    how="left",
    suffixes=("", "_found_on_task_5"),
)

print(f"Merge 5 done, {df.shape=}")

# ----------------------------------------------------------------------
# %%
# Merge 6
"""
      LEFT JOIN EVT_EVENT FOUND_ON_EVENT ON
        EVT_EVENT_REL.EVENT_DB_ID = FOUND_ON_EVENT.EVENT_DB_ID AND
        EVT_EVENT_REL.EVENT_ID    = FOUND_ON_EVENT.EVENT_ID
"""

# found_on_event = df6 = df5.merge(
# found_on_event =
found_on_event = evt_event
df = df.merge(
    found_on_event,
    left_on=["EVENT_DB_ID", "EVENT_ID"],
    right_on=["EVENT_DB_ID", "EVENT_ID"],
    how="left",
    suffixes=("", "_found_on_event_6"),  # 3rd time event-evt used
)

print(f"Merge 6 done, {df.shape=}")

# ----------------------------------------------------------------------
# %%
# MERGE 7
"""
      LEFT JOIN EVT_EVENT WORK_PKG_EVENT ON
        WORK_PKG_EVENT.EVENT_DB_ID = TASK_EVENT.H_EVENT_DB_ID AND
        WORK_PKG_EVENT.EVENT_ID    = TASK_EVENT.H_EVENT_ID
"""

work_pkg_event = evt_event
# df7 = df6.merge(
df = df.merge(
    work_pkg_event,
    left_on=["H_EVENT_DB_ID", "H_EVENT_ID"],
    right_on=["EVENT_DB_ID", "EVENT_ID"],
    how="left",
    suffixes=("", "_work_pkg_event_7"),  # 3rd time event-evt used
)
print(f"Merge 7 done, {df.shape=}")


# %%
# MERGE 8
"""
    LEFT JOIN SCHED_STASK WORK_PKG_SCHED_STASK ON
      WORK_PKG_SCHED_STASK.SCHED_DB_ID = WORK_PKG_EVENT.EVENT_DB_ID AND
      WORK_PKG_SCHED_STASK.SCHED_ID    = WORK_PKG_EVENT.EVENT_ID AND
      WORK_PKG_SCHED_STASK.TASK_CLASS_CD in ('CHECK','RD')
"""
work_pkg_sched_stask = sched_stask
# df8 = df7.merge(
df = df.merge(
    work_pkg_sched_stask[work_pkg_sched_stask["TASK_CLASS_CD"].isin(["CHECK", "RD"])],
    left_on=["EVENT_DB_ID", "EVENT_ID"],
    right_on=["SCHED_DB_ID", "SCHED_ID"],
    how="left",
    suffixes=("", "_evt_event_work_8"),
)
print(f"Merge 8 done, {df.shape=}")

# %%
# MERGE 9
"""
     LEFT OUTER JOIN SCHED_ACTION ON
        CORR_TASK_SS.SCHED_DB_ID = SCHED_ACTION.SCHED_DB_ID AND
        CORR_TASK_SS.SCHED_ID =  SCHED_ACTION.SCHED_ID
"""

# df9 = df8.merge(
df = df.merge(
    sched_action,
    left_on=["SCHED_DB_ID", "SCHED_DB_ID"],
    right_on=["SCHED_DB_ID", "SCHED_ID"],
    how="left",
    suffixes=("", "_sched_action_9"),
)

print(f"Merge 9 done, {df.shape=}")

# %%
# MERGE 10
"""
      LEFT JOIN EVT_INV ON
        EVT_INV.EVENT_DB_ID = FAULT_EVENT.EVENT_DB_ID AND
        EVT_INV.EVENT_ID    = FAULT_EVENT.EVENT_ID
"""
# df10 = df9.merge(
df = df.merge(
    evt_inv,
    left_on=["EVENT_DB_ID", "EVENT_ID"],
    right_on=["EVENT_DB_ID", "EVENT_ID"],
    how="left",
    suffixes=("", "_evt_inv_10"),
)

print(f"Merge 10 done, {df.shape=}")
# %%
# MERGE 11
"""
      LEFT JOIN INV_INV ON
        INV_INV.INV_NO_DB_ID = EVT_INV.H_INV_NO_DB_ID AND
        INV_INV.INV_NO_ID    = EVT_INV.H_INV_NO_ID
"""
# df11 = df10
# df = df

# Missing INV_INV table

print(f"Merge 11 done, {df.shape=}")
# %%
# MERGE 12
"""
      LEFT JOIN INV_AC_REG ON
        INV_INV.INV_NO_DB_ID = INV_AC_REG.INV_NO_DB_ID AND
        INV_INV.INV_NO_ID = INV_AC_REG.INV_NO_ID
"""
# df12 = df11.merge(
df = df.merge(
    inv_ac_reg,
    left_on=["H_INV_NO_DB_ID", "H_INV_NO_ID"],
    right_on=["INV_NO_DB_ID", "INV_NO_ID"],
    how="left",
    suffixes=("", "_inv_ac_reg_12"),
)
print(f"Merge 12 done, {df.shape=}")

# %%
# MERGE 13
"""
      LEFT JOIN INV_INV ACFT_II_NEW ON
        ACFT_II_NEW.INV_NO_DB_ID = CORR_TASK_SS.MAIN_INV_NO_DB_ID AND
        ACFT_II_NEW.INV_NO_ID    = CORR_TASK_SS.MAIN_INV_NO_ID
"""
# df13 = df12
# MISSING INV_INV
print(f"Merge 13 done, {df.shape=}")

# %%
# MERGE 14
"""
      LEFT JOIN INV_AC_REG ACFT_IAR_NEW ON
        ACFT_IAR_NEW.INV_NO_DB_ID = ACFT_II_NEW.H_INV_NO_DB_ID AND
        ACFT_IAR_NEW.INV_NO_ID = ACFT_II_NEW.H_INV_NO_ID
"""
# Print the column names that contain with "INV"
# print([col for col in df13.columns if "INV" in col])
# print()
# print(f"{inv_ac_reg.columns=}")

# inv_ac_reg has columns INV_NO_DB_ID, etc.
# df13 has columns H_INV_NO_DB_ID, etc.

acft_iar_new = inv_ac_reg
# df14 = df13.merge(
df = df.merge(
    acft_iar_new,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["MAIN_INV_NO_DB_ID", "MAIN_INV_NO_ID"],
    right_on=["INV_NO_DB_ID", "INV_NO_ID"],
    how="left",
    suffixes=("", "_acft_iar_new_14"),
)

print(f"Merge 14 done, {df.shape=}")
# %%
# MERGE 15
"""
      LEFT JOIN EVT_LOC ON
        EVT_LOC.EVENT_DB_ID = WORK_PKG_EVENT.EVENT_DB_ID AND
        EVT_LOC.EVENT_ID    = WORK_PKG_EVENT.EVENT_ID
"""

# df15 = df14.merge(
df = df.merge(
    evt_loc,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["EVENT_DB_ID", "EVENT_ID"],
    right_on=["EVENT_DB_ID", "EVENT_ID"],
    how="left",
    suffixes=("", "_evt_loc_15"),
)

print(f"Merge 15 done, {df.shape=}")

# %%
# MERGE 16
"""
     LEFT JOIN INV_LOC ON
        EVT_LOC.LOC_DB_ID = INV_LOC.LOC_DB_ID AND
        EVT_LOC.LOC_ID    = INV_LOC.LOC_ID
"""

# df16 = df15.merge(
df = df.merge(
    inv_loc,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["LOC_DB_ID", "LOC_ID"],
    right_on=["LOC_DB_ID", "LOC_ID"],
    how="left",
    suffixes=("", "_inv_loc_16"),
)

print(f"Merge 16 done, {df.shape=}")

# %%
# Release memory
# del df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15


# %%
# MERGE 17
"""
      INNER JOIN EQP_ASSMBL ON
        EQP_ASSMBL.ASSMBL_DB_ID = EVT_INV.ASSMBL_DB_ID AND
        EQP_ASSMBL.ASSMBL_CD    = EVT_INV.ASSMBL_CD
"""
# df17 = df16.merge(
df = df.merge(
    eqp_assmbl,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["ASSMBL_DB_ID", "ASSMBL_CD"],
    right_on=["ASSMBL_DB_ID", "ASSMBL_CD"],
    how="left",
    suffixes=("", "_eqp_assmbl_17"),
)

print(f"Merge 17 done, {df.shape=}")
# df.to_csv("merge17.csv", index=False)
df.to_parquet("merge17.parquet", index=False)

# ----------------------------------------------------------------------
# %%
# Merge 18
"""
      INNER JOIN EQP_ASSMBL_BOM ON
        EQP_ASSMBL_BOM.ASSMBL_DB_ID  = EVT_INV.ASSMBL_DB_ID AND
        EQP_ASSMBL_BOM.ASSMBL_CD     = EVT_INV.ASSMBL_CD AND
        EQP_ASSMBL_BOM.ASSMBL_BOM_ID = EVT_INV.ASSMBL_BOM_ID
"""
"""
df18 = df17.merge(
    eqp_assmbl_bom,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["ASSMBL_DB_ID", "ASSMBL_CD"],
    right_on=["ASSMBL_DB_ID", "ASSMBL_CD"],
    how="left",
    suffixes=("", "_eqp_assmbl_bom_18"),
)
print(f"Merge 18 done, {df18.shape=}")
"""
# This merge leads to an explosion of rows: there are too few unique values of the keys.

# ----------------------------------------------------------------------
# %%
# MERGE 19
"""
      INNER JOIN REF_EVENT_STATUS FAULT_STATUS_CODE ON
        FAULT_EVENT.EVENT_STATUS_DB_ID = FAULT_STATUS_CODE.EVENT_STATUS_DB_ID AND
        FAULT_EVENT.EVENT_STATUS_CD = FAULT_STATUS_CODE.EVENT_STATUS_CD
"""
fault_status_code = ref_event_status
df = df.merge(
    fault_status_code,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["EVENT_STATUS_DB_ID", "EVENT_STATUS_CD"],
    right_on=["EVENT_STATUS_DB_ID", "EVENT_STATUS_CD"],
    how="left",
    suffixes=("", "_fault_status_code_19"),
)
print(f"Merge 19 done, {df.shape=}")

# %%

# MERGE 20
"""
      INNER JOIN REF_EVENT_STATUS TASK_STATUS_CODE ON
        TASK_EVENT.EVENT_STATUS_DB_ID = TASK_STATUS_CODE.EVENT_STATUS_DB_ID AND
        TASK_EVENT.EVENT_STATUS_CD = TASK_STATUS_CODE.EVENT_STATUS_CD
"""
task_status_code = ref_event_status
df = df.merge(
    task_status_code,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["EVENT_STATUS_DB_ID", "EVENT_STATUS_CD"],
    right_on=["EVENT_STATUS_DB_ID", "EVENT_STATUS_CD"],
    how="left",
    suffixes=("", "_task_status_code_20"),
)
print(f"Merge 20 done, {df.shape=}")

# %%
# MERGE 21
"""
      LEFT OUTER JOIN FL_LEG FLIGHT ON
        SD_FAULT.LEG_ID = FLIGHT.LEG_ID
"""
flight = fl_leg
df = df.merge(
    flight,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on="LEG_ID",
    right_on="LEG_ID",
    how="left",
    suffixes=("", "_flight_21"),
)
print(f"Merge 21 done, {df.shape=}")

# %%
# MERGE 22
"""
      LEFT OUTER JOIN INV_LOC DEP ON
        FLIGHT.DEPARTURE_LOC_DB_ID = DEP.LOC_DB_ID AND
        FLIGHT.DEPARTURE_LOC_ID = DEP.LOC_ID
"""
dep = inv_loc
df = df.merge(
    dep,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["LOC_DB_ID", "LOC_ID"],
    right_on=["LOC_DB_ID", "LOC_ID"],
    how="left",
    suffixes=("", "_dep_22"),
)
print(f"Merge 22 done, {df.shape=}")

# %%
# MERGE 23
"""
      LEFT OUTER JOIN INV_LOC ARR ON
        FLIGHT.ARRIVAL_LOC_DB_ID = ARR.LOC_DB_ID AND
        FLIGHT.ARRIVAL_LOC_ID = ARR.LOC_ID
"""
arr = inv_loc
df = df.merge(
    arr,
    left_on=["LOC_DB_ID", "LOC_ID"],
    right_on=["LOC_DB_ID", "LOC_ID"],
    how="left",
    suffixes=("", "_task_status_code_23"),
)
print(f"Merge 23 done, {df.shape=}")
# %%
# MERGE 24
"""
      LEFT JOIN FL_LEG_DISRUPT ON
        FL_LEG_DISRUPT.SCHED_DB_ID = CORR_TASK_SS.SCHED_DB_ID AND
        FL_LEG_DISRUPT.SCHED_ID = CORR_TASK_SS.SCHED_ID
"""

# Remove all rows from fl_leg_disrupt where SCHED_DB_ID is empty
fl_leg_disrupt = fl_leg_disrupt.dropna(subset=["SCHED_DB_ID", "SCHED_ID"])
# Remove lines where SCHED_DB_ID or SCHED_ID is not an integer
fl_leg_disrupt = fl_leg_disrupt[
    fl_leg_disrupt["SCHED_DB_ID"].str.isnumeric()
    & fl_leg_disrupt["SCHED_ID"].str.isnumeric()
]

# Remove from `fl_leg_disrupt` all rows where either `SCHED_DB_ID` or `SCHED_ID` is not an integer.
fl_leg_disrupt = fl_leg_disrupt[
    fl_leg_disrupt["SCHED_DB_ID"].str.isnumeric()
    & fl_leg_disrupt["SCHED_ID"].str.isnumeric()
]

fl_leg_disrupt["SCHED_DB_ID"] = fl_leg_disrupt["SCHED_DB_ID"].astype(int)
fl_leg_disrupt["SCHED_ID"] = fl_leg_disrupt["SCHED_ID"].astype(int)

df1 = df[["SCHED_DB_ID", "SCHED_ID"]]
fl1 = fl_leg_disrupt[["SCHED_DB_ID", "SCHED_ID"]]
df1.to_csv("df1.csv", index=False)
fl1.to_csv("fl1.csv", index=False)


df = df.merge(  # ! ERROR <<<<<<<<<<<<<<<<
    # ! ValueError: You are trying to merge on int64 and object columns for key 'SCHED_DB_ID'. If you wish to proceed you should use pd.concat
    fl_leg_disrupt,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["SCHED_DB_ID", "SCHED_ID"],
    right_on=["SCHED_DB_ID", "SCHED_ID"],
    how="left",
    suffixes=("", "_fl_leg_disrupt_24"),
)
print(f"Merge 24 done, {df.shape=}")
df.to_parquet(BASE + "merged_df.parquet", index=False)
df.to_csv("merged_df.csv", index=False)
quit()

# ----------------------------------------------------------------------
