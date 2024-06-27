# %%
# Break down merge into individual pieces
# From Logbook_report.csv
# "FLEET","AC_REG_CD","SERIAL_NO_OEM","CORR_BARCODE","FAULT_FOUND_DATE","FAULT_SOURCE","DEPARTURE_LOCATION","LOGBOOK_TYPE","WRK_PKG_LOC","FAULT_NAME","FAULT_SDESC","CORRECTIVE_ACTION","FLIGHT","FLIGHT_UP_DT","MAINT_DELAY_TIME_QT","TASK_NAME","TASK_BARCODE","COMPLETION_DT","ARRIVAL_LOCATION","FAULT_STATUS","ACTION_DT","Dt Corrective Action","Corrective Action Time","ATA","LOGPAGE","WORK_PKG_BARCODE","FAULT_SEVERITY","DISRUPTION_ID"

# Save on memory

import pandas as pd

BASE = "../../copa_parquet/"

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
## # inv_inv = pd.read_parquet(BASE+'inv_inv.parquet')  # does not exist
inv_ac_reg = pd.read_parquet(BASE + "inv_ac_reg.parquet")
evt_loc = pd.read_parquet(BASE + "evt_loc.parquet")
inv_loc = pd.read_parquet(BASE + "inv_loc.parquet")
eqp_assmbl = pd.read_parquet(BASE + "eqp_assmbl.parquet")
eqp_assmbl_bom = pd.read_parquet(BASE + "eqp_assmbl_bom.parquet")
ref_event_status = pd.read_parquet(BASE + "ref_event_status.parquet")
fl_leg = pd.read_parquet(BASE + "fl_leg.parquet")
fl_leg_disrupt = pd.read_parquet(BASE + "fl_leg_disrupt.parquet")

# Perform the joins
df = sd_fault.merge(
    evt_event,
    left_on=["FAULT_DB_ID", "FAULT_ID"],
    right_on=["EVENT_DB_ID", "EVENT_ID"],
    how="inner",
    # suffixes=("", "_FAULT_EVENT"),
)
print(f"{sd_fault.shape=}, {evt_event.shape=}, {df.shape=}")
print(f"cols(df1): {df.columns}")
print("Merge 1 done.")

print(f"{sched_stask.columns=}")

duplicates = sched_stask.columns[sched_stask.columns.duplicated()]
print(f"Duplicated columns in sched_stask: {duplicates}")  # None

df = df.merge(
    sched_stask,
    left_on=["FAULT_DB_ID", "FAULT_ID"],
    right_on=["FAULT_DB_ID", "FAULT_ID"],
    how="inner",
    # suffixes=("", "_CORR_TASK_SS"),
)
print(f"df2 shape: {df.shape=}")
print("Merge 2 done.")

print("evt_event columns:", sorted(list(evt_event.columns)))
print()
# print("df2 columns:", sorted(list(df.columns)))
# evt_event joined for a second time
# (left) df2 table contains columns: "sched_db_id", "sched_id", "event_id", "event_db_id"
# (right) evt_event table contains columns "event_id" and "event_db_id"

# there are column duplication issues because evt_event appears twice on the right side o the merge
# No columns were added.
# The merge was done with evt_event a second time wit the same columns
# df3 = df2.merge(
#     evt_event,
#     left_on=["SCHED_DB_ID", "SCHED_ID"],
#     right_on=["EVENT_DB_ID", "EVENT_ID"],
#     how="inner",
#     suffixes=("", "_right"),
# )

# %%
# cols_with_x = df2.columns[df2.columns.str.endswith("_x")]
# cols_with_y = df2.columns[df2.columns.str.endswith("_y")]

"""
df_interleaved = pd.DataFrame()
for col_x, col_y in zip(cols_with_x, cols_with_y):
    df_interleaved[col_x] = df2[col_x]
    df_interleaved[col_y] = df2[col_y]
"""

# %%
"""
print(df_interleaved.shape)
print(df_interleaved.head(10))
cols = ["RSTAT_CD", "CREATION_DT", "REVISION_DT", "ALT_ID"]
for col in cols:
    col1 = col + "_x"
    col2 = col + "_y"
    col1_series = df_interleaved[col1]
    col2_series = df_interleaved[col2]
    nb_unique_1 = len(set(col1_series.values))
    nb_unique_2 = len(set(col2_series.values))
    frac_unique_1 = nb_unique_1 / df_interleaved.shape[0]
    frac_unique_2 = nb_unique_2 / df_interleaved.shape[0]
    # count the fraction of rows where col1_series == col2_series
    frac_same = (col1_series == col2_series).sum() / df_interleaved.shape[0]
    print(f"{col=}, {frac_unique_1=}, {frac_unique_2=}, {frac_same=}")
"""

# %%
# Compare each pair of columns and list the number of mismatched
# elements as a fraction of the total number of rows. Use
# df_interleaved as a starting point.
"""
for col_x, col_y in zip(cols_with_x, cols_with_y):
    print(f"{col_x=}, {col_y=}")
    print(f"{df_interleaved[col_x].equals(df_interleaved[col_y])=}")
    print(
        f"{(df_interleaved[col_x] != df_interleaved[col_y]).sum() / df_interleaved.shape[0]=}"
    )
    print()
"""

# %%

# df3 = df2

# Drop all columns from df3 whose labels end with _right.
# df3 = df3.loc[:, ~df3.columns.str.endswith("_right")]

"""
print(f"{df2.shape=}, {evt_event.shape=}, {df3.shape=}")
print(f"cols(df3): {sorted(list(df3.columns))}")

# Compare each pair of columns and list the number of mismatched elements as a fraction of the total number of rows
df3 = df
for col_x, col_y in zip(cols_with_x, cols_with_y):
    print(f"{col_x=}, {col_y=}")
    print(f"{df3[col_x].equals(df3[col_y])=}")
    print(f"{(df3[col_x] != df3[col_y]).sum() / df3.shape[0]=}")
    print()
"""

# ----------------------------------------------------------------------

#   LEFT JOIN EVT_EVENT_REL ON
#     FAULT_EVENT.EVENT_DB_ID = EVT_EVENT_REL.REL_EVENT_DB_ID AND
#     FAULT_EVENT.EVENT_ID    = EVT_EVENT_REL.REL_EVENT_ID AND
#     REL_TYPE_CD = 'DISCF'

# %%
# What is DISCF?  <<<<<<<
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
    suffixes=("", "_EVT_REL"),
)

print("Merge 4 done")
# Which are the common columns?

# %%
# MERGE 5
"""
      LEFT JOIN SCHED_STASK FOUND_ON_TASK ON
        EVT_EVENT_REL.EVENT_DB_ID = FOUND_ON_TASK.SCHED_DB_ID AND
        EVT_EVENT_REL.EVENT_ID    = FOUND_ON_TASK.SCHED_ID
"""
# df5 = df4.merge(
df = df.merge(
    sched_stask,
    left_on=["EVENT_DB_ID", "EVENT_ID"],
    right_on=["SCHED_DB_ID", "SCHED_ID"],
    how="left",
    suffixes=("", "_SCHED_STASK"),
)

print("Merge 5 done")

# %%
# Merge 6
"""
      LEFT JOIN EVT_EVENT FOUND_ON_EVENT ON
        EVT_EVENT_REL.EVENT_DB_ID = FOUND_ON_EVENT.EVENT_DB_ID AND
        EVT_EVENT_REL.EVENT_ID    = FOUND_ON_EVENT.EVENT_ID
"""

# found_on_event = df6 = df5.merge(
# found_on_event =
df = df.merge(
    evt_event,
    left_on=["EVENT_DB_ID", "EVENT_ID"],
    right_on=["EVENT_DB_ID", "EVENT_ID"],
    how="left",
    suffixes=("", "_event_evt3"),  # 3rd time event-evt used
)

print(f"Merge 6 done, {df.shape=}")

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
    suffixes=("", "_evt_loc_14"),
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
quit()
# %%
# MERGE 18
"""
      INNER JOIN EQP_ASSMBL_BOM ON
        EQP_ASSMBL_BOM.ASSMBL_DB_ID  = EVT_INV.ASSMBL_DB_ID AND
        EQP_ASSMBL_BOM.ASSMBL_CD     = EVT_INV.ASSMBL_CD AND
        EQP_ASSMBL_BOM.ASSMBL_BOM_ID = EVT_INV.ASSMBL_BOM_ID
"""
# df18 = df17.merge(
df = df.merge(
    eqp_assmbl_bom,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["ASSMBL_DB_ID", "ASSMBL_CD"],
    right_on=["ASSMBL_DB_ID", "ASSMBL_CD"],
    how="left",
    suffixes=("", "_eqp_assmbl_bom_18"),
)
print(f"Merge 18 done, {df.shape=}")

# %%
# MERGE 19
"""
      INNER JOIN REF_EVENT_STATUS FAULT_STATUS_CODE ON
        FAULT_EVENT.EVENT_STATUS_DB_ID = FAULT_STATUS_CODE.EVENT_STATUS_DB_ID AND
        FAULT_EVENT.EVENT_STATUS_CD = FAULT_STATUS_CODE.EVENT_STATUS_CD
"""
fault_status_code = ref_event_status
# df19 = df18.merge(
df = df.merge(
    fault_status_code,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["EVENT_STATUS_DB_ID", "EVENT_STATUS_CD"],
    right_on=["EVENT_STATUS_DB_ID", "EVENT_STATUS_CD"],
    how="left",
    suffixes=("", "fault_status_code_19"),
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
# df20 = df19.merge(
df = df.merge(
    task_status_code,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["EVENT_STATUS_DB_ID", "EVENT_STATUS_CD"],
    right_on=["EVENT_STATUS_DB_ID", "EVENT_STATUS_CD"],
    how="left",
    suffixes=("", "task_status_code_20"),
)
print(f"Merge 20 done, {df.shape=}")

# %%
# MERGE 21
"""
      LEFT OUTER JOIN FL_LEG FLIGHT ON
        SD_FAULT.LEG_ID = FLIGHT.LEG_ID
"""
flight = fl_leg
# df21 = df20.merge(
df = df.merge(
    flight,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on="LEG_ID",
    right_on="LEG_ID",
    how="left",
    suffixes=("", "flight_21"),
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
# df22 = df21.merge(
df = df.merge(
    dep,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["LOC_DB_ID", "LOC_ID"],
    right_on=["LOC_DB_ID", "LOC_ID"],
    how="left",
    suffixes=("", "dep_22"),
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
# df23 = df22.merge(
df = df.merge(
    arr,
    left_on=["LOC_DB_ID", "LOC_ID"],
    right_on=["LOC_DB_ID", "LOC_ID"],
    how="left",
    suffixes=("", "task_status_code_23"),
)
print(f"Merge 23 done, {df.shape=}")
# %%
# MERGE 24
"""
      LEFT JOIN FL_LEG_DISRUPT ON
        FL_LEG_DISRUPT.SCHED_DB_ID = CORR_TASK_SS.SCHED_DB_ID AND
        FL_LEG_DISRUPT.SCHED_ID = CORR_TASK_SS.SCHED_ID
"""
# df24 = df23.merge(
df = df.merge(
    fl_leg_disrupt,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["SCHED_DB_ID", "SCHED_ID"],
    right_on=["SCHED_DB_ID", "SCHED_ID"],
    how="left",
    suffixes=("", "fl_leg_disrupt_24"),
)
print(f"Merge 24 done, {df.shape=}")
quit()

print(f"{df3.shape=}, {evt_event_rel.shape=}, {df4.shape=}")
print(f"cols(df4): {sorted(list(df4.columns))}")

cols = ["REVISION_DT", "REVISION_DT_EVT_REL", "REVISION_DT_x", "REVISION_DT_y"]
print(f"\n{df4[cols].head(20)=}")
quit()


quit()

result = (
    sd_fault.merge(
        evt_event.rename(
            columns={"EVENT_DB_ID": "FAULT_DB_ID", "EVENT_ID": "FAULT_ID"}
        ),
        on=["FAULT_DB_ID", "FAULT_ID"],
        how="inner",
        suffixes=("", "_FAULT_EVENT"),
    )
    # Error on next line: ValueError: The column label 'FAULT_DB_ID' is not unique.
    .merge(
        sched_stask.rename(
            columns={"SCHED_DB_ID": "FAULT_DB_ID", "SCHED_ID": "FAULT_ID"}
        ),
        on=["FAULT_DB_ID", "FAULT_ID"],
        how="inner",
        suffixes=("", "_CORR_TASK_SS"),
    )
    .merge(
        evt_event.rename(
            columns={"EVENT_DB_ID": "SCHED_DB_ID", "EVENT_ID": "SCHED_ID"}
        ),
        on=["SCHED_DB_ID", "SCHED_ID"],
        how="inner",
        suffixes=("", "_TASK_EVENT"),
    )
    .merge(
        evt_event_rel,
        left_on=["EVENT_DB_ID_FAULT_EVENT", "EVENT_ID_FAULT_EVENT"],
        right_on=["REL_EVENT_DB_ID", "REL_EVENT_ID"],
        how="left",
    )
    .merge(
        sched_stask.rename(
            columns={"SCHED_DB_ID": "EVENT_DB_ID", "SCHED_ID": "EVENT_ID"}
        ),
        on=["EVENT_DB_ID", "EVENT_ID"],
        how="left",
        suffixes=("", "_FOUND_ON_TASK"),
    )
    .merge(
        evt_event.rename(
            columns={"EVENT_DB_ID": "EVENT_DB_ID", "EVENT_ID": "EVENT_ID"}
        ),
        on=["EVENT_DB_ID", "EVENT_ID"],
        how="left",
        suffixes=("", "_FOUND_ON_EVENT"),
    )
    .merge(
        evt_event.rename(
            columns={"EVENT_DB_ID": "H_EVENT_DB_ID", "EVENT_ID": "H_EVENT_ID"}
        ),
        on=["H_EVENT_DB_ID", "H_EVENT_ID"],
        how="left",
        suffixes=("", "_WORK_PKG_EVENT"),
    )
    .merge(
        sched_stask.rename(
            columns={
                "SCHED_DB_ID": "EVENT_DB_ID_WORK_PKG_EVENT",
                "SCHED_ID": "EVENT_ID_WORK_PKG_EVENT",
            }
        ),
        on=["EVENT_DB_ID_WORK_PKG_EVENT", "EVENT_ID_WORK_PKG_EVENT"],
        how="left",
        suffixes=("", "_WORK_PKG_SCHED_STASK"),
    )
    .merge(sched_action, on=["SCHED_DB_ID", "SCHED_ID"], how="left")
    .merge(
        evt_inv,
        left_on=["EVENT_DB_ID_FAULT_EVENT", "EVENT_ID_FAULT_EVENT"],
        right_on=["EVENT_DB_ID", "EVENT_ID"],
        how="left",
    )
    .merge(
        inv_inv,
        left_on=["H_INV_NO_DB_ID", "H_INV_NO_ID"],
        right_on=["INV_NO_DB_ID", "INV_NO_ID"],
        how="left",
    )
    .merge(inv_ac_reg, on=["INV_NO_DB_ID", "INV_NO_ID"], how="left")
    .merge(
        inv_inv.rename(
            columns={"INV_NO_DB_ID": "MAIN_INV_NO_DB_ID", "INV_NO_ID": "MAIN_INV_NO_ID"}
        ),
        on=["MAIN_INV_NO_DB_ID", "MAIN_INV_NO_ID"],
        how="left",
        suffixes=("", "_ACFT_II_NEW"),
    )
    .merge(
        inv_ac_reg.rename(
            columns={"INV_NO_DB_ID": "H_INV_NO_DB_ID", "INV_NO_ID": "H_INV_NO_ID"}
        ),
        on=["H_INV_NO_DB_ID", "H_INV_NO_ID"],
        how="left",
        suffixes=("", "_ACFT_IAR_NEW"),
    )
    .merge(
        evt_loc.rename(
            columns={
                "EVENT_DB_ID": "EVENT_DB_ID_WORK_PKG_EVENT",
                "EVENT_ID": "EVENT_ID_WORK_PKG_EVENT",
            }
        ),
        on=["EVENT_DB_ID_WORK_PKG_EVENT", "EVENT_ID_WORK_PKG_EVENT"],
        how="left",
    )
    .merge(inv_loc, on=["LOC_DB_ID", "LOC_ID"], how="left")
    .merge(eqp_assmbl, on=["ASSMBL_DB_ID", "ASSMBL_CD"], how="inner")
    .merge(
        eqp_assmbl_bom, on=["ASSMBL_DB_ID", "ASSMBL_CD", "ASSMBL_BOM_ID"], how="inner"
    )
    .merge(
        ref_event_status.rename(
            columns={
                "EVENT_STATUS_DB_ID": "EVENT_STATUS_DB_ID_FAULT_EVENT",
                "EVENT_STATUS_CD": "EVENT_STATUS_CD_FAULT_EVENT",
            }
        ),
        on=["EVENT_STATUS_DB_ID_FAULT_EVENT", "EVENT_STATUS_CD_FAULT_EVENT"],
        how="inner",
        suffixes=("", "_FAULT_STATUS_CODE"),
    )
    .merge(
        ref_event_status.rename(
            columns={
                "EVENT_STATUS_DB_ID": "EVENT_STATUS_DB_ID_TASK_EVENT",
                "EVENT_STATUS_CD": "EVENT_STATUS_CD_TASK_EVENT",
            }
        ),
        on=["EVENT_STATUS_DB_ID_TASK_EVENT", "EVENT_STATUS_CD_TASK_EVENT"],
        how="inner",
        suffixes=("", "_TASK_STATUS_CODE"),
    )
    .merge(fl_leg.rename(columns={"LEG_ID": "LEG_ID"}), on="LEG_ID", how="left")
    .merge(
        inv_loc.rename(
            columns={"LOC_DB_ID": "DEPARTURE_LOC_DB_ID", "LOC_ID": "DEPARTURE_LOC_ID"}
        ),
        on=["DEPARTURE_LOC_DB_ID", "DEPARTURE_LOC_ID"],
        how="left",
        suffixes=("", "_DEP"),
    )
    .merge(
        inv_loc.rename(
            columns={"LOC_DB_ID": "ARRIVAL_LOC_DB_ID", "LOC_ID": "ARRIVAL_LOC_ID"}
        ),
        on=["ARRIVAL_LOC_DB_ID", "ARRIVAL_LOC_ID"],
        how="left",
        suffixes=("", "_ARR"),
    )
    .merge(fl_leg_disrupt, on=["SCHED_DB_ID", "SCHED_ID"], how="left")
)

# Apply filters
result = result[result["FAULT_SOURCE_CD"].isin(["MECH", "PILOT", "CABIN", "AUTH"])]
result = result[result["TASK_CLASS_CD_WORK_PKG_SCHED_STASK"] != "RO"]

# Select and rename columns as per the SELECT statement
result = result[
    [
        "ASSMBL_CD",
        "AC_REG_CD",
        "SERIAL_NO_OEM",
        "BARCODE_SDESC",
        "ACTUAL_START_DT",
        "FAULT_SOURCE_CD",
        "LOC_CD_DEP",
        "FAULT_LOG_TYPE_CD",
        "LOC_CD",
        "EVENT_SDESC_FAULT_EVENT",
        "EVENT_LDESC_FAULT_EVENT",
        "ACTION_LDESC",
        "LEG_NO",
        "OFF_DT",
        "MAINT_DELAY_TIME_QT",
        "EVENT_SDESC_FOUND_ON_EVENT",
        "BARCODE_SDESC_FOUND_ON_TASK",
        "EVENT_DT_TASK_EVENT",
        "LOC_CD_ARR",
        "USER_STATUS_CD",
        "ACTION_DT",
        "ASSMBL_BOM_CD",
        "DOC_REF_SDESC",
        "BARCODE_SDESC_WORK_PKG_SCHED_STASK",
        "FAIL_SEV_CD",
        "DISRUPTION_DESC",
    ]
]

result = result.rename(
    columns={
        "ASSMBL_CD": "FLEET",
        "AC_REG_CD": "AC_REG_CD",
        "SERIAL_NO_OEM": "SERIAL_NO_OEM",
        "BARCODE_SDESC": "CORR_BARCODE",
        "ACTUAL_START_DT": "FAULT_FOUND_DATE",
        "FAULT_SOURCE_CD": "FAULT_SOURCE",
        "LOC_CD_DEP": "DEPARTURE_LOCATION",
        "FAULT_LOG_TYPE_CD": "LOGBOOK_TYPE",
        "LOC_CD": "WRK_PKG_LOC",
        "EVENT_SDESC_FAULT_EVENT": "FAULT_NAME",
        "EVENT_LDESC_FAULT_EVENT": "FAULT_SDESC",
        "ACTION_LDESC": "CORRECTIVE_ACTION",
        "LEG_NO": "FLIGHT",
        "OFF_DT": "FLIGHT_UP_DT",
        "MAINT_DELAY_TIME_QT": "MAINT_DELAY_TIME_QT",
        "EVENT_SDESC_FOUND_ON_EVENT": "TASK_NAME",
        "BARCODE_SDESC_FOUND_ON_TASK": "TASK_BARCODE",
        "EVENT_DT_TASK_EVENT": "COMPLETION_DT",
        "LOC_CD_ARR": "ARRIVAL_LOCATION",
        "USER_STATUS_CD": "FAULT_STATUS",
        "ACTION_DT": "ACTION_DT",
        "ASSMBL_BOM_CD": "ATA",
        "DOC_REF_SDESC": "LOGPAGE",
        "BARCODE_SDESC_WORK_PKG_SCHED_STASK": "WORK_PKG_BARCODE",
        "FAIL_SEV_CD": "FAULT_SEVERITY",
        "DISRUPTION_DESC": "DISRUPTION_ID",
    }
)

# Add calculated columns
result["Dt Corrective Action"] = result["ACTION_DT"].dt.strftime("%d/%m/%Y %H:%M:%S")
result["Corrective Action Time"] = result["ACTION_DT"].dt.strftime("%I:%M:%S %p")

# Sort the result
result = result.sort_values("ACTION_DT")

print(result)

# %%
