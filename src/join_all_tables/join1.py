# %%
# Break down merge into individual pieces
# From Logbook_report.csv
# "FLEET","AC_REG_CD","SERIAL_NO_OEM","CORR_BARCODE","FAULT_FOUND_DATE","FAULT_SOURCE","DEPARTURE_LOCATION","LOGBOOK_TYPE","WRK_PKG_LOC","FAULT_NAME","FAULT_SDESC","CORRECTIVE_ACTION","FLIGHT","FLIGHT_UP_DT","MAINT_DELAY_TIME_QT","TASK_NAME","TASK_BARCODE","COMPLETION_DT","ARRIVAL_LOCATION","FAULT_STATUS","ACTION_DT","Dt Corrective Action","Corrective Action Time","ATA","LOGPAGE","WORK_PKG_BARCODE","FAULT_SEVERITY","DISRUPTION_ID"

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

## sched_action = pd.read_parquet(BASE+'sched_action.parquet')
## evt_inv = pd.read_parquet(BASE+'evt_inv.parquet')
## # inv_inv = pd.read_parquet(BASE+'inv_inv.parquet')  # does not exist
## inv_ac_reg = pd.read_parquet(BASE+'inv_ac_reg.parquet')
## evt_loc = pd.read_parquet(BASE+'evt_loc.parquet')
## inv_loc = pd.read_parquet(BASE+'inv_loc.parquet')
## eqp_assmbl = pd.read_parquet(BASE+'eqp_assmbl.parquet')
## eqp_assmbl_bom = pd.read_parquet(BASE+'eqp_assmbl_bom.parquet')
## ref_event_status = pd.read_parquet(BASE+'ref_event_status.parquet')
## fl_leg = pd.read_parquet(BASE+'fl_leg.parquet')
## fl_leg_disrupt = pd.read_parquet(BASE+'fl_leg_disrupt.parquet')

# Perform the joins
df1 = sd_fault.merge(
    evt_event,
    left_on=["FAULT_DB_ID", "FAULT_ID"],
    right_on=["EVENT_DB_ID", "EVENT_ID"],
    how="inner",
    # suffixes=("", "_FAULT_EVENT"),
)
print(f"{sd_fault.shape=}, {evt_event.shape=}, {df1.shape=}")
print(f"cols(df1): {df1.columns}")

print(f"{sched_stask.columns=}")

duplicates = sched_stask.columns[sched_stask.columns.duplicated()]
print(f"Duplicated columns in sched_stask: {duplicates}")  # None

df2 = df1.merge(
    sched_stask,
    left_on=["FAULT_DB_ID", "FAULT_ID"],
    right_on=["FAULT_DB_ID", "FAULT_ID"],
    how="inner",
    # suffixes=("", "_CORR_TASK_SS"),
)
print(f"{df1.shape=}, {sched_stask.shape=}, {df2.shape=}")
print(f"cols(df2): {df2.columns}")

print("evt_event columns:", sorted(list(evt_event.columns)))
print()
print("df2 columns:", sorted(list(df2.columns)))
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
3

# %%
cols_with_x = df2.columns[df2.columns.str.endswith("_x")]
cols_with_y = df2.columns[df2.columns.str.endswith("_y")]

df_interleaved = pd.DataFrame()
for col_x, col_y in zip(cols_with_x, cols_with_y):
    df_interleaved[col_x] = df2[col_x]
    df_interleaved[col_y] = df2[col_y]

# %%
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


quit()
# %%
# Compare each pair of columns and list the number of mismatched elements as a fraction of the total number of rows. Use df_interleaved as a starting point.
for col_x, col_y in zip(cols_with_x, cols_with_y):
    print(f"{col_x=}, {col_y=}")
    print(f"{df_interleaved[col_x].equals(df_interleaved[col_y])=}")
    print(
        f"{(df_interleaved[col_x] != df_interleaved[col_y]).sum() / df_interleaved.shape[0]=}"
    )
    print()

# %%

df3 = df2

# Drop all columns from df3 whose labels end with _right.
# df3 = df3.loc[:, ~df3.columns.str.endswith("_right")]

print(f"{df2.shape=}, {evt_event.shape=}, {df3.shape=}")
print(f"cols(df3): {sorted(list(df3.columns))}")

# Compare each pair of columns and list the number of mismatched elements as a fraction of the total number of rows
for col_x, col_y in zip(cols_with_x, cols_with_y):
    print(f"{col_x=}, {col_y=}")
    print(f"{df3[col_x].equals(df3[col_y])=}")
    print(f"{(df3[col_x] != df3[col_y]).sum() / df3.shape[0]=}")
    print()

# ----------------------------------------------------------------------

#   LEFT JOIN EVT_EVENT_REL ON
#     FAULT_EVENT.EVENT_DB_ID = EVT_EVENT_REL.REL_EVENT_DB_ID AND
#     FAULT_EVENT.EVENT_ID    = EVT_EVENT_REL.REL_EVENT_ID AND
#     REL_TYPE_CD = 'DISCF'

# What is DISCF?  <<<<<<<

df4 = df3.merge(
    evt_event_rel[evt_event_rel["REL_TYPE_CD"] == "DISCF"],
    left_on=["EVENT_DB_ID", "EVENT_ID"],
    right_on=["REL_EVENT_DB_ID", "REL_EVENT_ID"],
    how="left",
    suffixes=("", "_EVT_REL"),
)

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
