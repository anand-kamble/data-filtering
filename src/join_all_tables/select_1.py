"""
Treatment of SELECT SQL statement
"""

import pandas as pd

"""
        EQP_ASSMBL.ASSMBL_CD AS FLEET,
        ACFT_IAR_NEW.AC_REG_CD,
        INV_INV.SERIAL_NO_OEM,
        CORR_TASK_SS.BARCODE_SDESC AS CORR_BARCODE,
        FAULT_EVENT.ACTUAL_START_DT AS FAULT_FOUND_DATE,
        SD_FAULT.FAULT_SOURCE_CD AS FAULT_SOURCE,
        DEP.LOC_CD AS DEPARTURE_LOCATION,
        sd_fault.fault_log_type_cd AS LOGBOOK_TYPE,
        INV_LOC.LOC_CD AS WRK_PKG_LOC,
        FAULT_EVENT.EVENT_SDESC AS FAULT_NAME,
        FAULT_EVENT.EVENT_LDESC AS FAULT_SDESC,
        SCHED_ACTION.ACTION_LDESC AS CORRECTIVE_ACTION,
        FLIGHT.LEG_NO AS FLIGHT,
        FLIGHT.OFF_DT AS FLIGHT_UP_DT,
        FL_LEG_DISRUPT.MAINT_DELAY_TIME_QT,
        FOUND_ON_EVENT.EVENT_SDESC AS TASK_NAME,
        FOUND_ON_TASK.BARCODE_SDESC AS TASK_BARCODE,
        TASK_EVENT.EVENT_DT AS COMPLETION_DT,
        ARR.LOC_CD AS ARRIVAL_LOCATION,
        FAULT_STATUS_CODE.USER_STATUS_CD AS FAULT_STATUS,
        SCHED_ACTION.ACTION_DT AS ACTION_DT,
        TO_CHAR(SCHED_ACTION.ACTION_DT, 'DD/MM/YYYY HH24:MI:SS') AS "Dt Corrective Action",
        TO_CHAR(SCHED_ACTION.ACTION_DT, 'HH24:MI:SS AM') AS "Corrective Action Time",
        ---SCHED_ACTION.REVISION_USER AS PERFORMED_BY,
        EQP_ASSMBL_BOM.ASSMBL_BOM_CD AS ATA,
FAULT_EVENT.DOC_REF_SDESC AS LOGPAGE,
WORK_PKG_SCHED_STASK.BARCODE_SDESC AS WORK_PKG_BARCODE,
        SD_FAULT.FAIL_SEV_CD AS FAULT_SEVERITY,
        FL_LEG_DISRUPT.DISRUPTION_DESC AS DISRUPTION_ID
"""


# Columns to keep
# INV_AC_REG ==> ACFT_IAR_NEW     # table rename from original table
#     AC_REG_CD,
#
# INV_LOC ==> ARR
#     LOC_CD ==> ARRIVAL_LOCATION,
#
# INV_INV  (not available)
#     SERIAL_NO_OEM,
#
# SCHED_STASK ==> CORR_TASK_SS
#     BARCODE_SDESCu ==> CORR_BARCODE,
#
# INV_LOC ==> DEP
#     LOC_CD ==> DEPARTURE_LOCATION,
#
# EQP_ASSMBL
#     ASSMBL_CD ==> FLEET,
#
# EQP_ASSMBL_BOM
#     ASSMBL_BOM_CD ==> ATA,
#
# EVT_EVENT ==> FAULT_EVENT
#     ACTUAL_START_DT ==> FAULT_FOUND_DATE,
#     EVENT_SDESC ==> FAULT_NAME,
#     EVENT_LDESC ==> FAULT_SDESC,
#     DOC_REF_SDESC ==> LOGPAGE,
#
# REF_EVENT_STATUS ==> FAULT_STATUS_CODE
#         USER_STATUS_CD ==>  FAULT_STATUS,
#
# FL_LEG ==> FLIGHT
#     LEG_NO ==>  FLIGHT,
#     OFF_DT ==> FLIGHT_UP_DT,
#
# FL_LEG_DISRUPT
#     MAINT_DELAY_TIME_QT,
#     DISRUPTION_DESC ==>  DISRUPTION_ID
#
# EVT_EVENT ==> FOUND_ON_EVENT
#     EVENT_SDESC ==> TASK_NAME,
#     BARCODE_SDESC ==> TASK_BARCODE,
#
# INV_LOC
#     LOC_CD ==> WRK_PKG_LOC,
#
# SCHED_ACTION
#     ACTION_LDESC ==> CORRECTIVE_ACTION,
#     ACTION_DT ==> ACTION_DT,
#
# SD_FAULT
#     FAULT_SOURCE_CD ==> FAULT_SOURCE,
#     FAIL_SEV_CD ==> FAULT_SEVERITY,
#     FAULT_LOG_TYPE_CD ==> LOGBOOK_TYPE,
#
# EVT_EVENT ==> TASK_EVENT
#     EVENT_DT ==> COMPLETION_DT,
#
# EVT_EVENT ==> WORK_PKG_SCHED_STASK
#     BARCODE_SDESC ==> WORK_PKG_BARCODE,
#
# SCHED_ACTION
#     ACTION_DT ==> ACTION_DT
#
#         --- convert SCHED_ACTION.ACTION_DT to two different columns
#         SCHED_ACTION.ACTION_DT AS "DT Corrective Action",
#
#

# Collect the columns to keep in a dict[old_table_name] = list[col_names]
tables = {}
tables["inv_ac_reg"] = ["AC_REG_CD"]
tables["inv_loc"] = ["ARRIVAL_LOCATION", "LOC_CD"]
# tables["inv_inv"] = ["SERIAL_NO_OEM"]  # table not available
tables["sched_stask"] = ["BARCODE_SDESC"]
tables["sched_action"] = ["ACTION_LDESC", "ACTION_DT"]
tables["sd_fault"] = ["FAULT_SOURCE_CD", "FAIL_SEV_CD", "FAULT_LOG_TYPE_CD"]
tables["eqp_assmbl"] = ["ASSMBL_CD"]
tables["eqp_assmbl_bom"] = ["ASSMBL_BOM_CD"]
tables["evt_loc"] = []
tables["evt_event"] = [
    "ACTUAL_START_DT",
    "EVENT_SDESC",
    "EVENT_LDESC",
    "DOC_REF_SDESC",
    "BARCODE_SDESC",
    "EVENT_DT",
]
tables["evt_event_rel"] = []
tables["evt_inv"] = []
tables["ref_event_status"] = ["USER_STATUS_CD"]
tables["fl_leg"] = ["LEG_NO", "OFF_DT"]
tables["fl_leg_disrupt"] = ["MAINT_DELAY_TIME_QT", "DISRUPTION_DESC"]


# ----------------------------------------------------------------------
# %%
def select_existing_columns(df: pd.DataFrame, columns: list[str]):
    """
    Select columns from a DataFrame, ignoring non-existent columns.

    Parameters:
    df (pandas.DataFrame): The input DataFrame
    columns (list): List of column names to select

    Returns:
    pandas.DataFrame: A new DataFrame with only the existing columns
    """
    existing_columns = [col for col in columns if col in df.columns]
    return df[existing_columns]


# ----------------------------------------------------------------------
# %%

cols = [
    "ASSMBL_CD",
    "ASSMBL_DB_ID",
    "EVENT_ID",
    "EVENT_DB_ID",
    "EVENT_STATUS_CD",
    "EVENT_STATUS_DB_ID",
    "FAULT_ID",
    "FAULT_DB_ID",
    "H_EVENT_ID",
    "H_EVENT_DB_ID",
    "H_INV_NO_DB_ID",
    "H_INV_NO_ID",
    "INV_NO_ID",
    "INV_NO_DB_ID",
    "LEG_ID",
    "LOC_ID",
    "LOC_DB_ID",
    "MAIN_INV_NO_ID",
    "MAIN_INV_NO_DB_ID",
    "REL_TYPE_CD",
    "REL_EVENT_ID",
    "REL_EVENT_DB_ID",
    "SCHED_ID",
    "SCHED_DB_ID",
    "TASK_CLASS_CD",
]

"""
Table: INV_AC_REG: ["INV_NO_DB_ID", "INV_NO_ID"]
"INV_NO_DB_ID","INV_NO_ID","INV_OPER_DB_ID","INV_OPER_CD","REG_BODY_DB_ID","REG_BODY_CD","INV_CAPABILITY_DB_ID","INV_CAPABILITY_CD","COUNTRY_DB_ID","COUNTRY_CD","AC_REG_CD","AIRWORTH_CD","PRIVATE_BOOL","VAR_NO_OEM","LINE_NO_OEM","FIN_NO_CD","RSTAT_CD","CREATION_DT","REVISION_DT","FORECAST_MODEL_DB_ID","FORECAST_MODEL_ID","PREVENT_LPA_BOOL","ISSUE_ACCOUNT_DB_ID","ISSUE_ACCOUNT_ID","LIC_DB_ID","LIC_ID","ETOPS_BOOL","INV_OPER_CHANGE_REASON"
"""

print("cols= ", cols)
for table, vcols in tables.items():
    tables[table] = vcols + cols

for table, vcols in tables.items():
    print(f"table: {table}, vcols= ", vcols)


# ----------------------------------------------------------------------
# %%
BASE = "../../copa_parquet/"
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
table_objs = {}
table_objs["inv_ac_reg"] = inv_ac_reg
table_objs["inv_loc"] = inv_loc
# table_objs["inv_inv"] = inv_inv  # not defined
table_objs["sched_stask"] = sched_stask
table_objs["sched_action"] = sched_action
table_objs["sd_fault"] = sd_fault
table_objs["eqp_assmbl"] = eqp_assmbl
table_objs["eqp_assmbl_bom"] = eqp_assmbl_bom
table_objs["evt_event"] = evt_event
table_objs["evt_event_rel"] = evt_event_rel
table_objs["evt_loc"] = evt_loc
table_objs["evt_inv"] = evt_inv
table_objs["ref_event_status"] = ref_event_status
table_objs["fl_leg"] = fl_leg
table_objs["fl_leg_disrupt"] = fl_leg_disrupt


def reset_tables():
    """Reset tables based on table_objs dict"""
    global inv_ac_reg, inv_loc, sched_stask, sched_action, sd_fault
    global eqp_assmbl, eqp_assmbl_bom, evt_event, ref_event_status
    global fl_leg, fl_leg_disrupt, evt_event_rel, evt_inv

    inv_ac_reg = table_objs["inv_ac_reg"]
    inv_loc = table_objs["inv_loc"]
    evt_inv = table_objs["evt_inv"]
    sched_stask = table_objs["sched_stask"]
    sched_action = table_objs["sched_action"]
    sd_fault = table_objs["sd_fault"]
    eqp_assmbl = table_objs["eqp_assmbl"]
    eqp_assmbl_bom = table_objs["eqp_assmbl_bom"]
    evt_event = table_objs["evt_event"]
    evt_event_rel = table_objs["evt_event_rel"]
    ref_event_status = table_objs["ref_event_status"]
    fl_leg = table_objs["fl_leg"]
    fl_leg_disrupt = table_objs["fl_leg_disrupt"]


# ----------------------------------------------------------------------
# %%

"""
inv_ac_reg.csv
"INV_NO_DB_ID", "INV_NO_ID", "INV_OPER_DB_ID", "INV_OPER_CD", "REG_BODY_DB_ID", "REG_BODY_CD", "INV_CAPABILITY_DB_ID", "INV_CAPABILITY_CD", "COUNTRY_DB_ID", "COUNTRY_CD", "AC_REG_CD", "AIRWORTH_CD", "PRIVATE_BOOL", "VAR_N    O_OEM", "LINE_NO_OEM", "FIN_NO_CD", "RSTAT_CD", "CREATION_DT", "REVISION_DT", "FORECAST_MODEL_DB_ID", "FORECAST_MODEL_ID", "PREVENT_LPA_BOOL", "ISSUE_ACCOUNT_DB_ID", "ISSUE_ACCOUNT_ID", "LIC_DB_ID", "LIC_ID", "ETOPS_BOOL", "INV_OPER_CHANGE_REASON"
"""

# Only keep the columns in tables
for name, df in table_objs.items():
    tables[name] = list(set(tables[name]))  # remove duplicates if any
    cols = tables[name]
    table_objs[name] = select_existing_columns(df, cols)
    print("\n=====================================")
    print(f"TABLE NAME: {name}")
    print("cols to check= ", sorted(cols))
    print("original table columns: ", sorted(df.columns))
    print(f"{name}: nb cols: ", len(table_objs[name].columns))
    print("Columns found in table: ", sorted(table_objs[name].columns))
    print("Nb columns found in table: ", len(table_objs[name].columns))
print("=====================================")

print(f"{len(inv_ac_reg.columns)=}")
print(f"{len(table_objs["inv_ac_reg"].columns)=}")
inv_ac_reg = table_objs["inv_ac_reg"]

reset_tables()
print(f"{len(inv_ac_reg.columns)=}")

# Check that all columns above are available
for table, cols in tables.items():
    df = table_objs[table]
    columns = df.columns
    print("nb columns: ", len(columns))
    # for col in cols:
    # if col not in columns:
    # print(f"Column {col} not in table {table}")

# %%
# Write out the tables to parquet files, which will be read by the merging code

for table_name, df in table_objs.items():
    df.to_parquet(f"parquet/{table_name}.parquet", index=False)

# ----------------------------------------------------------------------
# Columns  that don't exist:
# Column ARRIVAL_LOCATION not in table inv_loc
# Column BARCODE_SDESC not in table evt_event


quit()


# Columns to keep
# INV_AC_REG ==> ACFT_IAR_NEW     # table rename from original table
#     AC_REG_CD,
#
# INV_LOC ==> ARR
#     LOC_CD ==> ARRIVAL_LOCATION,
#
# INV_INV  (not available)
#     SERIAL_NO_OEM,
#
# SCHED_STASK ==> CORR_TASK_SS
#     BARCODE_SDESC ==> CORR_BARCODE,
#
# INV_LOC ==> DEP
#     LOC_CD ==> DEPARTURE_LOCATION,
#
# EQP_ASSMBL
#     ASSMBL_CD ==> FLEET,
#
# EQP_ASSMBL_BOM
#     ASSMBL_BOM_CD ==> ATA,
#
# EVT_EVENT ==> FAULT_EVENT
#     ACTUAL_START_DT ==> FAULT_FOUND_DATE,
#     EVENT_SDESC ==> FAULT_NAME,
#     EVENT_LDESC ==> FAULT_SDESC,
#     DOC_REF_SDESC ==> LOGPAGE,
#
# REF_EVENT_STATUS ==> FAULT_STATUS_CODE
#         USER_STATUS_CD ==>  FAULT_STATUS,
#
# FL_LEG ==> FLIGHT
#     LEG_NO ==>  FLIGHT,
#     OFF_DT ==> FLIGHT_UP_DT,
#
# FL_LEG_DISRUPT
#     MAINT_DELAY_TIME_QT,
#     DISRUPTION_DESC ==>  DISRUPTION_ID
#
# EVT_EVENT ==> FOUND_ON_EVENT
#     EVENT_SDESC ==> TASK_NAME,
#     BARCODE_SDESC ==> TASK_BARCODE,
#
# INV_LOC
#     LOC_CD ==> WRK_PKG_LOC,
#
# SCHED_ACTION
#     ACTION_LDESC ==> CORRECTIVE_ACTION,
#     ACTION_DT ==> ACTION_DT,
#
# SD_FAULT
#     FAULT_SOURCE_CD ==> FAULT_SOURCE,
#     FAIL_SEV_CD ==> FAULT_SEVERITY,
#     fault_log_type_cd ==> LOGBOOK_TYPE,
#
# EVT_EVENT ==> TASK_EVENT
#     EVENT_DT ==> COMPLETION_DT,
#
# EVT_EVENT ==> WORK_PKG_SCHED_STASK
#     BARCODE_SDESC ==> WORK_PKG_BARCODE,
#
# SCHED_ACTION
#     ACTION_DT ==> ACTION_DT
#
#         --- convert SCHED_ACTION.ACTION_DT to two different columns
#         SCHED_ACTION.ACTION_DT AS "DT Corrective Action",
#
#

# Collect the columns to keep in a dict[old_table_name] = list[col_names]
cols = {}
cols[""]
SELECT


# Column ARRIVAL_LOCATION not in table inv_loc
# Column BARCODE_SDESC not in table evt_event
