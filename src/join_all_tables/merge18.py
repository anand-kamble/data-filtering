import pandas as pd

BASE = "../../copa_parquet/"
eqp_assmbl_bom = pd.read_parquet(BASE + "eqp_assmbl_bom.parquet")
df = pd.read_parquet("merge17.parquet")

print("(merge17) eqp_assmbl_bom.columns= ", list(df.columns))
print("(merge17) df.columns= ", list(df.columns))

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
