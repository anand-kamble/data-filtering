import pandas as pd

def memory_use(df, msg):
    memory_bytes = df.memory_usage().sum()
    memory_mb = memory_bytes / 1024 / 1024
    print(f"({msg}): DataFrame memory usage: {memory_mb:.2f} MB")

BASE = "../../copa_parquet/"
eqp_assmbl_bom = pd.read_parquet(BASE + "eqp_assmbl_bom.parquet")
df = pd.read_parquet("merge17.parquet")
memory_use(df, "df")
memory_use(eqp_assmbl_bom, "eqp_assmbl_bom")

df1 = df.iloc[:, 0:5]
df1['ASSMBL_DB_ID'] = df['ASSMBL_DB_ID']
df1['ASSMBL_CD'] = df['ASSMBL_CD']

# Analyze whether the key is unique or not. 
print(f"{df1['ASSMBL_DB_ID'].nunique()=}")
print(f"{df1['ASSMBL_CD'].nunique()=}")
print(f"{eqp_assmbl_bom['ASSMBL_DB_ID'].nunique()=}")
print(f"{eqp_assmbl_bom['ASSMBL_CD'].nunique()=}")
print(f"{df1.shape=}")
print(f"{eqp_assmbl_bom.shape=}")
print(f"==> {df1['ASSMBL_DB_ID'].value_counts()=}")
print(f"==> {df1['ASSMBL_CD'].value_counts()=}")
print(f"==> {eqp_assmbl_bom['ASSMBL_DB_ID'].value_counts()=}")
print(f"==> {eqp_assmbl_bom['ASSMBL_CD'].value_counts()=}")
quit()


print("(merge17) eqp_assmbl_bom.columns= ", sorted(list(eqp_assmbl_bom.columns)))
print("======================================================")
print("(merge17) df.columns= ", sorted(list(df.columns)))

print("-----------------------------------------")
print(f"{len(df1.columns)=}, {len(eqp_assmbl_bom.columns)=}")


# MERGE 18
"""
      INNER JOIN EQP_ASSMBL_BOM ON
        EQP_ASSMBL_BOM.ASSMBL_DB_ID  = EVT_INV.ASSMBL_DB_ID AND
        EQP_ASSMBL_BOM.ASSMBL_CD     = EVT_INV.ASSMBL_CD AND
        EQP_ASSMBL_BOM.ASSMBL_BOM_ID = EVT_INV.ASSMBL_BOM_ID
"""

# df18 = df17.merge(
# Code crashes (kill) REASON UNKNOWN
df = df1.merge(
    eqp_assmbl_bom,
    # These columns are in df13 (should be in inv_ac_reg)
    left_on=["ASSMBL_DB_ID", "ASSMBL_CD"],
    right_on=["ASSMBL_DB_ID", "ASSMBL_CD"],
    how="left",
    suffixes=("", "_eqp_assmbl_bom_18"),
)
print(f"Merge 18 done, {df.shape=}")

