# Testing Table Joining


The source code can be found in the file: **[src/test_joining.py](../src/test_joining.py)**
## Code Structure and Functionality

### Data Import and Initialization

```python
# %%
import pandas as pd

# %%

tables = [
    "INV_LOC",
    "REF_FAIL_CATGRY",
    "REF_FAIL_CATGRY",
    "INV_LOC",
    "REF_FLIGHT_STAGE",
    "REF_FAIL_SEV",
    "REF_FAIL_PRIORITY",
    "REF_FAULT_SOURCE",
    "REF_FLIGHT_STAGE",
    "REF_FAIL_CATGRY",
    "SD_FAULT",
    "REF_FAULT_LOG_TYPE",
    "EVT_EVENT",
    "INV_AC_REG",
]

df: dict[str, pd.DataFrame] = dict()

for table in tables:
    df[table] = pd.read_parquet(f"../copa_parquet/{table}.parquet")
```

- **Purpose**: This block imports the `pandas` library and initializes a list of table names.
- **Functionality**: It reads data from Parquet files and stores each table in a dictionary of DataFrames.

### Display DataFrame Shapes
Print the shape of the dataframes
Here I will be prioritizing the tables with the most number of rows

```python
print("-" * 40, "\nDataframes shape: ")
for d in df:
    print(d, df[d].shape)
```

- **Purpose**: This block prints the shape of each DataFrame to understand the size of the data.
- **Functionality**: It helps identify which tables have the most rows, guiding which ones to prioritize for further analysis.

### Identifying Common Columns
The tables with the most number of rows are: INV_LOC, SD_FAULT, EVT_EVENT
After looking at the shapes of the dataframes, I will be joining the tables with the most number of rows

```python
df1 = df["INV_LOC"]
df2 = df["SD_FAULT"]
df3 = df["EVT_EVENT"]

# Finding the common columns between the three tables
common_columns = (
    set(df1.columns).intersection(set(df2.columns)).intersection(set(df3.columns))
)
print("-" * 40 + "\n")
print("Common columns in df1, df2, and df3: ", common_columns)
```

- **Purpose**: This block identifies columns that are common across the largest DataFrames.
- **Functionality**: It determines which columns could potentially be used as keys for merging the tables.

### Analyzing Common Columns
Let us find the number of common values in ALT_ID in the given table.
This will help us to determine the number of rows that will be merged

```python

for col in common_columns:
    print("-" * 40 + "\n")
    print(f"Checking the column: {col}")
    print("Number of unique values in the column in df1: ", df1[col].nunique())
    print("Number of unique values in the column in df2: ", df2[col].nunique())
    print("Number of unique values in the column in df3: ", df3[col].nunique())
    print(
        f"Number of {col} in df1 that are also in df2: ",
        df1[col].isin(df2[col]).sum(),
    )
    print(
        f"Number of {col} in df1 that are also in df3: ",
        df1[col].isin(df3[col]).sum(),
    )
    print(
        f"Number of {col} in df2 that are also in df3: ",
        df2[col].isin(df3[col]).sum(),
    )
```

- **Purpose**: This block analyzes the common columns to evaluate their suitability as merge keys.
- **Functionality**: It checks how unique the values are in each column and how much they overlap between the DataFrames.

### Conclusion

The script effectively identifies common columns among large DataFrames and assesses their potential as keys for merging. The analysis concludes that while `ALT_ID` is unique within tables, it lacks sufficient overlap across all tables to be a useful merge key. Other columns like `RSTAT_CD`, `REVISION_DT`, and `CREATION_DT` are not ideal due to non-uniqueness or being timestamps, making them unsuitable for merging. Therefore, a more suitable merging column needs to be identified or created for effective data integration.
