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
The tables with the most number of rows are: `INV_LOC`, `SD_FAULT`, `EVT_EVENT`  
After looking at the shapes of the dataframes, I will be choosing the tables with the most number of rows

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
For the tables to be merged, we need values which are common in all the tables.
For example, if we select `ALT_ID` as the common column, we need the values of `ALT_ID` to be unique in all the rows, but these
values should be common in all the tables. So that we can make one row from the three tables.

In the above code, we have checked the number of unique values in the column and the number of values for a column which are 
same in the other tables. The ideal merging column will be the one that has the same number of unique values in all the tables
and all the values are present in all the tables.

After looking at the results, we can see that `RSTAT_CD`, `REVISION_DT`, `CREATION_DT`, `ALT_ID` columns are shared between all the three tables.
We cannot use `RSTAT_CD` as the merging column because it does not have unique values.
`REVISION_DT` and `CREATION_DT` are also not ideal because they are timestamps and they are not unique. (There can be multiple rows with the same date)

`ALT_ID` has unique values but the values are not common in all the tables. 

## Expected Output

After running the script by `python test_joining.py` you should get this output.

```
---------------------------------------- 
Dataframes shape: 
INV_LOC (32947, 42)
REF_FAIL_CATGRY (2, 10)
REF_FLIGHT_STAGE (5, 10)
REF_FAIL_SEV (23, 13)
REF_FAIL_PRIORITY (6, 13)
REF_FAULT_SOURCE (6, 11)
SD_FAULT (1126756, 40)
REF_FAULT_LOG_TYPE (6, 9)
EVT_EVENT (9728446, 45)
INV_AC_REG (135, 28)
----------------------------------------

Common columns in df1, df2, and df3:  {'RSTAT_CD', 'REVISION_DT', 'CREATION_DT', 'ALT_ID'}
----------------------------------------

Checking the column: RSTAT_CD
Number of unique values in the column in df1:  2
Number of unique values in the column in df2:  1
Number of unique values in the column in df3:  1
Number of RSTAT_CD in df1 that are also in df2:  32944
Number of RSTAT_CD in df1 that are also in df3:  32944
Number of RSTAT_CD in df2 that are also in df3:  1126756
----------------------------------------

Checking the column: REVISION_DT
Number of unique values in the column in df1:  588
Number of unique values in the column in df2:  2377
Number of unique values in the column in df3:  2397
Number of REVISION_DT in df1 that are also in df2:  23438
Number of REVISION_DT in df1 that are also in df3:  23438
Number of REVISION_DT in df2 that are also in df3:  1126756
----------------------------------------

Checking the column: CREATION_DT
Number of unique values in the column in df1:  1117
Number of unique values in the column in df2:  2377
Number of unique values in the column in df3:  2395
Number of CREATION_DT in df1 that are also in df2:  5039
Number of CREATION_DT in df1 that are also in df3:  5039
Number of CREATION_DT in df2 that are also in df3:  1126756
----------------------------------------

Checking the column: ALT_ID
Number of unique values in the column in df1:  32947
Number of unique values in the column in df2:  1126756
Number of unique values in the column in df3:  9728446
Number of ALT_ID in df1 that are also in df2:  0
Number of ALT_ID in df1 that are also in df3:  0
Number of ALT_ID in df2 that are also in df3:  0
```
