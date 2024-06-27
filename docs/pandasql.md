# Experiment Report: Using Pandas and SQL

## Installation and Setup
I installed the `pandasql` package through as it raised errors when I tried to install it through poetry.  
`pip install pandasql` <- installed it using this command.

Below is the error I got when using poetry:

```bash
failed to query /usr/bin/python3 with code 1 err: '  File "/usr/local/poetry/venv/lib64/python3.11/site-packages/virtualenv/discovery/py_info.py", line 7\n    from __future__ import annotations\n    ^\nSyntaxError: future feature annotations is not defined\n'
```

## Data Loading

I am loading all the required tables using the `read_csv` function rather than using the `read_parquet` is because `read_csv` function allows us to specify the `nrow` argument, so that it is easy to quickly load and test the tables.

## SQL Query Adjustment
After that I copied the SQL query in a new file named `src/pandasql_joining/ISDP LOGBOOK REPORT_query.sql` where I commented the parts which were raising the errors. 
And after going through all the line in the SQL query, I found that there are some table mentioned in the SQL query which are not present in the folders which we have.

Below is the list of tables which I could not find:
 - `DEP`  
 - `ACFT_II_NEW`
 - `ACFT_IAR_NEW`
 - `INV_INV`
 - `FLIGHT`
 - `ARR`

### Information required about a SQL function
Also, we don't know how the `TO_CHAR` function from the SQL works in order to recreate it in the python script.


## Execution and Results
You can run the code by using the included bash script.
```bash
. ./pandasql_merge.sh
```

And the expected output is:

```
Data folder './copa' found.
Data folder './TABLES_ADD_20240515' found.
/home/amk23j/copa/data-filtering/src/pandasql_joining/main.py:49: ParserWarning: Skipping line 20: expected 1 fields, saw 3
Skipping line 27: expected 1 fields, saw 4
Skipping line 64: expected 1 fields, saw 3
Skipping line 65: expected 1 fields, saw 2
Skipping line 74: expected 1 fields, saw 3

  REF_EVENT_STATUS = pd.read_csv(OLDER_BASE_PATH + "REF_EVENT_STATUS.csv",nrows=100,encoding="latin1",on_bad_lines="warn")
==================================================
Merged Data:
Empty DataFrame
Columns: [FLEET, CORR_BARCODE, FAULT_FOUND_DATE, FAULT_SOURCE, LOGBOOK_TYPE, WRK_PKG_LOC, FAULT_NAME, FAULT_SDESC, CORRECTIVE_ACTION, MAINT_DELAY_TIME_QT, TASK_NAME, TASK_BARCODE, COMPLETION_DT, ACTION_DT, ATA, LOGPAGE, WORK_PKG_BARCODE, FAULT_SEVERITY, DISRUPTION_ID]
Index: []
```

## Results

After commenting the SQL query for missing tables, the merge resulted in the a table with 0 (zero) columns.