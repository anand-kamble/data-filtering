6th May 2024
<hr>

Below are the variables which might be useful.

| FILENAME | VAR_NAME | Desc | 
| :------  | :------  | :--- |
| SD_FAULT_REFERENCE.csv |  NOTES | Notes describing problem in short |  
| SD_FAULT_REFERENCE.csv |  CREATION_DT | Might be useful for sorting |
| FAIL_DEFER_REF.csv | PERF_PENALTIES_LDESC | Affects of the fault |
| EVT_EVENT | EVENT_LDESC | A long description for the event | 
| INV_LOC | -- | This table includes data about the inventory locations. | 
| REQ_PART | REQ_QT | The number of parts that you are requesting. |
| REQ_PART | REQ_BY_DT | This is the date when the request is needed. |
| REQ_PART | EST_ARRIVAL_DT | Estimated arrival date of the requested inventory (local time) | 
| REQ_PART | REQ_NOTE | Any notes that you may want to mention about the requested part. | 
| SCHED_STASK | TASK_PRIORITY_CD | Indicates the priority of a task as given by the line contoller/supervisor. | 
| SCHED_STASK | MAIN_INV_NO_ID | The main inventory of the task. | 
| SCHED_STASK | ROUTINE_BOOL | Specifies whether this task is considered "routine" work or not. The routine flag is typically used for reporting purposes. | 
| SCHED_STASK | INSTRUCTION_LDESC | This column is used to record the instructions for a task. This column will be copied from the baseline task definition. | 
| SCHED_STASK | EST_DURATION_QT | This is an estimated duration of the task. | 
| REF_FAIL_SEV | FAIL_SEV_ORD | The order for the severity
| FL_LEG | -- | (Entity to store flight information) Info about the flight, includes aircraft id, flight number, with arrival & departure.

* `--` in VAR_NAME column means the Desc is about the whole table.

### Converting dates
Converting the dates into pandas timestamp will allow us to work with data more efficiently. It is using 64-bit int. [Github src](https://github.com/pandas-dev/pandas/blob/main/pandas/_libs/tslibs/timestamps.pyx)  
[Perplexity](https://www.perplexity.ai/search/Python-libraries-to-5Aqe2NnkSYSBNdWMrAIR6Q)


## Execution 
 
`python src/main.py`  
To run the program you will need the pandas package installed. `pip install pandas`
> I have installed the pandas in a conda enviroment so it won't interefer with other packages.

