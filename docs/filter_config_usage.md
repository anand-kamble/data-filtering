
# How to create a config file

First we specify our output folder, format and filename. This will used by the program to output the final filtered data.
```json
{
  "output_dir": "copa_output",
  "output_format": "parquet",
  "output_file_name": "filtered.parquet"
}
```

Now we can add a source file which will be processed.   
Source files are added as a array of dictionaries with key `"data_files"`
```json
{
  "output_dir": "copa_output",
  "output_format": "parquet",
  "output_file_name": "filtered.parquet",
  "data_files": []
}
```

The `data_file` will contain dictionaries with keys `"fileName"`, `"fileType"` and `"colOfInterest"`.
```json
{
  "output_dir": "copa_output",
  "output_format": "parquet",
  "output_file_name": "filtered.parquet",
  "data_files": [
    {
      "fileName": "TABLES_ADD_20240515/EVT_INV.csv",
      "fileType": "csv",
      "colOfInterest": ["EVENT_ID"],
      "separator": ","
    },
  ]
}
```
In this example we are instructing the program to extract the column `EVENT_ID` from the `TABLES_ADD_20240515/EVT_INV.csv` file which is a `csv` file and the column are seperated by a `,` comma.

> `separator` key is optional but in this case provided since some columns are using a semicolon `;` as seperator.

The `colOfInterest` is a list so that you can add more column from that file. To add multiple column from one file.  
For example, Here I have added an extra column `ASSMBL_INV_NO_ID`
```json
{
  "output_dir": "copa_output",
  "output_format": "parquet",
  "output_file_name": "filtered.parquet",
  "data_files": [
    {
      "fileName": "TABLES_ADD_20240515/EVT_INV.csv",
      "fileType": "csv",
      "colOfInterest": ["EVENT_ID","ASSMBL_INV_NO_ID"],
      "separator": ","
    },
  ]
}
```

### Processing Multiple files 

Using the same technique we can add multiple files
```json
{
  "output_dir": "copa_output",
  "output_format": "parquet",
  "output_file_name": "filtered.parquet",
  "data_files": [
    {
      "fileName": "TABLES_ADD_20240515/EVT_INV.csv",
      "fileType": "csv",
      "colOfInterest": ["EVENT_ID","ASSMBL_INV_NO_ID"],
      "separator": ","
    },
    {
      "fileName": "copa/FAIL_MODE.csv",
      "fileType": "csv",
      "colOfInterest": ["FAIL_MODE_ID","FAIL_DEFER_CD"],
      "separator": ","
    }
  ]
}
```


# API Documentation


### Overview

The configuration file is written in JSON format and contains settings for output configuration and data file processing.
### Parameters

- **`output_dir`**: Directory where the output file will be saved.
  - **Type**: `string`
  - **Example**: `"copa_output"`

- **`output_file_name`**: Name of the output file.
  - **Type**: `string`
  - **Example**: `"filtered.parquet"`

- **`test_rows`**: Number of rows to process in test mode.
  - **Type**: `int`
  - **Example**: `100`

- **`test_mode`**: Flag to indicate if the process should run in test mode.
  - **Type**: `boolean`
  - **Example**: `true`

- **`data_files`**: List of data files to be processed.
  - **Type**: `array of objects`
  - **Object Properties**:
    - **`fileName`**: Path to the data file.
      - **Type**: `string`
      - **Example**: `"TABLES_ADD_20240515/ISDP LOGBOOK REPORT.csv"`
    - **`fileType`**: Type of the data file (e.g., `csv`).
      - **Type**: `string`
      - **Allowed Values**: `"csv"`, `"parquet"`, `"feather"`, `"pickel"`
      - **Example**: `"csv"`
    - **`colOfInterest`**: List of columns to be extracted from the data file.
      - **Type**: `array of strings`
      - **Example**: `["FLEET", "FLIGHT", "FAULT_FOUND_DATE"]`
    - **`separator`**: Delimiter used in the data file (for CSV files).
      - **Type**: `string`
      - **Example**: `","`
      - **Optional**
    - **`filePathOverwrite`**: Optional file path to overwrite the `fileName` field.
      - **Type**: `string`
      - **Optional**

- **`output_format`**: Format of the output file.
  - **Type**: `string`
  - **Allowed Values**: `"csv"`, `"parquet"`, `"feather"`, `"pickel"`
  - **Example**: `"parquet"`




### Example Configuration File for Adding columns from two tables

```json
{
  "output_dir": "copa_output",
  "output_format": "parquet",
  "output_file_name": "filtered.parquet",
  "data_files": [
    {
      "fileName": "TABLES_ADD_20240515/ISDP LOGBOOK REPORT.csv",
      "fileType": "csv",
      "colOfInterest": [
        "FLEET",
        "FLIGHT",
        "FAULT_FOUND_DATE",
        "FAULT_SOURCE",
        "FAULT_NAME",
        "FAULT_SDESC",
        "CORRECTIVE_ACTION",
        "MAINT_DELAY_TIME_QT",
        "ATA",
        "FAULT_SEVERITY"
      ],
      "separator": ","
    },
    {
      "fileName": "TABLES_ADD_20240515/EVT_INV.csv",
      "fileType": "csv",
      "colOfInterest": ["EVENT_ID"],
      "separator": ","
    }
  ]
}
```

### Example Configuration File for Adding columns from Three tables

```json
{
  "output_dir": "copa_output",
  "output_format": "parquet",
  "output_file_name": "filtered.parquet",
  "data_files": [
    {
      "fileName": "TABLES_ADD_20240515/ISDP LOGBOOK REPORT.csv",
      "fileType": "csv",
      "colOfInterest": [
        "FLEET",
        "FLIGHT",
        "FAULT_FOUND_DATE",
        "FAULT_SOURCE",
        "FAULT_NAME",
        "FAULT_SDESC",
        "CORRECTIVE_ACTION",
        "MAINT_DELAY_TIME_QT",
        "ATA",
        "FAULT_SEVERITY"
      ],
      "separator": ","
    },
    {
      "fileName": "TABLES_ADD_20240515/EVT_INV.csv",
      "fileType": "csv",
      "colOfInterest": ["EVENT_ID"],
      "separator": ","
    },
    {
      "fileName": "copa/FAIL_MODE.csv",
      "fileType": "csv",
      "colOfInterest": ["FAIL_MODE_ID"],
      "separator": ","
    }
  ]
}
```