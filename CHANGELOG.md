## 1.1.0 (2024-06-04)

### Feat

- **main**: added cli arguments
- generates categorized data based on ATA code
- **filter_configs**: added a filter for descriptions of the event
- **main.py**: updated for new tables with ATA
- **main.py**: updated for new tables with ATA
- generates categorized data based on ATA code
- **filter_configs**: added a filter for descriptions of the event

### Fix

- **tests/**: updated names of test methods
- **tests/**: updated names of test methods
- **.env**: added env file to fix pylance errors
- **main**: added suffix 'merged' to merged ATA csv files
- updated splitting function
- **main**: using set to remove duplicates
- **main**: does not separate ATA for their subclasses
- **main.py**: removed extra imports
- **main.py**: now separates the csv files as per the ATA
- **util.py**: changing the priority of patterns
- **utils.py**: added multiple pattern checks
- **util.py**: updated the regex pattern
- **main.py**: using lists instead of dataframe to ensure that rows are matched
- **data_processor**: detect the change in test_mode
- **main.py**: removed the test mode
- **main.py**: removed extra imports
- **main.py**: now separates the csv files as per the ATA
- **main.py**: using lists instead of dataframe to ensure that rows are matched
- **main.py**: removed the test mode
- **util.py**: changing the priority of patterns
- **utils.py**: added multiple pattern checks
- **util.py**: updated the regex pattern
- **main.py**: using lists instead of dataframe to ensure that rows are matched
- **data_processor**: detect the change in test_mode
- **main.py**: removed the test mode

### Refactor

- **test.py**: using assert from pytest
- cleanup
- **pypoetry.lock**: adding lock file to the repo
- **test.py**: removed extra methods and improved documentation
- **main**: changed base path
- **test.py**: using assert from pytest
- cleanup
- **pypoetry.lock**: adding lock file to the repo
- **test.py**: removed extra methods and improved documentation
- removed the terminal output file
- **trials.py**: adding the trials.py file into git
- removed the terminal output file
- **trials.py**: adding the trials.py file into git

## 1.0.4 (2024-05-18)

### Feat

- **main**: added cli arguments
- generates categorized data based on ATA code
- **filter_configs**: added a filter for descriptions of the event
- **main.py**: updated for new tables with ATA
- **main.py**: updated for new tables with ATA
- generates categorized data based on ATA code
- **filter_configs**: added a filter for descriptions of the event

### Fix

- **.env**: added env file to fix pylance errors
- **tests/**: updated names of test methods
- **.env**: added env file to fix pylance errors
- **main**: added suffix 'merged' to merged ATA csv files
- updated splitting function
- **main**: using set to remove duplicates
- **main**: does not separate ATA for their subclasses
- **main.py**: removed extra imports
- **main.py**: now separates the csv files as per the ATA
- **util.py**: changing the priority of patterns
- **utils.py**: added multiple pattern checks
- **util.py**: updated the regex pattern
- **main.py**: using lists instead of dataframe to ensure that rows are matched
- **data_processor**: detect the change in test_mode
- **main.py**: removed the test mode
- **main.py**: removed extra imports
- **main.py**: now separates the csv files as per the ATA
- **main.py**: using lists instead of dataframe to ensure that rows are matched
- **main.py**: removed the test mode
- **util.py**: changing the priority of patterns
- **utils.py**: added multiple pattern checks
- **util.py**: updated the regex pattern
- **main.py**: using lists instead of dataframe to ensure that rows are matched
- **data_processor**: detect the change in test_mode
- **main.py**: removed the test mode

### Refactor

- **main**: changed base path
- **test.py**: using assert from pytest
- cleanup
- **pypoetry.lock**: adding lock file to the repo
- **test.py**: removed extra methods and improved documentation
- removed the terminal output file
- **trials.py**: adding the trials.py file into git
- removed the terminal output file
- **trials.py**: adding the trials.py file into git

## 1.0.3 (2024-05-11)

### Fix

- **data_processor**: chages in the config file will now cause rebuild of cache
- **data_processor**: now it also stored the config with the output
- **data_processor**: changed the output message while loading files without cache

## 1.0.2 (2024-05-09)

## 2.0.0 (2024-05-09)

### Feat

- **copa_modules**: added support for cache
- **run.sh**: bash script to run the whole project

### Fix

- **pyproject.toml**: fixing the python version for macbooks arm chip

### Refactor

- **data_config.py**: added one more param to data config structure
- **_types**: updated the structure of config used to read the data files

## 1.0.1 (2024-05-08)

### Refactor

- adding the changelog file

## 1.0.0 (2024-05-08)

### Fix

- **poetry**: fixing the bug with cz bump

### Refactor

- **gitignore**: updated the git ignore
- **_types**: removing the typescript files

## 0.2.0 (2024-05-09)

### Feat

- **data_processor**: added fast load option
- Save dataframe function added
- **run.sh**: bash script to run the whole project

### Fix

- **gitignore**: added dataset file formats to git ignore
- **pyproject.toml**: removed voex from dependencies
- **pyproject.toml**: fixing the python version for macbooks arm chip

### Refactor

- removing parquet file

## 0.0.1 (2024-05-08)
