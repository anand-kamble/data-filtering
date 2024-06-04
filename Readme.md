## Use of LLM to improve plane maintenance

# Data Processor


### Parameters

- `config` (`data_config` or `None`): The configuration object containing parameters for data processing. If `None`, a `CopaError` will be raised.
- `base_path` (`str`, optional): The base path where the data is located relative to the `run.sh` script. Defaults to an empty string, which means the data is located in the current directory.
- `test_mode` (`bool`, optional): If `True`, the `DataProcessor` will run in test mode. Defaults to `False`.
- `test_rows` (`int`, optional): The number of rows to process in test mode. Defaults to `100`.
- `drop_duplicates` (`bool`, optional): If `True`, duplicate rows will be dropped during data processing. Defaults to `False`.
- `no_cache` (`bool`, optional): If `True`, caching will be disabled during data processing. Defaults to `False`.



## Installation

You'll need poetry to create the python virtual environment to run this project.  
> Here you can find how to install poetry: [https://python-poetry.org/docs/](https://python-poetry.org/docs/)

Once you have installed poetry, follow these steps to generate the filtered data.

1. `poetry install` - To install all the required dependencies.

2. `poetry run ./run.sh` - Run the program.
   > To run the tests please use
   > `poetry run ./run.sh --test`
