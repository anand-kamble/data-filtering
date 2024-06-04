## Use of LLM to Improve Plane Maintenance

# Data Processor

This project leverages a Language Model (LLM) to enhance the efficiency and accuracy of plane maintenance by processing and analyzing relevant data.

## Parameters

- `config` (`data_config` or `None`): The configuration object containing parameters for data processing. If `None`, a `CopaError` will be raised.
- `base_path` (`str`, optional): The base path where the data is located relative to the `run.sh` script. Defaults to an empty string, which means the data is located in the current directory.
- `test_mode` (`bool`, optional): If `True`, the `DataProcessor` will run in test mode. Defaults to `False`.
- `test_rows` (`int`, optional): The number of rows to process in test mode. Defaults to `100`.
- `drop_duplicates` (`bool`, optional): If `True`, duplicate rows will be dropped during data processing. Defaults to `False`.
- `no_cache` (`bool`, optional): If `True`, caching will be disabled during data processing. Defaults to `False`.

## Installation

To set up the Python virtual environment for this project, you'll need Poetry. Poetry is a tool for dependency management and packaging in Python. Follow the steps below to install Poetry and run the project:

### Step 1: Install Poetry

You can find instructions for installing Poetry [here](https://python-poetry.org/docs/).

### Step 2: Set Up the Environment

Once Poetry is installed, follow these steps to generate the filtered data:

1. **Install Dependencies:**
    ```sh
    poetry install
    ```

2. **Activate the Virtual Environment:**
    ```sh
    poetry shell
    ```

3. **Install Additional Tools (First-Time Setup Only):**
    Inside the shell, run:
    ```sh
    pip install -U commitizen
    ```

4. **Run the Data Processor:**
    ```sh
    python src/main.py
    ```
    Alternatively, if you have exited the Poetry shell, you can run:
    ```sh
    poetry run python src/main.py
    ```

## Running the Script

The `run.sh` script is designed to execute the data processing with default parameters or with user-specified parameters.

### Default Execution

If no arguments are provided, the script will use the default parameters specified in the script.

### Custom Execution

You can provide custom parameters when running the script. For example:
```sh
./run.sh --config "custom_config.json" --test_mode --test_rows 500
```

This flexibility allows you to tailor the data processing to your specific needs.
