## Installation Guide for LLM Plane Maintenance Data Processor

This guide provides step-by-step instructions to set up and run the Data Processor project, which leverages a Language Model (LLM) to enhance the efficiency and accuracy of plane maintenance by processing and analyzing relevant data.

### Prerequisites

Before you begin, ensure you have the following:

- Python 3.11 or 3.12
- Poetry (a tool for dependency management and packaging in Python)

### Step 1: Install Poetry

If you haven't already installed Poetry, you can find the instructions [here](https://python-poetry.org/docs/).

### Step 2: Clone the Repository

Clone the repository to your local machine using the following command:

```sh
git clone git@github.com:anand-kamble/data-filtering.git
cd data-filtering
```


### Step 3: Set Up the Environment

Once you have Poetry installed and the repository cloned, follow these steps to set up the environment and generate the filtered data:

#### 3.1 Install Dependencies

Run the following command to install all necessary dependencies:

```sh
poetry install
```

#### 3.2 Activate the Virtual Environment

Activate the virtual environment created by Poetry:

```sh
poetry shell
```

#### 3.3 Install Additional Tools (First-Time Setup Only)

This step is required only for development purposes. You can skip this step if you do not plan to contribute to the repository.

```sh
pip install -U commitizen
```

### Step 4: Run the Data Processor

You can run the data processor using the `run.sh` script. There are two ways to execute this script:

#### Default Execution

If no arguments are provided, the script will use the default parameters specified in the script:

```sh
./run.sh
```

Alternatively, if you have exited the Poetry shell, you can run:

```sh
poetry run ./run.sh
```

#### Custom Execution

You can provide custom parameters when running the script. For example, to specify a custom configuration file, enable test mode, and set the number of rows to process in test mode, use:

```sh
./run.sh --config "custom_config.json" --test_mode --test_rows 500
```

### Parameters

- `--config` (`data_config` or `None`): The configuration object containing parameters for data processing. If `None`, a `CopaError` will be raised.
- `--base_path` (`str`, optional): The base path where the data is located relative to the `run.sh` script. Defaults to an empty string, which means the data is located in the current directory.
- `--test_mode` (`bool`, optional): If `True`, the `DataProcessor` will run in test mode. Defaults to `False`.
- `--test_rows` (`int`, optional): The number of rows to process in test mode. Defaults to `100`.
- `--drop_duplicates` (`bool`, optional): If `True`, duplicate rows will be dropped during data processing. Defaults to `False`.
- `--no_cache` (`bool`, optional): If `True`, caching will be disabled during data processing. Defaults to `False`.

### Notes

- Ensure that your data files are placed correctly in the directory specified by `base_path` (if provided).

This guide should help you get started with setting up and running the Data Processor project. If you encounter any issues or have any questions, please  reach out to the maintainers.