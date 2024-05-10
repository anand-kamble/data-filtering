import os
from typing import Dict, List, Union

import pandas as pd
from halo import Halo

from .._error import CopaError
from .._logger import Logger
from .._types import data_config, dataset, file_types


class data_processor:
    """
    The DataProcessor class is responsible for processing data based on a given configuration.

    Attributes:
        config (DataConfig): The configuration object containing parameters for data processing.
        base_path (str): The base path where the data is located.
        data (Dataset): The dataset object that will hold the processed data, initialized as an empty dictionary.
    """

    def __init__(
        self,
        config: data_config | None,
        base_path: str = "",
        test_mode: bool = False,
        test_rows: int = 100,
        drop_duplicates: bool = False,
        no_cache: bool = False,
    ):
        """
        Initializes the DataProcessor object with the provided configuration and options.

        Args:
            config (DataConfig | None): The configuration object containing parameters for data processing.
                                        If None, a CopaError will be raised.
            base_path (str, optional): The base path where the data is located. Defaults to an empty string,
                                        which means the data is located in the current directory.
            test_mode (bool, optional): If True, the DataProcessor will run in test mode. Defaults to False.
            test_rows (int, optional): The number of rows to process in test mode. Defaults to 100.
            drop_duplicates (bool, optional): If True, duplicate rows will be dropped during data processing. Defaults to False.
            no_cache (bool, optional): If True, caching will be disabled during data processing. Defaults to False.

        Raises:
            CopaError: If the config parameter is not provided.

        Note:
            The DataProcessor uses a Logger object for logging its activities. The logger is initialized during the creation of the DataProcessor object.
        """
        self.base_path: str = base_path
        if not config:
            raise CopaError("No configuration provided.")
        self.config: data_config = config
        self.test_rows = test_rows
        self.test_mode = test_mode
        self.drop_duplicates = drop_duplicates
        self.no_cache = no_cache

        self.logger: Logger = Logger("DataProcessor")
        test_log_msg = (
            "Running in test mode." if test_mode else "Running in normal mode."
        )
        self.logger.log(
            f"Initializing DataProcessor { 'in test mode.' if test_mode else 'Running in normal mode.'} with base_path: {base_path}"
        )

    def load(self) -> pd.DataFrame:
        if self.config:
            # print(data_config.__annotations__.keys())
            config_diff = set(data_config.__annotations__.keys()).difference(
                self.config.keys()
            )
            self.logger.log(f"Config diff is: {config_diff}")
            if bool(config_diff):
                self.logger.log("Invalid configuration provided.")
                raise CopaError("Invalid configuration provided.")

            # self.config: data_config = config
            if self.no_cache == False and self.__check_filtered_file_exists():
                self.logger.log("Filtered file already exists.", print_message=True)
                spinner = Halo(
                    text="Loading the filtered File...",
                    spinner="dots",
                )
                spinner.start()
                file_path = (
                    self.config["output_dir"] + "/" + self.config["output_file_name"]
                )
                self.__filtered_data = self.__read_dataframe(
                    file_path, self.config["output_format"]
                )
                spinner.succeed("Filtered file loaded successfully.")
                return self.__filtered_data
            else:
                if self.no_cache == False:
                    self.logger.log(
                        "Filtered file does not exist. Generating new one...",
                        print_message=True,
                    )
                else:
                    self.logger.log(
                        "Cache is disabled. Generating new filtered file...",
                        print_message=True,
                    )
                spinner = Halo(
                    text="Loading Files...",
                    spinner="dots",
                )
                spinner.start()
                self.data: dataset = {}
                self.load_files(fast_load=self.test_mode, nrows=self.test_rows)

                spinner.succeed("Files loaded successfully.")
                spinner.text = "Filtering Data..."
                spinner.start()
                self.filter_data(drop_duplicates=self.drop_duplicates)
                spinner.succeed("Data filtered successfully.")
                file_path_to_save = (
                    self.config["output_dir"] + "/" + self.config["output_file_name"]
                )
                if not os.path.exists(self.config["output_dir"]):
                    os.mkdir(self.config["output_dir"])
                self.save_filtered_data(file_path_to_save, self.config["output_format"])
                return self.__filtered_data
        else:
            raise CopaError("No configuration provided.")

    def __check_filtered_file_exists(self):
        """
        Check if the filtered file exists in the specified output directory.

        This method checks if the output directory exists. If it does, it checks if the output file is in the directory.
        If the directory does not exist, it returns False.

        Returns:
            bool: True if the output file exists in the output directory, False otherwise.
        """
        if os.path.exists(self.config["output_dir"]):
            return self.config["output_file_name"] in os.listdir(
                self.config["output_dir"]
            )
        else:
            return False

    def __read_dataframe(
        self, file_path: str, file_type: file_types, **kwargs
    ) -> pd.DataFrame:
        if file_type == "csv":
            return pd.read_csv(file_path, iterator=False, **kwargs)
        elif file_type == "parquet":
            return pd.read_parquet(file_path)
        elif file_type == "feather":
            return pd.read_feather(file_path)
        else:
            raise CopaError(
                f"Invalid file type {file_type} encountered while reading dataframe: {file_path}"
            )

    def update_config(self, config: data_config) -> "data_processor":
        """
        Updates the configuration object for the DataProcessor instance.

        Args:
            config (DataConfig): The new configuration object.

        Returns:
            self (DataProcessor): Returns the instance of the DataProcessor.

        Raises:
            TypeError: If the provided config is None or it's an empty list.
        """
        if not config or len(config) == 0:
            raise TypeError("config must be provided.")
        self.config = config
        return self

    def load_files(self, fast_load: bool = False, nrows=100) -> bool:
        """
        Loads files based on the provided configuration.

        This method iterates over the files specified in the configuration. For each file, it constructs the file path,
        loads the file into a pandas DataFrame, and stores the DataFrame in the `data` attribute using the file name as the key.

        Parameters:
        fast_load (bool): If True, only the first 10 rows of each file are loaded. Default is False.
        nrows (int): Number of rows to read from the file. Default is 100. Only works when fast_load is True.

        Returns:
            bool: True if the files are loaded successfully, otherwise it raises an exception.

        Raises:
            ValueError: If no configuration is provided.
            Exception: If there is an error reading a file.
        """
        if self.config is None:
            raise ValueError("No configuration provided.")
        else:
            for file in self.config["data_files"]:
                self.logger.log("Loading file: " + file["fileName"])
                try:
                    filePath = self.base_path + file["fileName"]
                    if file["fileType"] == "csv":
                        self.data[file["fileName"]] = pd.read_csv(
                            filePath,
                            sep=file["separator"],
                            encoding="latin1",
                            low_memory=False,
                            on_bad_lines="warn",
                            nrows=nrows if fast_load else None,
                        )
                except:
                    raise Exception(f"Error reading file: {filePath}")
        return True

    def filter_data(self, drop_duplicates=False) -> pd.DataFrame:
        """
        Filters the loaded data based on the configuration provided.

        This method iterates over the keys in the `data` attribute, which correspond to the file names. For each file,
        it finds the corresponding configuration and filters the DataFrame based on the columns of interest specified in the configuration.
        If the `drop_duplicates` parameter is set to True, it removes duplicate columns from the final DataFrame.

        Parameters:
        drop_duplicates (bool): If True, duplicate columns are removed from the final DataFrame. Default is False.

        Returns:
            pd.DataFrame: A DataFrame that concatenates the filtered data from all files.

        Raises:
            ValueError: If no configuration is provided.
            Exception: If there is an error filtering a file.
        """
        if self.config is None:
            raise ValueError("No configuration provided.")
        self.__filtered_data = pd.DataFrame()
        for k in self.data.keys():
            self.logger.log("Filtering file: " + k)
            file_config = next(
                item for item in self.config["data_files"] if item["fileName"] == k
            )
            try:
                if file_config["colOfInterest"] != ["--"]:
                    self.__filtered_data = pd.concat(
                        [
                            self.__filtered_data,
                            self.data[k][file_config["colOfInterest"]],
                        ],
                        axis=1,
                    )
                elif file_config["colOfInterest"] == ["--"]:
                    self.__filtered_data = pd.concat(
                        [self.__filtered_data, self.data[k]], axis=1
                    )
            except:
                raise Exception(f"Error filtering file: {k}")

        if drop_duplicates:
            duplicate_cols = self.__filtered_data.columns[
                self.__filtered_data.columns.duplicated()
            ]
            self.__filtered_data.drop(columns=duplicate_cols, inplace=True)

        return self.__filtered_data

    def save_filtered_data(self, filename: str, format: file_types):
        """
        Save the filtered data to a file in the specified format.

        Parameters:
        filename (str): The name of the file to save the data to.
        format (file_types): The format to save the data in. Supported formats are "csv", "parquet", "feather", and "pickle".

        Raises:
        ValueError: If the specified format is not supported.
        """
        if format == "csv":
            self.__filtered_data.to_csv(filename)
        elif format == "parquet":
            self.__filtered_data.to_parquet(filename)
        elif format == "feather":
            self.__filtered_data.to_feather(filename)
        elif format == "pickle":
            self.__filtered_data.to_pickle(filename)
        else:
            raise ValueError("Invalid format")
