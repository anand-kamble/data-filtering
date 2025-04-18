import json
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
        self.logger: Logger = Logger("DataProcessor")
        self.logger.log(
            f"Initializing DataProcessor { 'in test mode.' if test_mode else 'Running in normal mode.'} with base_path: {base_path}"
        )
        self.base_path: str = base_path
        if not config:
            raise CopaError("No configuration provided.")
        self.config: data_config = config
        self.config["test_rows"] = test_rows
        self.config["test_mode"] = test_mode
        self.test_mode = test_mode
        self.drop_duplicates = drop_duplicates
        self.no_cache = no_cache

    def load(self) -> pd.DataFrame:
        """
        Load the data based on the provided configuration.

        This method checks if a filtered file already exists based on the configuration. If it does, it loads the file.
        If the file does not exist or caching is disabled, it loads the raw data files, filters the data, and saves the filtered data.

        Returns:
            pd.DataFrame: The filtered data as a pandas DataFrame.

        Raises:
            CopaError: If the configuration is invalid or not provided.
        """
        if self.config:
            # print(data_config.__annotations__.keys())
            config_diff = set(data_config.__annotations__.keys()).difference(
                self.config.keys()
            )
            self.logger.log(f"Config diff is: {config_diff}")
            if bool(config_diff):
                self.logger.log("Invalid configuration provided.")
                raise CopaError("Invalid configuration provided.")
            filtered_file_exists = self.__check_filtered_file_exists()
            config_chagned = (
                self.__check_if_config_changed() if filtered_file_exists else False
            )
            # self.config: data_config = config
            if self.no_cache == False and filtered_file_exists and not config_chagned:
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
                self.logger.log("Filtered data loaded successfully.")
                return self.__filtered_data
            else:
                if self.no_cache == False and filtered_file_exists != True:
                    self.logger.log(
                        "Filtered file does not exist. Generating new one...",
                        print_message=True,
                    )
                elif config_chagned == True:
                    self.logger.log(
                        "Configuration has changed. Generating new file...",
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
                self.load_files(
                    fast_load=self.test_mode, nrows=self.config["test_rows"]
                )

                spinner.succeed("Files loaded successfully.")
                spinner.text = "Filtering Data..."
                spinner.start()
                self.filter_data(drop_duplicates=self.drop_duplicates)
                spinner.succeed("Data filtered successfully.")
                file_path_to_save = (
                    self.config["output_dir"] + "/" + self.config["output_file_name"]
                )
                if not os.path.exists(self.config["output_dir"]):
                    self.logger.log("output directory does not exist. Creating one...")
                    os.mkdir(self.config["output_dir"])
                self.save_filtered_data(file_path_to_save, self.config["output_format"])
                self.logger.log("Filtered data saved successfully.")
                return self.__filtered_data
        else:
            raise CopaError("No configuration provided.")

    def __check_filtered_file_exists(self) -> bool:
        """
        Check if the filtered file exists in the specified output directory.

        This method checks if the output directory exists. If it does, it checks if the output file is in the directory.
        If the directory does not exist, it returns False.

        Returns:
            bool: True if the output file exists in the output directory, False otherwise.
        """
        self.logger.log("Checking if filtered file exists...")
        if os.path.exists(self.config["output_dir"]):
            return self.config["output_file_name"] in os.listdir(
                self.config["output_dir"]
            ) and ("config.json" in os.listdir(self.config["output_dir"]))
        else:
            return False

    def __check_if_config_changed(self) -> bool:
        """
        Check if the configuration has changed since the last run.

        This method checks if the configuration has changed since the last run by comparing the current configuration
        with the configuration saved in the output directory.

        Returns:
            bool: True if the configuration has changed, False otherwise.
        """
        self.logger.log("Checking if configuration has changed...")
        with open(self.config["output_dir"] + "/config.json", "r") as json_file:
            old_config = json.load(json_file)
            return old_config != self.config

    def __read_dataframe(
        self, file_path: str, file_type: file_types, **kwargs
    ) -> pd.DataFrame:
        """
        Reads a dataframe from a file.

        This method reads a dataframe from a file based on the provided file path and file type.
        It supports csv, parquet, and feather file types.

        Args:
            file_path (str): The path to the file to read.
            file_type (file_types): The type of the file to read. Supported types are csv, parquet, and feather.
            **kwargs: Additional keyword arguments to pass to the pandas read function.

        Returns:
            pd.DataFrame: The loaded data as a pandas DataFrame.

        Raises:
            CopaError: If an unsupported file type is provided.
        """
        self.logger.log("Reading dataframe from file: " + file_path)
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
                raise CopaError(f"Error filtering file: {k}")

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
        with open(self.config["output_dir"] + "/config.json", "w") as json_file:
            json.dump(self.config, json_file)
