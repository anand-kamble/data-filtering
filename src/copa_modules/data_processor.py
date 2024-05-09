from typing import Dict, List, Union
import pandas as pd
from ._types import data_config, dataset, file_types


class data_processor:
    """
    The DataProcessor class is responsible for processing data based on a given configuration.

    Attributes:
        config (DataConfig): The configuration object containing parameters for data processing.
        base_path (str): The base path where the data is located.
        data (Dataset): The dataset object that will hold the processed data, initialized as an empty dictionary.
    """

    def __init__(self, config: data_config | None, base_path: str = ""):
        """
        Constructs all the necessary attributes for the DataProcessor object.

        Args:
            config (DataConfig): The configuration object containing parameters for data processing.
            base_path (str, optional): The base path where the data is located. Defaults to an empty string, 
                                       which means the data is located in the current directory.

        Raises:
            ValueError: If the config parameter is not provided.
        """
        # self.config: DataConfig | None = config if config else None
        self.config = config

        self.base_path: str = base_path
        self.data: dataset = {}

    def update_config(self, config: data_config) -> 'data_processor':
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

    def load_files(self) -> bool:
        """
        Loads files based on the configuration provided.

        This method iterates over the files specified in the configuration. For each file, it constructs the file path,
        loads the file into a pandas DataFrame, and stores the DataFrame in the `data` attribute using the file name as the key.

        Returns:
            bool: True if the files are loaded successfully, otherwise it raises an exception.

        Raises:
            ValueError: If no configuration is provided.
            Exception: If there is an error reading a file.
        """
        if self.config is None:
            raise ValueError("No configuration provided.")
        else:
            for file in self.config:
                print("Loading file: ", file["fileName"])
                try:
                    filePath = self.base_path + file["fileName"]
                    if file["fileType"] == "csv":
                        self.data[file["fileName"]] = pd.read_csv(
                            filePath,
                            sep=file["separator"],
                            encoding="latin1",
                            low_memory=False,
                            on_bad_lines="warn",
                        )
                except:
                    raise Exception(f"Error reading file: {filePath}")
        return True

    def filter_data(self) -> pd.DataFrame:
        """
        Filters the loaded data based on the configuration provided.

        This method iterates over the keys in the `data` attribute, which correspond to the file names. For each file, 
        it finds the corresponding configuration and filters the DataFrame based on the columns of interest specified in the configuration.

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
            print("Filtering file: ", k)
            fileConfig = next(
                item for item in self.config if item["fileName"] == k)
            try:
                if fileConfig["colOfInterest"] != ["--"]:
                    self.__filtered_data = pd.concat(
                        [self.__filtered_data, self.data[k][fileConfig["colOfInterest"]]], axis=1)
                elif fileConfig["colOfInterest"] == ["--"]:
                    self.__filtered_data = pd.concat(
                        [self.__filtered_data, self.data[k]], axis=1)
            except:
                raise Exception(f"Error filtering file: {k}")

        return self.__filtered_data
    
    def save_filtered_date(self,filename:str,format:file_types):
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
