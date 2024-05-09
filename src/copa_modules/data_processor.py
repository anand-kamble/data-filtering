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

    def load_files(self, fast_load: bool = False, nrows = 100) -> bool:
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
                            nrows= nrows if fast_load else None
                        )
                except:
                    raise Exception(f"Error reading file: {filePath}")
        return True

    def filter_data(self, drop_duplicates = False) -> pd.DataFrame:
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
            print("Filtering file: ", k)
            file_config = next(
                item for item in self.config if item["fileName"] == k)
            try:
                if file_config["colOfInterest"] != ["--"]:
                    self.__filtered_data = pd.concat(
                        [self.__filtered_data, self.data[k][file_config["colOfInterest"]]], axis=1)
                elif file_config["colOfInterest"] == ["--"]:
                    self.__filtered_data = pd.concat(
                        [self.__filtered_data, self.data[k]], axis=1)
            except:
                raise Exception(f"Error filtering file: {k}")
        
        if drop_duplicates:
            duplicate_cols = self.__filtered_data.columns[self.__filtered_data.columns.duplicated()]
            self.__filtered_data.drop(columns=duplicate_cols, inplace=True)


        return self.__filtered_data
    
    def save_filtered_data(self,filename:str,format:file_types):
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
