from typing import Dict, List, Union
import pandas as pd


class Data:
    def __init__(self, config: Dict[str, List[str]], base_path: str = ''):
        self.config = config if config else None
        self.base_path = base_path
        self.data = {}
        

    def updateConfig(self, config: Dict[str, List[str]]):
        self.config = config
        return self
        
    def readCsv(self) -> 'Data':
        for k in self.config.keys():
            try:
                read_data = pd.read_csv(self.base_path + k, sep=';',encoding='latin1',low_memory=False,on_bad_lines="warn")
            except:
                try:
                    read_data = pd.read_csv(self.base_path + k, sep=',',encoding='latin1',low_memory=False,on_bad_lines="warn")
                except:
                    raise Exception(f"Error reading file: {k}")
            self.data[k] = read_data

        return self
    
    def filterData(self) -> pd.DataFrame:
        self.filteredData = pd.DataFrame()
        for k in self.config.keys():
            if self.config[k] != ['--']:
                self.filteredData = pd.concat([self.filteredData, self.data[k][self.config[k]]], axis=1)
            elif self.config[k] == ['--']:
                self.filteredData = pd.concat([self.filteredData, self.data[k]], axis=1)
        return self.filteredData
        
