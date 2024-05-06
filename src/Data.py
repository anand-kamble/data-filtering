from typing import Dict, List, Union
import pandas as pd


class Data:
    def __init__(self, config: Dict[str, List[str]], base_path: str = ''):
        self.config = config if config else None
        self.base_path = base_path
        self.data = {}

    def updateConfig(self, config: Dict[str, List[str]]):
        self.config = config
        
    def read_csv(self) -> pd.DataFrame:
        for k in self.config.keys():
            self.data[k] = pd.read_csv(self.base_path + k, sep=';',encoding='latin1')
            
        
