"""
Filename: DataConfig.py
Created on: 7th May 2024

This module defines the FileConfig dataclass and DataConfig type alias
"""

from typing import List, Union
from dataclasses import dataclass


@dataclass
class FileConfig:
    fileName: str
    colOfInterest: List[str]
    fileType: str = "csv"
    separator: Union[str, None] = None
    filePathOverwrite: Union[str, None] = None


DataConfig = List[FileConfig]
