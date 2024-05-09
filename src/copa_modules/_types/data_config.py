"""
Filename: DataConfig.py
Created on: 7th May 2024

This module defines the file_config dataclass and DataConfig type alias
"""

from typing import List, TypedDict, Optional
from dataclasses import dataclass


@dataclass
class file_config(TypedDict):
    fileName: str
    colOfInterest: List[str]
    fileType: str
    separator: Optional[str]
    filePathOverwrite: Optional[str]


data_config = list[file_config]
