"""
Filename: DataConfig.py
Created on: 7th May 2024

This module defines the FileConfig dataclass and DataConfig type alias
"""

from typing import List, TypedDict, Optional
from dataclasses import dataclass


@dataclass
class FileConfig(TypedDict):
    fileName: str
    colOfInterest: List[str]
    fileType: str
    separator: Optional[str]
    filePathOverwrite: Optional[str]


DataConfig = list[FileConfig]
