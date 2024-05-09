"""
Filename: DataConfig.py
Created on: 7th May 2024

This module defines the file_config dataclass and DataConfig type alias
"""

from typing import List, TypedDict, Optional
from dataclasses import dataclass
from .file_types import file_types


@dataclass
class file_config(TypedDict):
    fileName: str
    colOfInterest: List[str]
    fileType: str
    separator: Optional[str]
    filePathOverwrite: Optional[str]

@dataclass
class data_config(TypedDict):
    output_dir: str
    data_files: list[file_config]
    output_format: file_types
