"""
Filename: DataConfig.py
Created on: 7th May 2024

This module defines the file_config dataclass and DataConfig type alias
"""

from dataclasses import dataclass
from typing import List, Optional, TypedDict

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
    output_file_name: str
    test_rows: int
    data_files: list[file_config]
    output_format: file_types
