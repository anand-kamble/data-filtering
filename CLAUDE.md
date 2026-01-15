# CLAUDE.md - AI Assistant Guide for Data Filtering Repository

**Last Updated:** 2026-01-15
**Repository:** anand-kamble/data-filtering
**Purpose:** COPA Aircraft Maintenance Data Processing System

This document provides AI assistants with comprehensive context about this codebase's structure, conventions, workflows, and best practices.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Quick Start for AI Assistants](#quick-start-for-ai-assistants)
3. [Repository Structure](#repository-structure)
4. [Key Technologies](#key-technologies)
5. [Data Processing Architecture](#data-processing-architecture)
6. [Code Conventions](#code-conventions)
7. [Development Workflow](#development-workflow)
8. [Configuration System](#configuration-system)
9. [Testing Framework](#testing-framework)
10. [Common Tasks](#common-tasks)
11. [Domain Knowledge](#domain-knowledge)
12. [Important Files Reference](#important-files-reference)

---

## Project Overview

This is a **Python-based data processing system** for analyzing aircraft maintenance records from COPA (Panama's national airline). The system filters, organizes, and transforms large CSV datasets containing maintenance logbook data, fault information, and aircraft registration details.

**Core Capabilities:**
- Extract specific columns from multiple maintenance data tables
- Filter and organize data by ATA (Air Transport Association) codes
- Convert between data formats (CSV, Parquet, Feather, Pickle)
- Split data by aircraft tail numbers for analysis
- Cache processed results to avoid redundant computation
- Support test mode for rapid development with data subsets

**Primary Use Case:**
Enable efficient analysis of aircraft maintenance patterns, fault tracking, and maintenance scheduling through organized, filtered datasets.

---

## Quick Start for AI Assistants

### Before Making Changes

1. **Read these critical files first:**
   - `src/copa_modules/_types/data_config.py` - Configuration schema
   - `src/copa_modules/_data_processor/data_processor.py` - Core processing logic
   - `filter_configs/var_of_interest.json` - Current data configuration

2. **Understand the workflow:**
   - User provides CSV files → Configuration specifies columns → Processor filters → Output organized by ATA

3. **Check existing tests:**
   - Run: `./run.sh --test` or `python tests/main.py`

### Key Commands

```bash
# Standard execution (test mode, 33K rows)
./run.sh

# Full data processing
poetry run python src/main.py --config filter_configs/var_of_interest.json

# Run tests
./run.sh --test

# Install dependencies
poetry install
```

### When to Use Cache

- **Cache enabled (default):** Faster for iterative development, skips reprocessing if config unchanged
- **Disable cache (`--no_cache`):** When input CSV files updated or force reprocessing needed

---

## Repository Structure

```
data-filtering/
├── src/                                 # Main source code
│   ├── copa_modules/                    # Core package
│   │   ├── _data_processor/             # DataProcessor class (core logic)
│   │   │   └── data_processor.py        # Load, filter, cache, save operations
│   │   ├── _error/                      # Custom exception handling
│   │   │   └── error_class.py           # CopaError with colored output
│   │   ├── _logger/                     # Logging utilities
│   │   │   └── logger.py                # Logger with timestamps
│   │   ├── _types/                      # Type definitions
│   │   │   ├── data_config.py           # Config TypedDicts
│   │   │   ├── data_set.py              # Dataset type alias
│   │   │   └── file_types.py            # Supported formats
│   │   └── utils/                       # Helper functions
│   │       └── util.py                  # Date/ATA extraction (regex)
│   ├── main.py                          # CLI entry point
│   ├── join_tables.py                   # Analyze table join compatibility
│   ├── split_by_tail_number.py          # Split data by aircraft
│   ├── write_parquet.py                 # CSV to Parquet converter
│   ├── AC_REG_CD_Analysis.py            # Registration code analysis
│   └── ATA_Per_AirCraft.py              # ATA event visualization
│
├── tests/                               # Testing framework
│   ├── main.py                          # Test runner
│   └── test.py                          # Custom Test class
│
├── filter_configs/                      # JSON configurations
│   ├── var_of_interest.json             # Main config (columns to extract)
│   └── ata_filter.json                  # ATA-specific filtering
│
├── docs/                                # Documentation
│   ├── filter_config_usage.md           # Config file creation guide
│   ├── Variable_list.md                 # Important column descriptions
│   ├── split_by_tail_number.md          # Aircraft splitting docs
│   ├── AC_REG_CD_Analysis.md            # Registration analysis
│   └── test_joining.md                  # Table joining guide
│
├── notebooks/                           # Jupyter analysis notebooks
├── Benchmark/                           # Performance benchmarking
├── run.sh                               # Default execution wrapper
├── pyproject.toml                       # Poetry dependencies
└── README.md                            # User-facing documentation
```

### Directory Purposes

| Directory | Purpose |
|-----------|---------|
| `src/copa_modules/` | Core reusable modules (processor, logger, types, errors) |
| `filter_configs/` | JSON files defining which columns to extract from which files |
| `tests/` | Custom test framework and test suite |
| `docs/` | Technical documentation for specific features |
| `notebooks/` | Jupyter notebooks for exploratory data analysis |
| `Benchmark/` | Performance testing and optimization |

---

## Key Technologies

### Core Stack

- **Python 3.11-3.12** - Primary language
- **Poetry** - Dependency management and virtual environment
- **pandas 2.2.2** - Data manipulation (primary)
- **modin 0.29.0** - Parallel pandas execution
- **Parquet/Feather** - Efficient columnar storage formats

### Data Processing Libraries

- **tidypandas 0.3.0** - Tidyverse-style operations
- **dfply 0.3.3** - Piped data frame operations
- **static-frame 2.6.0** - Alternative data frame library
- **ydata-profiling 4.7.0** - Data quality profiling

### Visualization & Analysis

- **seaborn 0.13.2** - Statistical visualization
- **matplotlib** - Base plotting (implicit via seaborn)
- **jupyter 1.0.0** - Interactive notebooks

### Development Tools

- **commitizen** - Semantic versioning and changelog automation
- **pre-commit** - Git hooks for commit message validation
- **pytest 8.2.0** - Testing framework
- **halo 0.0.31** - CLI progress spinners

### Output Format Support

- CSV (via pandas)
- Parquet (via pandas/pyarrow)
- Feather (via pandas/pyarrow)
- Pickle (via pandas)

---

## Data Processing Architecture

### Processing Pipeline Flow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. CONFIGURATION LOADING                                    │
│    • Read JSON config (filter_configs/*.json)               │
│    • Validate against TypedDict schema                      │
│    • Parse CLI arguments                                    │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. CACHE CHECK                                              │
│    • Check if output file exists                            │
│    • Compare current config with saved config.json          │
│    • If match: Load cached data, skip to step 6             │
│    • If no match or --no_cache: Continue to step 3          │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│ 3. DATA LOADING (load_files)                                │
│    • Iterate over data_files in config                      │
│    • Read CSV with pandas (encoding: latin1)                │
│    • Handle bad lines (warn and skip)                       │
│    • Optional: Limit to test_rows if test_mode=True         │
│    • Store in dictionary: {filename: DataFrame}             │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│ 4. FILTERING (filter_data)                                  │
│    • For each file, extract colOfInterest columns           │
│    • Concatenate all filtered DataFrames (axis=1)           │
│    • Optional: Drop duplicate columns if --drop_duplicates  │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│ 5. SAVING                                                   │
│    • Save filtered DataFrame to output_format               │
│    • Save config.json for cache validation                  │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│ 6. SPLITTING (split_dataframe_to_csv)                      │
│    • Extract unique ATA values                              │
│    • Strategy "full": One CSV per unique ATA value          │
│    • Strategy "merged": Group by ATA prefix (first 2 chars) │
│    • Output: ata_filtered/ATA_*.csv                         │
└─────────────────────────────────────────────────────────────┘
```

### Key Classes

**DataProcessor** (`src/copa_modules/_data_processor/data_processor.py`)

Core class managing the entire data pipeline:

```python
class DataProcessor:
    def __init__(self, config: data_config):
        """Initialize with validated configuration"""

    def load_files(self) -> None:
        """Load CSV files into memory as DataFrames"""

    def filter_data(self, drop_duplicates: bool = False) -> None:
        """Extract columns of interest and concatenate"""

    def save_filtered_data(self) -> None:
        """Export filtered data to specified format"""

    def __check_filtered_file_exists(self) -> bool:
        """Validate cache freshness"""
```

### Data Flow Example

**Input:** `ISDP LOGBOOK REPORT.csv` (10M rows × 100 columns)

**Configuration:**
```json
{
  "colOfInterest": ["FLEET", "FAULT_FOUND_DATE", "ATA", "FAULT_SEVERITY"]
}
```

**Output:** `copa_output/filtered.parquet` (10M rows × 4 columns)

**Split Output:** `ata_filtered/ATA_32.csv`, `ata_filtered/ATA_24.csv`, etc.

---

## Code Conventions

### Naming Conventions

Follow these patterns consistently:

| Element | Convention | Example |
|---------|-----------|---------|
| Classes | PascalCase | `DataProcessor`, `Logger`, `CopaError` |
| Functions/Methods | snake_case | `load_files()`, `filter_data()`, `split_dataframe_to_csv()` |
| Variables | snake_case | `filtered_data`, `my_data_processor`, `config_path` |
| Constants | UPPER_SNAKE_CASE | `DEFAULT_SEPARATOR = ","` |
| Type Aliases | lowercase | `data_config`, `file_types`, `data_set` |
| Private Methods | `__method_name` | `__check_filtered_file_exists()` |

### Code Style Guidelines

**Type Hints:**
Always use type hints for function parameters and return values:

```python
def load_files(self) -> None:
    """Load all configured data files"""

def filter_data(self, drop_duplicates: bool = False) -> None:
    """Filter dataframes to columns of interest"""
```

**Docstrings:**
Use Google-style docstrings:

```python
def split_dataframe_to_csv(df: pd.DataFrame, split_by: str, strategy: str) -> None:
    """
    Split DataFrame into multiple CSV files based on column values.

    Args:
        df: Source DataFrame to split
        split_by: Column name to group by
        strategy: "full" for individual files, "merged" for prefix grouping

    Returns:
        None (writes files to disk)

    Raises:
        CopaError: If split_by column not found in DataFrame
    """
```

**Error Handling:**
Use custom `CopaError` for domain-specific errors:

```python
from copa_modules._error.error_class import CopaError

if column not in df.columns:
    raise CopaError(f"Column '{column}' not found in DataFrame")
```

**Logging:**
Use centralized Logger for status updates:

```python
from copa_modules._logger.logger import Logger

logger = Logger(scope="DataProcessor")
logger.log_entry("Loading files...")
logger.log_success("Successfully loaded 5 files")
logger.log_warning("Duplicate columns detected")
```

### Module Organization

**Package Structure:**
Private internal packages use underscore prefix (`_data_processor`, `_logger`, `_error`):

```python
# Good: Clear separation of internal vs. external
src/copa_modules/_data_processor/  # Internal implementation
src/copa_modules/utils/             # Public utilities

# Bad: No underscore for internal packages
src/copa_modules/data_processor/    # Unclear if public API
```

**Import Patterns:**
Use absolute imports from `copa_modules`:

```python
# Good
from copa_modules._data_processor.data_processor import DataProcessor
from copa_modules._types.data_config import data_config
from copa_modules.utils.util import extract_ata_code

# Bad
from _data_processor.data_processor import DataProcessor  # Relative path
```

### Type Safety

**TypedDict for Configurations:**
All config structures use TypedDict for validation:

```python
from typing import TypedDict, Literal

class file_config(TypedDict):
    fileName: str
    fileType: Literal["csv", "parquet", "feather", "pickle"]
    separator: str
    colOfInterest: list[str]

class data_config(TypedDict):
    output_dir: str
    output_file_name: str
    output_format: Literal["csv", "parquet", "feather", "pickle"]
    data_files: list[file_config]
```

---

## Development Workflow

### Initial Setup

1. **Clone repository:**
   ```bash
   git clone git@github.com:anand-kamble/data-filtering.git
   cd data-filtering
   ```

2. **Install Poetry** (if not installed):
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

3. **Install dependencies:**
   ```bash
   poetry install
   ```

4. **Activate virtual environment:**
   ```bash
   poetry shell
   ```

### Running the Application

**Quick Test (Default):**
```bash
./run.sh
```
Equivalent to:
```bash
poetry run python src/main.py \
    --config filter_configs/var_of_interest.json \
    --test_mode \
    --test_rows 33000 \
    --drop_duplicates \
    --split merged
```

**Full Processing:**
```bash
poetry run python src/main.py \
    --config filter_configs/var_of_interest.json \
    --drop_duplicates \
    --split full
```

**CLI Arguments:**

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--config` | str | Required | Path to JSON configuration file |
| `--test_mode` | flag | False | Enable test mode (limited rows) |
| `--test_rows` | int | 1000 | Number of rows in test mode |
| `--drop_duplicates` | flag | False | Remove duplicate columns after filtering |
| `--no_cache` | flag | False | Force reprocessing, ignore cache |
| `--split` | str | "full" | Split strategy: "full" or "merged" |

### Testing

**Run test suite:**
```bash
./run.sh --test
# or
python tests/main.py
```

**Test framework features:**
- Custom chainable assertions
- Soft mode (continue on failure) or hard mode (exit on failure)
- Markdown export of results
- Colored terminal output

**Example test:**
```python
from tests.test import Test
from copa_modules._data_processor.data_processor import DataProcessor

processor_test = Test("DataProcessor", mode="soft")
processor_test.expect(my_processor).has_type(DataProcessor)
processor_test.expect(data.shape[0]).to_be(33000)
processor_test.export_results()  # Saves to test_results.md
```

### Git Workflow

**Commit Messages:**
Use conventional commits (enforced by commitizen pre-commit hook):

```bash
# Good commits
git commit -m "feat: add support for Parquet output format"
git commit -m "fix: handle missing ATA column in split function"
git commit -m "docs: update filter_config_usage.md with examples"
git commit -m "refactor: extract ATA parsing into util function"

# Bad commits
git commit -m "Update code"  # Rejected by pre-commit hook
git commit -m "Fixed bug"     # Rejected by pre-commit hook
```

**Commit Types:**
- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `refactor:` - Code restructuring without functionality change
- `test:` - Test additions or modifications
- `chore:` - Maintenance tasks

**Changelog Management:**
Version bumps and changelog updates managed automatically via commitizen:

```bash
cz bump  # Analyzes commits and updates version/CHANGELOG.md
```

### Adding New Features

**Checklist for AI Assistants:**

1. **Understand existing patterns:**
   - Read relevant source files first
   - Check for similar implementations
   - Review type definitions in `_types/`

2. **Maintain type safety:**
   - Add TypedDict definitions for new configs
   - Use type hints in function signatures
   - Update `file_types.py` if adding new formats

3. **Follow error handling patterns:**
   - Use `CopaError` for domain errors
   - Add descriptive error messages
   - Log warnings for non-fatal issues

4. **Update documentation:**
   - Add docstrings to new functions/classes
   - Update relevant `.md` files in `docs/`
   - Add examples to configuration documentation

5. **Test changes:**
   - Add tests in `tests/main.py`
   - Run test suite before committing
   - Test with both test mode and full data

6. **Commit properly:**
   - Use conventional commit messages
   - Create focused, single-purpose commits
   - Update CHANGELOG.md if significant feature

---

## Configuration System

### Configuration File Structure

Configuration files are JSON documents validated against the `data_config` TypedDict schema.

**Example: `filter_configs/var_of_interest.json`**

```json
{
  "output_dir": "copa_output",
  "output_file_name": "filtered.parquet",
  "output_format": "parquet",
  "test_rows": 33000,
  "test_mode": true,
  "data_files": [
    {
      "fileName": "TABLES_ADD_20240515/ISDP LOGBOOK REPORT.csv",
      "fileType": "csv",
      "separator": ",",
      "colOfInterest": [
        "FLEET",
        "FLIGHT",
        "FAULT_FOUND_DATE",
        "FAULT_SOURCE",
        "FAULT_NAME",
        "FAULT_SDESC",
        "CORRECTIVE_ACTION",
        "MAINT_DELAY_TIME_QT",
        "ATA",
        "FAULT_SEVERITY"
      ]
    },
    {
      "fileName": "TABLES_ADD_20240515/SD_FAULT.csv",
      "fileType": "csv",
      "separator": ",",
      "colOfInterest": [
        "FAULT_DB_ID",
        "FAULT_ID",
        "FAULT_CD",
        "FAULT_LDESC"
      ]
    }
  ]
}
```

### Configuration Schema

**Top-Level (`data_config`):**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `output_dir` | str | Yes | Directory for processed output |
| `output_file_name` | str | Yes | Name of filtered output file |
| `output_format` | str | Yes | Format: "csv", "parquet", "feather", "pickle" |
| `test_rows` | int | No | Number of rows in test mode |
| `test_mode` | bool | No | Enable test mode |
| `data_files` | list | Yes | List of input file configurations |

**File Configuration (`file_config`):**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `fileName` | str | Yes | Relative or absolute path to data file |
| `fileType` | str | Yes | Format: "csv", "parquet", "feather", "pickle" |
| `separator` | str | Yes | CSV separator (typically "," or ";") |
| `colOfInterest` | list[str] | Yes | Columns to extract from this file |

### Creating New Configurations

**Step 1: Identify data sources**
```bash
ls TABLES_ADD_20240515/*.csv
```

**Step 2: Inspect column names**
```python
import pandas as pd
df = pd.read_csv("TABLES_ADD_20240515/EVT_EVENT.csv", nrows=1)
print(df.columns.tolist())
```

**Step 3: Create configuration JSON**
```json
{
  "output_dir": "my_analysis",
  "output_file_name": "event_data.parquet",
  "output_format": "parquet",
  "data_files": [
    {
      "fileName": "TABLES_ADD_20240515/EVT_EVENT.csv",
      "fileType": "csv",
      "separator": ",",
      "colOfInterest": ["EVENT_DB_ID", "EVENT_TYPE_CD", "EVENT_DT"]
    }
  ]
}
```

**Step 4: Validate configuration**
```bash
poetry run python src/main.py --config my_config.json --test_mode --test_rows 100
```

### Cache Behavior

**Cache validation logic:**

1. Check if `{output_dir}/{output_file_name}` exists
2. Check if `{output_dir}/config.json` exists
3. Compare current config with saved `config.json`
4. If identical: Load cached file, skip processing
5. If different or `--no_cache` flag: Reprocess data

**When cache is invalidated:**
- Column list changed (`colOfInterest` modified)
- Input file names changed
- Output format changed
- `--no_cache` flag used

**When cache persists:**
- CLI arguments changed (e.g., `--test_rows`)
- Input CSV files updated (limitation: not detected)

---

## Testing Framework

### Custom Test Class

The project uses a custom chainable assertion framework (`tests/test.py`) instead of standard pytest for the main test suite.

**Basic Usage:**

```python
from tests.test import Test

# Create test suite
my_test = Test("Suite Name", mode="soft")

# Chainable assertions
my_test.expect(value).to_be(expected)
my_test.expect(obj).has_type(ExpectedClass)
my_test.expect(obj).has_attribute("property_name")
my_test.expect(3.14159).is_approximately(3.14, tolerance=0.01)

# Export results to Markdown
my_test.export_results()  # Creates test_results.md
```

### Test Modes

**Soft Mode (default):**
- Continues running all tests even if some fail
- Useful for seeing all failures at once
- Use for development and CI

```python
test = Test("My Tests", mode="soft")
```

**Hard Mode:**
- Exits immediately on first failure
- Useful for debugging specific issues
- Use when you need fast feedback on a specific test

```python
test = Test("My Tests", mode="hard")
```

### Assertion Methods

| Method | Usage | Description |
|--------|-------|-------------|
| `.to_be(expected)` | `test.expect(5).to_be(5)` | Exact equality check |
| `.has_type(cls)` | `test.expect(obj).has_type(DataProcessor)` | Type validation |
| `.has_attribute(name)` | `test.expect(obj).has_attribute("load_files")` | Attribute existence |
| `.is_approximately(val, tol)` | `test.expect(3.14).is_approximately(3.1, 0.1)` | Float comparison |

### Running Tests

**Execute test suite:**
```bash
./run.sh --test
```

**Direct execution:**
```bash
poetry run python tests/main.py
```

**Expected output:**
```
✓ Test passed: DataProcessor has correct type
✓ Test passed: Filtered data has 33000 rows
✗ Test failed: Expected ATA column (expected vs actual)

Test results exported to test_results.md
```

### Adding New Tests

**Pattern to follow:**

```python
# tests/main.py

from tests.test import Test
from copa_modules._data_processor.data_processor import DataProcessor

def test_data_processor():
    """Test DataProcessor functionality"""

    # Setup
    test = Test("DataProcessor Tests", mode="soft")
    config = load_config("filter_configs/var_of_interest.json")
    processor = DataProcessor(config)

    # Tests
    test.expect(processor).has_type(DataProcessor)
    test.expect(processor).has_attribute("load_files")
    test.expect(processor).has_attribute("filter_data")

    processor.load_files()
    test.expect(len(processor.files)).to_be(5)

    processor.filter_data()
    test.expect(processor.filtered_data).has_attribute("shape")

    # Export results
    test.export_results()

if __name__ == "__main__":
    test_data_processor()
```

---

## Common Tasks

### Task 1: Add Support for New Input Format

**Scenario:** Add support for JSON input files

**Steps:**

1. **Update type definitions** (`src/copa_modules/_types/file_types.py`):
   ```python
   from typing import Literal

   file_types = Literal["csv", "parquet", "feather", "pickle", "json"]
   ```

2. **Update DataProcessor loading logic** (`src/copa_modules/_data_processor/data_processor.py`):
   ```python
   def load_files(self) -> None:
       for file in self.config["data_files"]:
           file_type = file["fileType"]
           file_path = file["fileName"]

           if file_type == "csv":
               df = pd.read_csv(file_path, sep=file["separator"])
           elif file_type == "parquet":
               df = pd.read_parquet(file_path)
           elif file_type == "json":  # New format
               df = pd.read_json(file_path)
           # ... other formats
   ```

3. **Update configuration schema** (`src/copa_modules/_types/data_config.py`):
   ```python
   class file_config(TypedDict):
       fileName: str
       fileType: file_types  # Uses updated Literal
       separator: str
       colOfInterest: list[str]
   ```

4. **Test with sample JSON file:**
   ```json
   {
     "data_files": [
       {
         "fileName": "test_data.json",
         "fileType": "json",
         "separator": "",
         "colOfInterest": ["column1", "column2"]
       }
     ]
   }
   ```

5. **Add test in `tests/main.py`:**
   ```python
   test.expect(processor.load_files()).to_not_raise_error()
   test.expect("test_data.json" in processor.files).to_be(True)
   ```

### Task 2: Add New Column Extraction Utility

**Scenario:** Extract date from FAULT_FOUND_DATE column

**Steps:**

1. **Add utility function** (`src/copa_modules/utils/util.py`):
   ```python
   import re
   from datetime import datetime

   def extract_date(date_string: str) -> datetime | None:
       """
       Extract date from COPA date string format.

       Args:
           date_string: Date string in format "DD-MMM-YYYY HH:MM"

       Returns:
           datetime object or None if parsing fails
       """
       try:
           pattern = r"(\d{2}-\w{3}-\d{4})"
           match = re.search(pattern, date_string)
           if match:
               return datetime.strptime(match.group(1), "%d-%b-%Y")
       except Exception:
           return None
   ```

2. **Add to a processing script** (e.g., new analysis file):
   ```python
   from copa_modules.utils.util import extract_date

   df["FAULT_DATE"] = df["FAULT_FOUND_DATE"].apply(extract_date)
   ```

3. **Update docstring in `utils/util.py`** with example:
   ```python
   """
   Utility functions for data extraction and transformation.

   Available functions:
   - extract_ata_code: Extract ATA code from text
   - extract_date: Parse COPA date strings
   """
   ```

### Task 3: Modify Data Splitting Strategy

**Scenario:** Add new splitting strategy by fault severity

**Steps:**

1. **Add new split function** (`src/copa_modules/utils/util.py`):
   ```python
   def split_by_severity(
       df: pd.DataFrame,
       severity_col: str = "FAULT_SEVERITY",
       output_dir: str = "severity_filtered"
   ) -> None:
       """
       Split DataFrame by fault severity levels.

       Args:
           df: Source DataFrame
           severity_col: Column containing severity values
           output_dir: Directory for output files
       """
       import os

       os.makedirs(output_dir, exist_ok=True)

       for severity in df[severity_col].unique():
           if pd.isna(severity):
               continue

           filtered = df[df[severity_col] == severity]
           filename = f"{output_dir}/SEVERITY_{severity}.csv"
           filtered.to_csv(filename, index=False)

           print(f"Created {filename} with {len(filtered)} rows")
   ```

2. **Add CLI argument** (`src/main.py`):
   ```python
   parser.add_argument(
       "--split_by",
       type=str,
       default="ata",
       choices=["ata", "severity", "aircraft"],
       help="Column to split data by"
   )
   ```

3. **Update main execution logic** (`src/main.py`):
   ```python
   if args.split_by == "ata":
       split_dataframe_to_csv(filtered_data, "ATA", args.split)
   elif args.split_by == "severity":
       split_by_severity(filtered_data, "FAULT_SEVERITY")
   ```

### Task 4: Add Data Quality Validation

**Scenario:** Validate required columns exist before processing

**Steps:**

1. **Add validation function** (`src/copa_modules/utils/util.py`):
   ```python
   def validate_columns(df: pd.DataFrame, required_cols: list[str]) -> bool:
       """
       Validate that all required columns exist in DataFrame.

       Args:
           df: DataFrame to validate
           required_cols: List of required column names

       Returns:
           True if all columns exist

       Raises:
           CopaError: If any required columns are missing
       """
       missing = [col for col in required_cols if col not in df.columns]

       if missing:
           from copa_modules._error.error_class import CopaError
           raise CopaError(
               f"Missing required columns: {', '.join(missing)}\n"
               f"Available columns: {', '.join(df.columns)}"
           )

       return True
   ```

2. **Integrate into DataProcessor** (`src/copa_modules/_data_processor/data_processor.py`):
   ```python
   from copa_modules.utils.util import validate_columns

   def filter_data(self, drop_duplicates: bool = False) -> None:
       """Filter data and validate columns"""

       for file_name, df in self.files.items():
           file_config = self.__get_file_config(file_name)
           required_cols = file_config["colOfInterest"]

           # Validate before filtering
           validate_columns(df, required_cols)

           # Continue with filtering...
   ```

### Task 5: Export Data Profile Report

**Scenario:** Generate data quality report using ydata-profiling

**Steps:**

1. **Create new script** (`src/generate_profile.py`):
   ```python
   """Generate data quality profile report"""

   import pandas as pd
   from ydata_profiling import ProfileReport
   from copa_modules._data_processor.data_processor import DataProcessor
   import json

   def generate_profile(config_path: str, output_path: str = "data_profile.html"):
       """Generate data quality profile report"""

       # Load config and process data
       with open(config_path) as f:
           config = json.load(f)

       processor = DataProcessor(config)
       processor.load_files()
       processor.filter_data()

       # Generate profile
       profile = ProfileReport(
           processor.filtered_data,
           title="COPA Data Quality Report",
           explorative=True
       )

       profile.to_file(output_path)
       print(f"Profile report saved to {output_path}")

   if __name__ == "__main__":
       import sys
       config = sys.argv[1] if len(sys.argv) > 1 else "filter_configs/var_of_interest.json"
       generate_profile(config)
   ```

2. **Run profile generation:**
   ```bash
   poetry run python src/generate_profile.py filter_configs/var_of_interest.json
   ```

3. **Add to documentation** (`docs/data_quality.md`):
   ```markdown
   # Data Quality Profiling

   Generate comprehensive data quality reports using ydata-profiling.

   ## Usage

   poetry run python src/generate_profile.py <config_path>

   ## Output

   HTML report with:
   - Column statistics
   - Missing value analysis
   - Correlation matrices
   - Duplicate detection
   ```

---

## Domain Knowledge

### Aircraft Maintenance Context

**COPA (Compañía Panameña de Aviación):**
Panama's national airline. This project processes their maintenance records to analyze aircraft health, fault patterns, and maintenance efficiency.

### ATA Codes (Air Transport Association)

**Purpose:** Standardized system for categorizing aircraft components and maintenance tasks.

**Format:** Two-digit codes (e.g., "32", "24", "00") or four-digit subcodes (e.g., "32-41").

**Common ATA Codes in Dataset:**

| ATA Code | System | Description |
|----------|--------|-------------|
| 00 | General | Weight and balance, aircraft servicing |
| 21 | Air Conditioning | Environmental control systems |
| 24 | Electrical Power | Generators, batteries, distribution |
| 27 | Flight Controls | Ailerons, elevators, rudder, flaps |
| 28 | Fuel | Fuel tanks, pumps, distribution |
| 29 | Hydraulic Power | Hydraulic systems and components |
| 32 | Landing Gear | Wheels, brakes, steering, shock absorbers |
| 34 | Navigation | Instruments, autopilot, FMS |
| 36 | Pneumatic | Bleed air systems |
| 49 | Auxiliary Power Unit (APU) | APU systems and controls |
| 52 | Doors | Passenger doors, cargo doors |
| 71-80 | Powerplant | Engines and related systems |

**Usage in Code:**

```python
# Extract ATA code from text
from copa_modules.utils.util import extract_ata_code

text = "ATA 32-41: Landing gear malfunction"
code = extract_ata_code(text)  # Returns "32"

# Split data by ATA
split_dataframe_to_csv(df, split_by="ATA", strategy="merged")
# Creates: ATA_32.csv, ATA_24.csv, etc.
```

### Key Data Tables

**Primary Tables in COPA Dataset:**

| Table | Description | Key Columns |
|-------|-------------|-------------|
| ISDP LOGBOOK REPORT | Flight-by-flight maintenance log | FLEET, FLIGHT, FAULT_FOUND_DATE, ATA, FAULT_SEVERITY |
| SD_FAULT | Fault definitions and descriptions | FAULT_DB_ID, FAULT_CD, FAULT_LDESC |
| EVT_EVENT | Maintenance events and actions | EVENT_DB_ID, EVENT_TYPE_CD, EVENT_DT |
| INV_AC_REG | Aircraft registration info | AC_REG_CD (tail number), INV_NO_DB_ID |
| SCHED_STASK | Scheduled maintenance tasks | SCHED_DB_ID, TASK_CD, NEXT_SCHED_DEAD_DT |
| REQ_PART | Parts requests and inventory | REQ_PART_DB_ID, PART_NO, QTY_REQUIRED |

**Table Relationships:**

```
INV_AC_REG (aircraft info)
    │
    ├──→ ISDP LOGBOOK REPORT (flight logs)
    │         │
    │         └──→ SD_FAULT (fault definitions)
    │
    └──→ EVT_EVENT (maintenance events)
              │
              └──→ SCHED_STASK (scheduled tasks)
                        │
                        └──→ REQ_PART (parts needed)
```

### Important Columns

**FAULT_SEVERITY:**
Maintenance criticality levels (exact values domain-specific).

**AC_REG_CD:**
Aircraft registration code (tail number), e.g., "HP-1825CMP".

**MAINT_DELAY_TIME_QT:**
Duration of maintenance delay (impacts operational efficiency).

**CORRECTIVE_ACTION:**
Free-text description of maintenance action taken.

**FAULT_FOUND_DATE:**
Timestamp when fault was discovered (format: "DD-MMM-YYYY HH:MM").

### Data Characteristics

**Volume:**
- ISDP LOGBOOK REPORT: ~10M rows
- Processing time (full): 10-15 minutes
- Processing time (test mode, 33K rows): ~30 seconds

**Data Quality Issues:**
- Inconsistent date formats
- Missing ATA codes in some rows
- Duplicate columns across joined tables
- Bad lines in CSV files (handled by pandas `on_bad_lines="warn"`)

**Encoding:**
Latin-1 (ISO-8859-1) used for CSV files due to Spanish characters.

---

## Important Files Reference

### Configuration Files

| File | Purpose | When to Modify |
|------|---------|----------------|
| `filter_configs/var_of_interest.json` | Main processing config | Add/remove columns, change data sources |
| `filter_configs/ata_filter.json` | ATA-specific filtering | Customize ATA-based processing |
| `pyproject.toml` | Dependencies and metadata | Add new libraries, update versions |
| `.env` | Environment variables | Set PYTHONPATH or other env vars |

### Core Source Files

| File | Lines | Purpose | Modify When |
|------|-------|---------|-------------|
| `src/main.py` | ~150 | CLI entry point | Add CLI arguments, change execution flow |
| `src/copa_modules/_data_processor/data_processor.py` | ~300 | Core processing logic | Change loading/filtering/caching behavior |
| `src/copa_modules/_types/data_config.py` | ~50 | Configuration schema | Add new config options |
| `src/copa_modules/_types/file_types.py` | ~10 | Supported formats | Add new file format support |
| `src/copa_modules/_logger/logger.py` | ~80 | Logging utility | Modify log format or behavior |
| `src/copa_modules/_error/error_class.py` | ~40 | Custom exception | Add new error types |
| `src/copa_modules/utils/util.py` | ~100 | Helper functions | Add new utility functions |

### Analysis Scripts

| File | Purpose | Input | Output |
|------|---------|-------|--------|
| `src/write_parquet.py` | CSV to Parquet batch conversion | Directory of CSVs | Parquet files |
| `src/split_by_tail_number.py` | Split data by aircraft | Merged dataset | Per-aircraft CSVs |
| `src/join_tables.py` | Analyze join compatibility | Multiple tables | Common column report |
| `src/AC_REG_CD_Analysis.py` | Registration pattern analysis | LOGBOOK + INV_AC_REG | Analysis plots |
| `src/ATA_Per_AirCraft.py` | ATA event visualization | Filtered data | Time-series plots |

### Documentation Files

| File | Audience | Content |
|------|----------|---------|
| `README.md` | End users | Quick start, installation, basic usage |
| `INSTALL.md` | Developers | Detailed setup instructions |
| `CLAUDE.md` | AI assistants | This file - comprehensive technical guide |
| `docs/filter_config_usage.md` | Developers | Configuration file creation guide |
| `docs/Variable_list.md` | Data analysts | Column definitions and meanings |
| `docs/split_by_tail_number.md` | Developers | Aircraft splitting documentation |
| `Questions.md` | Domain experts | Data clarification questions |

### Shell Scripts

| Script | Purpose | Arguments |
|--------|---------|-----------|
| `run.sh` | Default execution wrapper | `--test` (optional) |
| `split_by_tail_number.sh` | Run aircraft splitting | None |
| `write_parquet.sh` | Run Parquet conversion | None |

---

## Best Practices for AI Assistants

### When Modifying Code

1. **Always read files before editing:**
   - Never propose changes to code you haven't seen
   - Understand existing patterns before adding new code

2. **Maintain type safety:**
   - Use type hints for all new functions
   - Update TypedDict definitions when changing configs
   - Validate types at runtime where appropriate

3. **Follow existing conventions:**
   - Match naming patterns (snake_case, PascalCase)
   - Use Google-style docstrings
   - Import from `copa_modules` with absolute paths

4. **Test changes:**
   - Add tests to `tests/main.py` for new features
   - Run `./run.sh --test` before committing
   - Test with both test mode and full data if possible

5. **Document changes:**
   - Update relevant `.md` files in `docs/`
   - Add docstrings to new functions/classes
   - Include examples in docstrings

6. **Error handling:**
   - Use `CopaError` for domain-specific errors
   - Provide descriptive error messages
   - Log warnings for non-fatal issues

### When Analyzing Issues

1. **Check logs first:**
   - Review `copa_logs.txt` for error messages
   - Look for warnings about bad CSV lines
   - Check for missing column errors

2. **Verify configuration:**
   - Validate JSON syntax in config files
   - Ensure all `colOfInterest` exist in source files
   - Check file paths are correct

3. **Test with small dataset:**
   - Use `--test_mode --test_rows 100` for quick debugging
   - Isolate issues before processing full data

4. **Check cache state:**
   - Use `--no_cache` to rule out cache issues
   - Compare `config.json` in output dir with current config

### When Adding Features

1. **Plan before coding:**
   - Identify affected files
   - Consider impact on existing functionality
   - Check if similar patterns exist elsewhere

2. **Start with types:**
   - Update TypedDict definitions first
   - Ensure type safety from the start

3. **Implement incrementally:**
   - Add feature in small, testable chunks
   - Test each chunk before moving forward

4. **Update documentation:**
   - Add to this CLAUDE.md if significant
   - Update user-facing README.md if needed
   - Add examples to relevant docs

---

## Troubleshooting Guide

### Common Issues

**Issue: "Column not found in DataFrame"**
- **Cause:** Config specifies column that doesn't exist in CSV
- **Solution:**
  1. Check column names: `pd.read_csv(file, nrows=1).columns`
  2. Update `colOfInterest` in config
  3. Verify separator (comma vs. semicolon)

**Issue: "Cache not updating after data changes"**
- **Cause:** Cache only checks config, not source files
- **Solution:** Use `--no_cache` flag to force reprocessing

**Issue: "Memory error during processing"**
- **Cause:** Large dataset loaded entirely into memory
- **Solution:**
  1. Use test mode: `--test_mode --test_rows 50000`
  2. Process files individually
  3. Consider using Modin for parallel processing

**Issue: "Bad lines warning during CSV read"**
- **Cause:** Malformed rows in source CSV
- **Solution:** This is expected, pandas skips bad lines automatically

**Issue: "Pre-commit hook rejects commit message"**
- **Cause:** Commit message doesn't follow conventional format
- **Solution:** Use format: `type: description` (e.g., `feat: add new feature`)

---

## Version History

**Current Version:** 1.1.0 (as of pyproject.toml)

**Key Versions:**
- 1.1.0: Added split by tail number functionality
- 1.0.0: Initial release with core filtering and ATA splitting

See `CHANGELOG.md` for detailed version history.

---

## Additional Resources

### Internal Documentation

- `docs/filter_config_usage.md` - Configuration file guide
- `docs/Variable_list.md` - Column definitions
- `docs/split_by_tail_number.md` - Aircraft splitting guide
- `INSTALL.md` - Setup instructions

### External Resources

- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Poetry Documentation](https://python-poetry.org/docs/)
- [ATA Spec 100](https://en.wikipedia.org/wiki/ATA_Spec_100) - ATA code reference
- [Commitizen](https://commitizen-tools.github.io/commitizen/) - Commit conventions

---

## Contributing Guidelines

When contributing to this repository:

1. **Use conventional commits** (enforced by pre-commit hook)
2. **Add tests** for new functionality
3. **Update documentation** (README, relevant docs/, and this file)
4. **Maintain type safety** with type hints and TypedDict
5. **Follow code conventions** (naming, docstrings, error handling)
6. **Test with both test and full data** before committing

---

**Document Maintenance:**
This CLAUDE.md file should be updated when:
- New features are added
- Project structure changes significantly
- Development workflows change
- New conventions are established

**Last Updated:** 2026-01-15
**Maintainer:** AI Assistant (Claude)
**Repository:** https://github.com/anand-kamble/data-filtering
