# Aircraft Maintenance Data Analysis

This repository contains Jupyter notebooks for analyzing and visualizing aircraft maintenance data from multiple ATA tables and a comprehensive logbook report.

## Table of Contents
- [Overview](#overview)
- [Installation](#installation)
- [Usage](#usage)
- [Merged ATA Visualization Notebook](#merged-ata-visualization-notebook)
- [Data Exploration Notebook](#data-exploration-notebook)
- [Contributing](#contributing)
- [License](#license)

## Overview

This repository contains two notebooks:
1. **Merged ATA Visualization**: Analyzes and visualizes aircraft maintenance data from merged ATA tables.
2. **Data Exploration**: Explores and analyzes the Copa dataset, focusing on fault occurrences, their severity, and actions performed.

## Installation

To run the notebooks, you'll need to install the required Python libraries. You can install them using pip:

```bash
pip install pandas tqdm plotly matplotlib
```

## Usage

1. Open the Jupyter notebooks:

```bash
jupyter notebook
```

2. Navigate to `Merged_ATA_Visualization.ipynb` or `Data_exploration.ipynb` and execute the cells to perform the analysis and generate visualizations.

## Merged ATA Visualization Notebook

### Purpose

This notebook is designed to analyze and visualize aircraft maintenance data from multiple merged ATA tables. It involves loading data from CSV files, processing and cleaning the data, combining individual dataframes, and conducting exploratory data analysis. Visualizations such as heatmaps, pie charts, histograms, and bar charts are used to understand the data better.

### Key Analyses

- Visualization of missing values
- Analysis of fault severity
- Analysis of fault sources
- Distribution analysis by fleet
- Fault occurrence over time

## Data Exploration Notebook

### Purpose

This notebook explores and analyzes the Copa dataset, focusing on understanding fault occurrences, their severity, and actions performed. It involves loading and inspecting data, analyzing ATA codes, examining fault found dates, and exploring fault severity and scheduled actions.

### Key Analyses

- Inspection and summary of dataset
- Frequency analysis of ATA codes
- Distribution of fault found dates
- Analysis of fault severity levels
- Analysis of scheduled actions and their dates

---
