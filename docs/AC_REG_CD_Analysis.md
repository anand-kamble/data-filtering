# Analysis of Aircraft Maintenance Data

## Data Trends Post-2019
The following graph illustrates trends in maintenance data after 2019:

![Aircraft Maintenance After 2019](./imgs/Aircraft_after_2019.png)

**Observations:**
- **Periodic Maintenance Increases:** There is a noticeable increase in maintenance activities, particularly in ATA Chapter 25 (Equipment & Furnishings) and ATA Chapter 05 (Time Limits/Maintenance Checks), approximately every 3-4 months. This periodic surge likely indicates scheduled inspections or recurring maintenance needs.
- **COVID-19 Impact:** A significant drop in maintenance activities during 2020 correlates with the COVID-19 pandemic, reflecting reduced flight operations and aircraft grounding.

## Maintenance Activity Over Time
The graph below provides a comprehensive view of maintenance activity over time:

![All-Time Aircraft Maintenance](./imgs/Aircraft_all_time.png)

**Observations:**
- **Pandemic Effect:** The decline in maintenance activities during 2020 is evident and is likely due to the significant reduction in flight operations and subsequent aircraft grounding during the pandemic.

## Aircraft vs. ATA Chapter Heatmap
### Heatmap Overview
The heatmap below visualizes the relationship between different aircraft and ATA Chapters:

#### Heatmap of Aircraft vs. ATA Chapters
![Heatmap of Aircraft vs. ATA](./imgs/ATAg_vs_TAIL_num_heatmap_normalized.png)

**Key Insights:**
- **Dominant ATA Chapters:** ATA Chapter 25 (Equipment & Furnishings) and ATA Chapter 05 (Time Limits/Maintenance Checks) are the most prevalent across the fleet, indicating critical areas for regular maintenance and frequent fault detection.
- **Sparse Data:** Most cells represent low percentages, highlighting specific combinations of `ATAg` and `AC_REG_CD` with minimal occurrences.

#### Heatmap Excluding Top 5 ATA Chapters
![Heatmap Excluding Top 5 ATA](./imgs/ATAg_vs_TAIL_num_without_top_5_ATA_heatmap_normalized.png)

**Key Insights:**
- **Detailed Patterns:** By excluding the top 5 ATA Chapters, this heatmap reveals detailed patterns in less frequent maintenance activities. It shows a more comprehensive distribution of other ATA Chapters.
- **Anomalies:** Some aircraft show higher occurrences of ATA 00 and ATA 01 faults, indicating possible anomalies or specific maintenance needs that differ from the general trend.

> ### Note:
> - **Normalization:** The heatmaps are normalized across the Y-axis to minimize the effect of varying entry counts per aircraft, ensuring a fair comparison of maintenance activities.
> - **Rendering Limitations:** Not all aircraft are shown in the heatmap due to rendering limitations, which may affect the visibility of some data points.

## Useful Columns for Prediction

After looking at the merged table, we have multiple columns of which following can be useful.  
I have listed all the columns with their data types and a short description. (These descriptions are generated with AI)

| **Column Name**               | **Description**                    | **Data Type**  |
|-------------------------------|------------------------------------|----------------|
| **INV_NO_ID**                 | Investigation Number ID            | Categorical    |
| **AC_REG_CD**                 | Aircraft Registration Code         | Categorical    |
| **VAR_NO_OEM**                | OEM Variant Number                 | Categorical    |
| **LINE_NO_OEM**               | OEM Line Number                    | Categorical    |
| **REVISION_DT**               | Record Revision Date               | Date           |
| **FLEET**                     | Fleet Information                  | Categorical    |
| **SERIAL_NO_OEM**             | Serial Number OEM                  | Categorical    |
| **FAULT_FOUND_DATE**          | Fault Found Date                   | Date           |
| **FAULT_SOURCE**              | Fault Source                       | Categorical    |
| **DEPARTURE_LOCATION**        | Departure Location                 | Categorical    |
| **FAULT_NAME**                | Fault Name                         | Categorical    |
| **FAULT_SDESC**               | Fault Short Description            | Categorical    |
| **CORRECTIVE_ACTION**         | Corrective Action Taken            | Categorical    |
| **MAINT_DELAY_TIME_QT**       | Maintenance Delay Time             | Numerical      |
| **ARRIVAL_LOCATION**          | Arrival Location                   | Categorical    |
| **FAULT_STATUS**              | Fault Status                       | Categorical    |
| **Dt Corrective Action**      | Date of Corrective Action          | Date           |
| **Corrective Action Time**    | Time of Corrective Action          | Time           |
| **ATA**                       | ATA Code                           | Categorical    |
| **FAULT_SEVERITY**            | Fault Severity                     | Categorical    |


## Applications of LLM (Large Language Models)

Large Language Models (LLMs) can significantly enhance the quality and usability of the dataset by performing the following tasks:

1. **Data Cleaning**: For columns like `FAULT_SDESC` and `ATA`, LLMs can be employed to correct grammatical errors, identify and rectify manual data entry mistakes, and ensure consistency across entries.

2. **Error Detection**: LLMs can spot and suggest corrections for spelling mistakes, improper codes, and other inaccuracies that may occur during data entry.

3. **Standardization**: LLMs can help in standardizing the terminology and format of entries to maintain uniformity across the dataset, making it easier to analyze and interpret.
