# Apache Airflow ETL Pipeline for Toll Data

## Overview
This project contains an Apache Airflow DAG designed to process toll data from multiple sources, including CSV, TSV, and fixed-width files. The pipeline extracts, transforms, and consolidates the data into a structured format.

## Features
- **Unzipping Data:** Extracts toll data from a `.tgz` archive.
- **Extracting Data:** Extracts relevant fields from CSV, TSV, and fixed-width files.
- **Consolidating Data:** Combines extracted data from multiple sources.
- **Transforming Data:** Converts vehicle type values to uppercase.
- **Scheduling:** Runs daily as an Airflow DAG.

## File Structure
```
/opt/airflow/dags/finalassignment/staging/
    ├── tolldata.tgz
    ├── vehicle-data.csv
    ├── tollplaza-data.tsv
    ├── payment-data.txt
    ├── csv_data.csv
    ├── tsv_data.csv
    ├── fixed_width_data.csv
    ├── extracted_data.csv
    ├── transformed_data.csv
```

## DAG Configuration
The DAG is defined with the following default parameters:
- **Owner:** ENA
- **Start Date:** Current date
- **Retries:** 1 retry with a 5-minute delay
- **Email Alerts:** Notifications on failure and retry attempts
- **Schedule:** Runs daily (`@daily`)

## Dependencies and Execution Flow
1. `unzip_data`: Extracts the `.tgz` archive.
2. `extract_data_from_csv`: Extracts columns from `vehicle-data.csv`.
3. `extract_data_from_tsv`: Extracts specific fields from `tollplaza-data.tsv`.
4. `extract_data_from_fixed_width`: Extracts columns from `payment-data.txt`.
5. `consolidate_data`: Merges all extracted data into a single CSV file.
6. `transform_data`: Converts vehicle type values to uppercase.

## Running the DAG
To run the DAG in Apache Airflow:
1. Place the DAG script in the Airflow DAGs folder.
2. Ensure that the necessary data files exist in the `staging` directory.
3. Start the Airflow scheduler and web server.
4. Enable and trigger the DAG via the Airflow UI or CLI.

## Requirements
- Apache Airflow installed and configured.
- Required data files present in the staging directory.
- Bash and `awk` installed for command-line data processing.
