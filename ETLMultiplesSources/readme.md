# Project Title: Multi-Format Data ETL Pipeline

This project is an Extract-Transform-Load (ETL) pipeline designed to process data from multiple file formats (CSV, JSON, XML), transform the data into a standardized format, and save it to a CSV file for further use. The pipeline also incorporates logging functionality to track the progress of the ETL workflow.

#### Description

1. Extraction:
Reads data from CSV, JSON, and XML files located in the current directory.
Extracts relevant information (name, height, and weight) into a unified pandas DataFrame.
2. Transformation:
Converts height from inches to meters (rounded to two decimal places).
Converts weight from pounds to kilograms (rounded to two decimal places).
3. Loading:
Saves the transformed data into a CSV file (transformed_data.csv) for downstream analysis.
4. Logging:
Logs progress messages at each phase of the ETL process (start and completion of extraction, transformation, and loading) in a log file (log_file.txt) with timestamps.

#### Features Multi-Format Support:
Can process data from CSV, JSON, and XML files seamlessly.

#### Data Transformation:
Normalizes units of measurement to standard metric values for consistency.

#### Logging:
Maintains an activity log for better traceability and debugging.

#### Automation:
Automatically detects and processes all relevant files in the directory.

#### Technologies Used: Python Libraries:
1. pandas: For data manipulation and transformation.
2. glob: To locate files matching specific patterns (e.g., .csv, .json, .xml).
3. xml.etree.ElementTree: To parse XML files.
4. datetime: For generating timestamps in logs.

#### Outputs: Transformed Data:
1. A CSV file (transformed_data.csv) containing the following columns:
2. name: Name of the person.
3. height: Height in meters.
4. weight: Weight in kilograms.

#### Log File:
A log file (log_file.txt) that records the start and end of each ETL phase with timestamps.

#### Workflow Run the Script:
Executes the ETL pipeline, processes the files in the directory, and generates outputs.

#### Review Logs:
Open log_file.txt to monitor the progress and completion of each ETL phase.

#### Analyze Transformed Data:
1. Use the transformed_data.csv for downstream analysis or integration with other tools.

#### Potential Use Cases
1. Data normalization for multi-source datasets.
2. Preparing data for analytics or visualization pipelines.
3. Data integration for systems that consume standardized metrics.
