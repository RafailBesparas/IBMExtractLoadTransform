# ETL and Web Scraping Project: Largest Banks Data Processing

This project involves an end-to-end pipeline for extracting, transforming, and loading (ETL) data about the largest banks in the world, sourced from a historical snapshot of Wikipedia. The primary objective is to scrape data, transform it into a standardized format, and store it for analysis in CSV and SQL database formats.

# Project Overview

## Data Source:
1. Wikipedia (archived URL): List of Largest Banks.
2. Exchange Rate CSV File: Exchange Rates.

## Key Features:
1. Data Extraction: Web scraping the largest banks' table using Beautiful Soup.
2. Data Transformation: Converting market capitalization values to multiple currencies (GBP, EUR, INR) based on exchange rate data.
3. Data Loading: Saving the processed data to a CSV file and an SQLite database.
4. Query Execution: Running SQL queries for insights, such as the top 10 banks by market capitalization and average market capitalization in GBP.
5. Logging: Maintaining a detailed log of ETL process stages in a log file for monitoring.

## Technologies Used: Python Libraries:
1. BeautifulSoup for web scraping.
2. pandas and numpy for data manipulation.
3. sqlite3 for database operations.
 
## Output:
1. Processed CSV: Largest_banks_data.csv.
2. SQLite Database: Banks.db.

## Project Files Code Files:
1. etl_pipeline.py: Contains the Python script for the ETL pipeline.

## Output Files:
1. Largest_banks_data.csv: CSV file with processed data.
2. Banks.db: SQLite database with the final data table.

## Log File:
1. code_log.txt: Logs ETL process stages.
