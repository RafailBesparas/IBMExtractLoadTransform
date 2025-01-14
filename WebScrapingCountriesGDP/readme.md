# Project Overview
This project is an Extract-Transform-Load (ETL) pipeline for analyzing and processing global GDP data from Wikipedia. The project automates the process of extracting GDP information from a web page, transforming it into a clean and standardized format, and saving it into a CSV file and a database. It also includes functionality for querying the data to derive meaningful insights.

#### Description
The project demonstrates a complete ETL pipeline using Python, with the following steps:

##### Extraction:
1. Web scraping using the requests library and BeautifulSoup to retrieve GDP data from the Wikipedia page (archived URL).
2. Extracts relevant attributes: country names and their respective GDP values in USD (millions).

##### Transformation:
1. Converts GDP values from USD (millions) to USD (billions) for better readability and analysis.
2. Cleans the data by removing invalid entries and ensuring consistent formatting.

##### Loading:
1. Saves the processed data to a CSV file for easy sharing and downstream processing.
2. Loads the data into an SQLite database for advanced querying and persistent storage.

##### Querying:
1. Executes SQL queries to retrieve information, such as countries with a GDP greater than or equal to 100 billion USD.

##### Logging:
1. Implements a logging mechanism to track progress and ensure traceability of all stages of the pipeline.

#### Key Features
1. Automation: Fully automated ETL pipeline for streamlined data processing.
2 .Scalability: Easily extendable to handle additional attributes or datasets.
3. Database Integration: Stores data in an SQLite database for efficient querying.
4. Error Handling: Incorporates basic error checks for missing or malformed data.
5. Data Transformation: Provides a standardized format for GDP data to facilitate analysis.

#### Outputs
1. CSV File: Countries_by_GDP.csv containing:
2. Country: Country name.
3. GDP_USD_billions: GDP in billions of USD.
4. Database: SQLite database (World_Economies.db) with a table Countries_by_GDP containing the same attributes.

#### Technologies Used
1. Python Libraries: requests, BeautifulSoup, pandas, numpy, sqlite3, datetime.
2. Data Storage: SQLite database and CSV file.

#### Potential Use Cases
1. Economic analysis and reporting.
2. Comparative studies of GDP among countries.
3. Integration with dashboards for visual analytics.
