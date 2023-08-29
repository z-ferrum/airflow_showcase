# airflow_showcase

This repo is a collection of small Airflow project. 1 dag file corresponds to 1 project, some project have a dedicated folder, named after the dag file.

All projects are based on Airflow configuration with LocalExecutor, Postgres as backend and Redis as broker.
All projects are run and tested on Windows 11 within a WSL2 Ubuntu 20.04 LTS subsystem and a python virtual environment (left outside of the repo).
All dependencies are listed in the requirements.txt file.

Disclaimers:
- This repo is a work in progress, some projects are not finished yet.
- Some design choices were made to play with airflow functionalities amd may not be the best practice for production.


New project ideas:

Exchange Rate Data Pipeline
Description: Create an Airflow DAG that fetches exchange rates from a public API like Open Exchange Rates or European Central Bank and stores them in Snowflake. Use DBT to transform this data for analytics.

    Modular tasks: The tasks in the pipeline are modular and perform one specific operation.
    Retry Mechanisms: The DAG includes a basic retry mechanism in case of failure.
    XCom for Task Communication: We use XCom to pass data between tasks, which is useful for passing small volumes of data.

Financial Portfolio Management
Description: Create an Airflow DAG that fetches data from various financial APIs to track stock, ETF, or cryptocurrency prices. Load this data into Snowflake and use DBT for portfolio analysis.

Web Scraping for Job Listings
Description: Develop a DAG to scrape job listings from websites like Indeed or LinkedIn and store them in Snowflake. Use DBT to provide analytics like average salaries, required skills, etc.