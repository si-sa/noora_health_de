# Noora Health Data Engineering Pipeline

## Overview
This project builds a data pipeline to extract the data from excel and load into bigquery for processing WhatsApp message and status data from Noora Health’s intervention.
It includes analysis and visualization to key metrics as well.

## Project Structure
    ├── noorav/ # Python virtual environment
    ├── requirements.txt # Python dependencies
    ├── src/extract_load.py # Python scripts for data loading and processing
    ├── sql/ # SQL scripts for transformation and validation
    ├── README.md # Project documentation
    ├── .gitignore # Files to ignore before pushing to git
    ├── Data Engineer Task Assignment.xlsx
    └── noorah-003be3dc481f.json  # Service account key to access bigquery via its API


## Setup

1. Create and activate a Python virtual environment:
    python3 -m venv noorav
    source noorav/bin/activate

2. Install dependencies:
    pip install -r requirements.txt

3. Set Google Cloud credentials to extract and load excel data into BigQuery:
    - Registered a Google Cloud account
    - Created a Google Cloud project, BigQuery API enabled, created a service account.
    - Granted bigquery-admin role to the service account to use BQ API
    - Generate and placed service account JSON key file in the current project directory.
    - Updated path in src/extract_load.py to use the BQ API via the service account.

## USAGE
    To load data from excel and upload to BigQuery,
        run: python src/extract_load.py
    
    - Use the SQL scripts in sql/ to transform and validate data in BigQuery.
    - Run visualizations in looker based on queries in sql/visualize.sql
