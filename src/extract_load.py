# src/load_data.py
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

# Setup credentials and client
credentials = service_account.Credentials.from_service_account_file(
    'noorah-003be3dc481f.json'
)
client = bigquery.Client(credentials=credentials, project='noorah')

print('client : ', client)

# Load data from Google Sheets using pandas
# sheet_url = "https://docs.google.com/spreadsheets/d/1XC0YaSQ4WjLwhzCB96RxF23-NjFga1Fisr-9lX_7hmk/export?format=csv&gid={}"
# messages_gid = '1033608769'
# statuses_gid = '966707183'

# Load data from excel sheet
excel_path = '/Users/sarath/Desktop/workspace/Personal/Noora/Data Engineer Task Assignment.xlsx'
messages = pd.read_excel(excel_path, sheet_name='Messages')
statuses = pd.read_excel(excel_path, sheet_name='Statuses')

# messages = pd.read_csv(sheet_url.format(messages_gid))
# statuses = pd.read_csv(sheet_url.format(statuses_gid))

print(f"messages shape : {messages.shape}")
print(f"statuses shape : {statuses.shape}")

# Define dataset and table names
dataset_id = 'noorah.noora_data'
messages_table_id = f"{dataset_id}.raw_messages"
statuses_table_id = f"{dataset_id}.raw_statuses"

# Check and create dataset if it doesn't exist
try:
    client.get_dataset(dataset_id)  # Try to fetch the dataset
except NotFound:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"  # Or your preferred location
    client.create_dataset(dataset)
    print(f"Created dataset {dataset_id}")

# Upload to BigQuery
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
client.load_table_from_dataframe(messages, messages_table_id, job_config=job_config).result()
client.load_table_from_dataframe(statuses, statuses_table_id, job_config=job_config).result()
print("Data loaded successfully into BigQuery.")