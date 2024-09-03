import os
import glob
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up env variables
project_id = os.getenv('PROJECT_ID')
stg_dataset_id = os.getenv('DATASET_ID')
prod_dataset_id = os.getenv('PROD_DATASET_ID')

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_file('service-account-file.json')
client = bigquery.Client(credentials=credentials, project=project_id)

# List of SQL files to execute
sql_files = glob.glob('schemas/*.sql')

# Function to read and execute SQL files
def execute_sql(fileName):
    with open(fileName, 'r') as file:
        sql_query = file.read().format(prod_dataset=prod_dataset_id, stg_dataset=stg_dataset_id)
        client.query(sql_query).result()
        print(f'Executed {fileName}')

# Execute all SQL files
for sql_file in sql_files:
    execute_sql(sql_file)