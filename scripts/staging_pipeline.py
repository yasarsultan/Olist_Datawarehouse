import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import glob
import os
from dotenv import load_dotenv

load_dotenv()


# Authenticating and initializing the BigQuery client
credentials = service_account.Credentials.from_service_account_file('service-account-file.json') # Change the file parameter with your Google Cloud service account file
project_id = os.getenv('PROJECT_ID')  # Change PROJECT_ID to your Google Cloud project id
client = bigquery.Client(credentials=credentials, project=project_id)

def load_to_bigquery(df, table_id, schema):
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result() # Wait till job is completed
    print(f"Loaded {job.output_rows} rows into {table_id}.")


schemas = {
    # "customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"
    'customers.csv': [
        bigquery.SchemaField('customer_id', 'STRING', mode='REQUIRED', description='Unique identifier for the customer'),
        bigquery.SchemaField('customer_unique_id', 'STRING', mode='REQUIRED', description='Unique identifier for the customer across systems'),
        bigquery.SchemaField('customer_zip_code_prefix', 'INTEGER'),
        bigquery.SchemaField('customer_city', 'STRING'),
        bigquery.SchemaField('customer_state', 'STRING')
    ],
    # "geolocation_zip_code_prefix","geolocation_lat","geolocation_lng","geolocation_city","geolocation_state"
    'geolocation.csv': [
        bigquery.SchemaField('geolocation_zip_code_prefix', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('geolocation_lat', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('geolocation_lng', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('geolocation_city', 'STRING'),
        bigquery.SchemaField('geolocation_state', 'STRING'),
    ],
    # "order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"
    'order_items.csv': [
        bigquery.SchemaField('order_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('order_item_id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('product_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('seller_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('shipping_limit_date', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('price', 'FLOAT', mode='REQUIRED'),
        bigquery.SchemaField('freight_value', 'FLOAT', mode='REQUIRED'),
    ],
    # "order_id","payment_sequential","payment_type","payment_installments","payment_value"
    'order_payments.csv': [
        bigquery.SchemaField('order_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('payment_sequential', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('payment_type', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('payment_installments', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('payment_value', 'FLOAT', mode='REQUIRED'),
    ],
    # "review_id","order_id","review_score","review_comment_title",
    # "review_comment_message","review_creation_date","review_answer_timestamp"
    'order_reviews.csv': [
        bigquery.SchemaField('review_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('order_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('review_score', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('review_comment_title', 'STRING'),
        bigquery.SchemaField('review_comment_message', 'STRING'),
        bigquery.SchemaField('review_creation_date', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('review_answer_timestamp', 'STRING', mode='REQUIRED'),
    ],
    # "order_id","customer_id","order_status","order_purchase_timestamp","order_approved_at",
    # "order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"
    'orders.csv': [
        bigquery.SchemaField('order_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('customer_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('order_status', 'STRING'),
        bigquery.SchemaField('order_purchase_timestamp', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('order_approved_at', 'STRING'),
        bigquery.SchemaField('order_delivered_carrier_date', 'STRING'),
        bigquery.SchemaField('order_delivered_customer_date', 'STRING'),
        bigquery.SchemaField('order_estimated_delivery_date', 'STRING'),
    ],
    # "product_id","product_category_name","product_name_lenght","product_description_lenght",
    # "product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm"
    'products.csv': [
        bigquery.SchemaField('product_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('product_category_name', 'STRING'),
        bigquery.SchemaField('product_name_lenght', 'INTEGER'),
        bigquery.SchemaField('product_description_lenght', 'INTEGER'),
        bigquery.SchemaField('product_photos_qty', 'INTEGER'),
        bigquery.SchemaField('product_weight_g', 'INTEGER'),
        bigquery.SchemaField('product_length_cm', 'INTEGER'),
        bigquery.SchemaField('product_height_cm', 'INTEGER'),
        bigquery.SchemaField('product_width_cm', 'INTEGER'),
    ],
    
    'sellers.csv': [
        bigquery.SchemaField('seller_id', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('seller_zip_code_prefix', 'INTEGER'),
        bigquery.SchemaField('seller_city', 'STRING'),
        bigquery.SchemaField('seller_state', 'STRING'),
    ],

    'product_category_name_translation.csv': [
        bigquery.SchemaField('product_category_name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('product_category_name_english', 'STRING', mode='REQUIRED'),
    ]
}


# Reading and loading data
data_files = glob.glob('olist/*.csv')

for file in data_files:
    df = pd.read_csv(file)
    file_name = os.path.basename(file)
    dataset_id = os.getenv('DATASET_ID')# Change DATASET_ID to your Google Cloud BigQuery dataset id
    table_id = f'{dataset_id}.{file_name.split(".")[0]}' # 
    
    if file_name in schemas:
        try:
            schema = schemas[file_name]
            load_to_bigquery(df, table_id, schema)
        except Exception as e:
            print(f"Error loading {file_name} to BigQuery: {e}")
        continue
    else:
        print(f"No schema defined for {file_name}. So skipping it...")