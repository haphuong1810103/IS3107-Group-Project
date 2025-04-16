import yfinance as yf
import pandas as pd
import datetime
from google.cloud import storage, bigquery
import os
import json
from dotenv import load_dotenv

# === CONFIGURATION ===
TICKERS = ['^GSPC', 'DJIA', '^NDX', 'BTC-USD', 'DOGE-USD']
BUCKET_NAME = 'yfinance-data'
DATA_DIR = 'yfinance_30day_data_json/'

BQ_DATASET = 'market_data'
BQ_TABLE = 'yf_30days_json'

# Google credentials setup
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')


def upload_json_to_gcs(df, ticker):
    """Upload DataFrame to GCS as newline-delimited JSON."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    today = datetime.datetime.now().date()
    filename = f"{DATA_DIR}{ticker.replace('^','')}_{today}.json"
    blob = bucket.blob(filename)
    
    # Check if the blob already exists
    if blob.exists():
        # If it exists, download and append to it
        existing_content = blob.download_as_string().decode('utf-8')
        if existing_content and not existing_content.endswith('\n'):
            existing_content += '\n'
        
        # Convert new DataFrame to newline-delimited JSON
        new_content = df.to_json(orient='records', lines=True)
        
        # Combine existing and new content
        combined_content = existing_content + new_content
        blob.upload_from_string(combined_content, content_type='application/json')
    else:
        # If it doesn't exist, create a new file
        json_data = df.to_json(orient='records', lines=True)
        blob.upload_from_string(json_data, content_type='application/json')

    print(f"Uploaded {filename} to GCS bucket {BUCKET_NAME}")
    return f"gs://{BUCKET_NAME}/{filename}"

def load_json_to_bigquery(gcs_uri):
    """Load JSON data from GCS to BigQuery."""
    client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Wait until finished
    print(f"Loaded data from {gcs_uri} to BigQuery table {table_ref}")
    return table_ref