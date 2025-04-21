import yfinance as yf
import pandas as pd
from datetime import datetime
import os
import json
from google.cloud import storage, bigquery, secretmanager
from google.oauth2 import service_account
import json
import os
from stock_crawl import get_tickers, get_latest_data_date, download_data
from dotenv import load_dotenv

load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS') 

# === CONFIGURATION ===
TICKERS = ['^GSPC', 'DJIA', '^NDX', 'BTC-USD', 'DOGE-USD']
BUCKET_NAME = 'yfinance-data'
DATA_DIR = 'yfinance_daily_data_json/'

BQ_DATASET = 'market_data'
BQ_TABLE = 'yf_daily_json'

def upload_json_to_gcs(df, ticker):
    """Upload DataFrame to GCS as newline-delimited JSON."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    ticker_safe = ticker.replace('^','')
    filename = f"{DATA_DIR}{ticker_safe}.json"
    blob = bucket.blob(filename)
    
    new_content = df.to_json(orient='records', lines=True)
    
    if blob.exists():
        existing_content = blob.download_as_string().decode('utf-8')
        
        if existing_content and not existing_content.endswith('\n'):
            existing_content += '\n'
        
        combined_content = existing_content + new_content
        blob.upload_from_string(combined_content, content_type='application/json')
        print(f"Appended new data to {filename} in GCS bucket {BUCKET_NAME}")
    else:
        blob.upload_from_string(new_content, content_type='application/json')
        print(f"Created new file {filename} in GCS bucket {BUCKET_NAME}")

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

voo_latest_date = get_latest_data_date("VOO")
ticker_data = download_data("VOO", voo_latest_date, datetime.now())
gcs_uri = upload_json_to_gcs(ticker_data, "VOO")
print("Loaded to GCS")
load_json_to_bigquery(gcs_uri)
print("Loaded to BigQuery")