import yfinance as yf
import pandas as pd
import datetime
import os
import json
from google.cloud import storage, bigquery, secretmanager
from google.oauth2 import service_account
import json
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

def get_authenticated_storage_client(project_id: str) -> storage.Client:
    """
    Creates authenticated GCS client using credentials from Secret Manager
    
    Args:
        project_id: GCP project ID containing the secret
        
    Returns:
        Authenticated storage client
    """
    secret_json = access_secret_version(project_id, "is3107-key", "latest")
    credentials_info = json.loads(secret_json)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return storage.Client(credentials=credentials, project=project_id)

def access_secret_version(project_id: str, secret_id: str, version_id: str) -> str:
    """Helper function to access secret version"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# === CONFIGURATION ===
TICKERS = ['^GSPC', 'DJIA', '^NDX', 'BTC-USD', 'DOGE-USD']
BUCKET_NAME = 'yfinance-data'
DATA_DIR = 'yfinance_daily_data_json/'

BQ_DATASET = 'market_data'
BQ_TABLE = 'yf_daily_json'

def upload_json_to_gcs(df, ticker):
    """Upload DataFrame to GCS as newline-delimited JSON, appending to existing file."""
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