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
DATA_DIR = 'yfinance_30day_data_json/'

BQ_DATASET = 'market_data'
BQ_TABLE = 'yf_30days_json'

def upload_json_to_gcs(df, ticker):
    """Upload DataFrame to GCS as newline-delimited JSON, appending to existing data with today's date in filename."""
    client = get_authenticated_storage_client(PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    
    # Create today's filename
    today = datetime.datetime.now().date()
    filename = f"{DATA_DIR}{ticker.replace('^','')}_{today}.json"
    blob = bucket.blob(filename)
    
    # Check if we have any historical data for this ticker (regardless of date)
    ticker_prefix = f"{DATA_DIR}{ticker.replace('^','')}_"
    historical_blobs = list(bucket.list_blobs(prefix=ticker_prefix))
    
    if historical_blobs:
        # Find the most recent blob (regardless of date)
        historical_blobs.sort(key=lambda x: x.time_created, reverse=True)
        latest_blob = historical_blobs[0]
        
        # Download existing content if it's not today's file
        if latest_blob.name != filename:
            existing_content = latest_blob.download_as_string().decode('utf-8').strip()
            
            # Convert new DataFrame to JSON lines
            new_content = df.to_json(orient='records', lines=True)
            
            # Combine with existing content (add newline if needed)
            if existing_content:
                combined_content = existing_content + '\n' + new_content
            else:
                combined_content = new_content
        else:
            # Today's file already exists, append to it
            existing_content = blob.download_as_string().decode('utf-8').strip()
            new_content = df.to_json(orient='records', lines=True)
            combined_content = existing_content + '\n' + new_content if existing_content else new_content
        
        # Upload the combined content to today's filename
        blob.upload_from_string(combined_content, content_type='application/json')
        
        # Delete the old file if it's not today's
        if latest_blob.name != filename:
            latest_blob.delete()
            
        print(f"Updated and consolidated data in {filename}")
    else:
        # No existing data - create new file
        json_data = df.to_json(orient='records', lines=True)
        blob.upload_from_string(json_data, content_type='application/json')
        print(f"Created new file {filename}")
    
    return f"gs://{BUCKET_NAME}/{filename}"

def load_json_to_bigquery(gcs_uri):
    """Load JSON data from GCS to BigQuery."""
    client = get_authenticated_storage_client(PROJECT_ID)
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