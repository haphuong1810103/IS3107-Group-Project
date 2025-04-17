import yfinance as yf
import pandas as pd
import datetime
import json
from google.cloud import secretmanager, storage
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

# === FUNCTIONS ===
def get_tickers():
    """Return the list of tickers to download."""
    return TICKERS

def get_latest_data_date(ticker):
    """Find the latest date for which we have data for the given ticker."""
    client = get_authenticated_storage_client(PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    
    # List all blobs for this ticker
    ticker_prefix = f"{DATA_DIR}{ticker.replace('^','')}_"
    blobs = list(bucket.list_blobs(prefix=ticker_prefix))
    
    if not blobs:
        # If no data exists, use the default start date
        return datetime.datetime(2025, 3, 1)
    
    latest_date = datetime.datetime(2025, 3, 1)  # Default start date
    
    for blob in blobs:
        # For each file, check the most recent data point inside
        content = blob.download_as_string()
        
        # Parse each JSON line
        json_lines = content.decode('utf-8').strip().split('\n')
        if json_lines:
            # Get dates from all records
            dates = []
            for line in json_lines:
                record = json.loads(line)
                dates.append(datetime.datetime.strptime(record['Date'], '%Y-%m-%d'))
            
            # Find the max date in this file
            if dates:
                file_max_date = max(dates)
                if file_max_date > latest_date:
                    latest_date = file_max_date
    
    # Add one day to start from the next day
    return latest_date + datetime.timedelta(days=1)

def download_data(ticker, start_date, end_date):
    """Download data for the specified ticker and date range."""
    if start_date >= end_date:
        print(f"No new data to download for {ticker} (start_date: {start_date}, end_date: {end_date})")
        return pd.DataFrame()
    
    print(f"Downloading {ticker} data from {start_date} to {end_date}")
    df = yf.download(ticker, start=start_date, end=end_date)
    
    if df.empty:
        print(f"No data available for {ticker} in the requested date range")
        return df
    
    df = df.reset_index()

    # If MultiIndex, flatten by combining levels with underscore
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns.values]

    df['Date'] = df['Date'].astype(str)
    df['Ticker'] = ticker
    return df