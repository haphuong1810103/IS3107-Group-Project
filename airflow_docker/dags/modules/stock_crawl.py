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
TICKERS = ['^GSPC', '^DJI', '^NDX', 'BTC-USD', 'DOGE-USD']
BUCKET_NAME = 'yfinance-data'
DATA_DIR = 'yfinance_daily_data_json/'

BQ_DATASET = 'market_data'
BQ_TABLE = 'yf_daily_json'

# === FUNCTIONS ===
def get_tickers():
    """Return the list of tickers to download."""
    return TICKERS

def get_latest_data_date(tickers):
    """Find the latest date for which we have data for the given ticker."""
    client = get_authenticated_storage_client(PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob_name = f"{DATA_DIR}stock_data.json"
    blob = bucket.blob(blob_name)
    
    # Initialize with default dates
    result = {ticker: datetime.datetime(2023, 1, 1) for ticker in tickers}
    
    if not blob.exists():
        return result
    
    content = blob.download_as_string()
    json_lines = content.decode('utf-8').strip().split('\n')
    
    for line in json_lines:
        if line.strip():  
            try:
                record = json.loads(line)
                ticker = record.get('Ticker') 
                if ticker in tickers:
                    date = datetime.datetime.strptime(record['Date'], '%Y-%m-%d')
                    if date > result[ticker]:
                        result[ticker] = date
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing line in {blob_name}: {e}")
                continue
    
    # Add one day to get the start date for new data
    for ticker in result:
        result[ticker] = result[ticker] + datetime.timedelta(days=1)

    return result

def download_data(ticker_start_dates):
    """Download and return combined stock data for multiple tickers from their respective start dates to today."""
    end_date = datetime.datetime.today().date()
    all_data = []

    for ticker, start_date in ticker_start_dates.items():
        if isinstance(start_date, datetime.datetime):
            start_date = start_date.date()

        if start_date >= end_date:
            print(f"No new data to download for {ticker} (start_date: {start_date}, end_date: {end_date})")
            continue

        print(f"Downloading {ticker} data from {start_date} to {end_date}")
        df = yf.download(ticker, start=start_date, end=end_date)

        if df.empty:
            print(f"No data available for {ticker} in the requested date range")
            continue

        df = df.reset_index()

        # If MultiIndex, flatten by taking the top-level column names
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns.values]

        df['Date'] = df['Date'].astype(str)
        df['Ticker'] = ticker
        all_data.append(df)

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()