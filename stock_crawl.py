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
DATA_DIR = 'yfinance_daily_data_json/'

BQ_DATASET = 'market_data'
BQ_TABLE = 'yf_daily_json'

# Google credentials setup
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')


def get_latest_data_date(ticker):
    """Find the latest date for which we have data for the given ticker."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    ticker_safe = ticker.replace('^','')
    blob_name = f"{DATA_DIR}{ticker_safe}.json"
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        return datetime.datetime(2023, 1, 1)
    
    content = blob.download_as_string()
    json_lines = content.decode('utf-8').strip().split('\n')
    
    if not json_lines:
        return datetime.datetime(2025, 3, 1)
    
    # Get dates from all records
    dates = []
    for line in json_lines:
        if line.strip():  
            try:
                record = json.loads(line)
                dates.append(datetime.datetime.strptime(record['Date'], '%Y-%m-%d'))
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing line in {blob_name}: {e}")
                continue
    
    if not dates:
        return datetime.datetime(2025, 3, 1)
    
    # Return the day after the most recent date
    return max(dates) + datetime.timedelta(days=1)

def download_data(ticker, start_date, end_date):
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

def run_pipeline():
    """Run the data pipeline for all tickers."""
    current_date = datetime.datetime.now()
    
    for ticker in TICKERS:
        # Get the latest date for which we have data
        start_date = get_latest_data_date(ticker)
        
        # Download only new data
        df = download_data(ticker, start_date, current_date)
        
        if not df.empty:
            gcs_uri = upload_json_to_gcs(df, ticker)
            load_json_to_bigquery(gcs_uri)
        else:
            print(f"No new data for {ticker}")

if __name__ == "__main__":
    run_pipeline()