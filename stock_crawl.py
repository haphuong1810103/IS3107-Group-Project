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

# Google credentials setup (set your path here)
load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')  # ← CHANGE THIS
print(f"Loaded PROJECT_ID: {PROJECT_ID}") 


# === DATE RANGE ===
START_DATE = datetime.datetime(2025, 3, 1)
END_DATE = datetime.datetime.now()

# === FUNCTIONS ===

def download_data(ticker):
    df = yf.download(ticker, start=START_DATE, end=END_DATE)
    df = df.reset_index()

    #If MultiIndex, flatten by combining levels with underscore
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns.values]

    df['Date'] = df['Date'].astype(str)

    df['Ticker'] = ticker
    return df

def upload_json_to_gcs(df, ticker):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    filename = f"{DATA_DIR}{ticker.replace('^','')}_{datetime.datetime.now().date()}.json"
    blob = bucket.blob(filename)

    # Convert DataFrame to newline-delimited JSON (one record per line)
    json_data = df.to_json(orient='records', lines=True)
    blob.upload_from_string(json_data, content_type='application/json')

    print(f"Uploaded {filename} to GCS bucket {BUCKET_NAME}")
    return f"gs://{BUCKET_NAME}/{filename}"

def load_json_to_bigquery(gcs_uri):
    client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_APPEND"
    )

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Wait until finished


def run_pipeline():
    for ticker in TICKERS:
        df = download_data(ticker)
        if not df.empty:
            gcs_uri = upload_json_to_gcs(df, ticker)
            load_json_to_bigquery(gcs_uri)
if __name__ == "__main__":
    run_pipeline()