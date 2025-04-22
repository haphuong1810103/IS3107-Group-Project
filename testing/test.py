from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
import json
import yfinance as yf
import pandas as pd
from google.cloud import storage, bigquery
from dotenv import load_dotenv

# === Load environment ===
load_dotenv()
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# === Config ===
TICKERS = ['^GSPC', 'DJIA', '^NDX', 'BTC-USD', 'DOGE-USD']
BUCKET_NAME = 'yfinance-data'
DATA_DIR = 'yfinance_daily_data_json/'
BLOB_NAME = 'combined.json'
BQ_DATASET = 'market_data'
BQ_TABLE = 'yf_daily_json'

@dag(schedule_interval="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["yfinance", "GCS", "BigQuery"])
def yf_to_bq_pipeline():
    @task()
    def get_latest_data_date():
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"{DATA_DIR}{BLOB_NAME}")

        if not blob.exists():
            return "2023-01-01"

        content = blob.download_as_string().decode('utf-8').strip().split('\n')

        dates = []
        for line in content:
            if line.strip():
                try:
                    record = json.loads(line)
                    dates.append(record['Date'])
                except Exception:
                    continue

        if not dates:
            return "2025-03-01"

        latest_date = max(pd.to_datetime(dates))
        next_day = (latest_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        return next_day

    @task()
    def download_all_ticker_data(start_date: str):
        all_data = []
        end_date = datetime.today().strftime('%Y-%m-%d')

        for ticker in TICKERS:
            print(f"Downloading {ticker} from {start_date} to {end_date}")
            df = yf.download(ticker, start=start_date, end=end_date)
            if df.empty:
                continue
            df = df.reset_index()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] for col in df.columns.values]
            df['Date'] = df['Date'].astype(str)
            df['Ticker'] = ticker
            all_data.append(df)

        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            return df_combined.to_json(orient='records', lines=True)
        return ""

    @task()
    def upload_to_gcs(new_json: str) -> str:
        if not new_json:
            return ""

        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"{DATA_DIR}{BLOB_NAME}")

        if blob.exists():
            existing = blob.download_as_string().decode('utf-8')
            if existing and not existing.endswith('\n'):
                existing += '\n'
            combined = existing + new_json
        else:
            combined = new_json

        blob.upload_from_string(combined, content_type='application/json')
        gcs_uri = f"gs://{BUCKET_NAME}/{DATA_DIR}{BLOB_NAME}"
        print(f"Uploaded to {gcs_uri}")
        return gcs_uri

    @task()
    def load_into_bigquery(gcs_uri: str):
        if not gcs_uri:
            print("No new data to load.")
            return

        client = bigquery.Client()
        table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition="WRITE_APPEND"
        )

        load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()
        print(f"Loaded into BigQuery: {table_ref}")

    # === DAG Flow ===
    latest_date = get_latest_data_date()
    new_data_json = download_all_ticker_data(latest_date)
    gcs_uri = upload_to_gcs(new_data_json)
    load_into_bigquery(gcs_uri)

dag_instance = yf_to_bq_pipeline()
