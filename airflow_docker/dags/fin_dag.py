from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from modules.stock_crawl import get_tickers, get_latest_data_date, download_data
from modules.upload_data import upload_json_to_gcs, load_json_to_bigquery
from google.cloud import storage, bigquery
from io import StringIO
import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 3,  # Set retries to 3
    'retry_delay': timedelta(minutes=5),  # Delay between retries
    'email': ['nigeltanjerkang@gmail.com'],
    'email_on_failure': True,  # Send email on task failure
    'email_on_retry': False,  # Optionally, set to True if you want emails on retry as well
}

@dag(dag_id='fin_dag', default_args=default_args, schedule_interval='@daily', catchup=False)
def fin_dag():
    @task(task_id='initialize')
    def initialize():
        return "Initializing the DAG"
    
    @task_group(group_id='download_data')
    def download_data_group():
        @task(task_id='yfinance_fetch_tickers')
        def fetch_tickers():
            return get_tickers()
        
        @task(task_id='yfinance_get_latest_date')
        def get_latest_date(ticker):
            return get_latest_data_date(ticker)
        
        @task(task_id='yfinance_download_data')
        def download_and_upload(ticker, start_date):
            start_date = start_date.strftime('%Y-%m-%d')
            end_date = datetime.today().strftime('%Y-%m-%d')

            df = download_data(ticker, start_date, end_date)

            if df.empty:
                print(f"No new data for {ticker} since {start_date}. Skipping upload.")
                return None
            
            gcs_uri = upload_json_to_gcs(df, ticker)
            return gcs_uri
        
        @task(task_id='yfinance_load_json_to_bigquery')
        def load_json_to_bigquery(gcs_uri):
            """Load JSON data from GCS to BigQuery."""
            return load_json_to_bigquery(gcs_uri)

        tickers = fetch_tickers()
        start_dates = get_latest_date.expand(ticker=tickers)
        gcs_uris = download_and_upload.expand(ticker=tickers, start_date=start_dates)
        load_json_to_bigquery.expand(gcs_uri=gcs_uris)
    
    @task(task_id='end')
    def end():
        return "DAG execution completed."
    
    initialize() >> download_data_group() >> end()

dag_instance = fin_dag()