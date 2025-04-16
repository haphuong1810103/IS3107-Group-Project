from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from modules.stock_crawl import get_tickers, get_latest_data_date, download_data
from modules.upload_data import upload_json_to_gcs, load_json_to_bigquery
from google.cloud import storage, bigquery
from io import StringIO
import pandas as pd
import json
import os
from dotenv import load_dotenv

#
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
        print("Initializing the DAG...")
    
    @task_group(group_id='download_data')
    def download_data_group():
        @task(task_id='yfinance_fetch_tickers')
        def fetch_tickers():
            return get_tickers()
        
        @task(task_id='yfinance_get_latest_data_date')
        def get_latest_data_date(ticker):
            return get_latest_data_date(ticker)
        
        @task(task_id='yfinance_download_data')
        def download_data(ticker, start_date):
            return download_data(ticker, start_date)
        
        @task(task_id='yfinance_upload_json_to_gcs')
        def upload_json_to_gcs(df, ticker):
            """Upload DataFrame to GCS as newline-delimited JSON."""
            return upload_json_to_gcs(df, ticker)
        
        @task(task_id='yfinance_load_json_to_bigquery')
        def load_json_to_bigquery(gcs_uri):
            """Load JSON data from GCS to BigQuery."""
            return load_json_to_bigquery(gcs_uri)

        tickers = fetch_tickers()

        for ticker in tickers:
            start_date = get_latest_data_date(ticker)
            df = download_data(ticker, start_date)
            
            if not df.empty:
                gcs_uri = upload_json_to_gcs(df, ticker)
                load_json_to_bigquery(gcs_uri)
            else: 
                print(f"No new data for {ticker} since {start_date}. Skipping upload.")
    
    @task(task_id='end')
    def end():
        print("DAG execution completed.")
    
    initialize() >> download_data_group() >> end()

dag_instance = fin_dag()