from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from modules.stock_crawl import get_tickers, get_latest_data_date, download_data
from modules.upload_data import upload_json_to_gcs, load_json_to_bigquery
from google.cloud import storage, bigquery
from io import StringIO
import pandas as pd
import json
import os

#
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 2,  # Set retries to 2
    'retry_delay': timedelta(minutes=1),  # Delay between retries
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
    def download_dump_data():
        @task(task_id='yfinance_fetch_tickers')
        def fetch_tickers():
            return get_tickers()
        
        @task(task_id='yfinance_get_latest_data_date')
        def get_latest_data_dates(ticker_list):
            return [{"ticker": ticker, "latest_date": get_latest_data_date(ticker)} for ticker in ticker_list]

        @task(task_id='yfinance_download_data')
        def download_and_upload_data(ticker, start_date):
            end_date = datetime.now()
            ticker_data = download_data(ticker, start_date, end_date)
            
            if not ticker_data.empty:
                gcs_uri = upload_json_to_gcs(ticker_data, ticker)
                load_json_to_bigquery(gcs_uri)
            else:
                print(f"No new data for {ticker} since {start_date}. Skipping upload.")
        
        tickers = fetch_tickers()
        latest_data_dates = get_latest_data_dates(tickers)
        download_and_upload_data.expand_kwargs(latest_data_dates)


    @task(task_id='end')
    def end():
        return "DAG execution completed."
    
    initialize() >> download_dump_data() >> end()

dag_instance = fin_dag()