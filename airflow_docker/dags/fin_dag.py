import pandas as pd
import json
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from modules.stock_crawl import get_tickers, get_latest_data_date, download_data
from modules.upload_data import upload_json_to_gcs, load_json_to_bigquery
from airflow.exceptions import AirflowException

# Set up environment
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 2,  # Set retries to 2
    'retry_delay': timedelta(seconds=30),  # Delay between retries
    'email': ['nigeltanjerkang@gmail.com'],
    'email_on_failure': False,  # Send email on task failure
    'email_on_retry': False,  # Optionally, set to True if you want emails on retry as well
}

@dag(dag_id='fin_dag', default_args=default_args, schedule_interval="@daily", catchup=False)
def fin_dag():    
    @task(task_id='fetch_tickers')
    def fetch_ticker_list():
        return get_tickers()

    @task(task_id='get_latest_dates')
    def get_latest_dates(ticker_list):
        return get_latest_data_date(ticker_list)  # returns dict {ticker: latest_date}

    @task(task_id='download_upload_all')
    def download_upload_all(ticker_start_dates: dict):
        try:
            combined_df = download_data(ticker_start_dates)
        except Exception as e:
            raise AirflowException(f"Error during data download: {e}")
        
        if not combined_df.empty:
            try:
                gcs_uri = upload_json_to_gcs(combined_df)
                load_json_to_bigquery(gcs_uri)
                return "Upload successful"
            except Exception as e:
                raise AirflowException(f"Error during upload: {e}")
        else:
            print("No new data to upload.")
            return "No data"

    tickers = fetch_ticker_list()
    latest_dates = get_latest_dates(tickers)
    download_upload_all(latest_dates)

dag_instance = fin_dag()