from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from modules.stock_crawl import run_pipeline

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
    @task
    def run_pipeline_task():
        run_pipeline()
    
    