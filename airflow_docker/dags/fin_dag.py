import pandas as pd
import json
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from modules.get_stock_tickers import get_largest_companies_fmp, get_top_50_tickers
from modules.stock_crawl import get_latest_data_date, download_data
from modules.upload_data import upload_company_data_json_to_gcs, upload_daily_data_json_to_gcs, load_json_to_bigquery
from modules.forecast import fetch_market_data, arima_backtest, arima_predict, add_features, xgboost_backtest_forecast, upload_forecast_to_bigquery
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

@dag(dag_id='fin_dag', default_args=default_args, schedule_interval=None, catchup=False)
def fin_dag():   
    @task_group(group_id='ingest_data')
    def ingest_data():
        @task(task_id='get_largest_market_cap_companies')
        def get_largest_market_cap_companies():
            return get_largest_companies_fmp()
        
        @task(task_id='upload_largest_companies')
        def upload_largest_companies(largest_companies_df):
            gcs_uri = upload_company_data_json_to_gcs(largest_companies_df, "company_data_json", "largest_companies")
            load_json_to_bigquery(gcs_uri, "market_data", "largest_companies_data_json")
        
        @task(task_id='fetch_tickers')
        def fetch_ticker_list():
            return get_top_50_tickers(PROJECT_ID)

        @task(task_id='get_latest_dates')
        def get_latest_dates(ticker_list):
            return get_latest_data_date(ticker_list)  # returns dict {ticker: latest_date}

        @task(task_id='download_upload_all')
        def download_upload_all(ticker_start_dates: dict):
            try:
                new_data_df = download_data(ticker_start_dates)
            except Exception as e:
                raise AirflowException(f"Error during data download: {e}")
            
            if not new_data_df.empty:
                try:
                    gcs_uri = upload_daily_data_json_to_gcs(new_data_df, "yfinance_daily_data_json", "stock_data")
                    load_json_to_bigquery(gcs_uri, "market_data", "yf_daily_json")
                    return "Upload successful"
                except Exception as e:
                    raise AirflowException(f"Error during upload: {e}")
            else:
                print("No new data to upload.")
                return "No data"

        largest_companies = get_largest_market_cap_companies()
        upload_largest_companies(largest_companies)

        tickers = fetch_ticker_list()
        latest_dates = get_latest_dates(tickers)
        download_upload_all(latest_dates)
    
    @task_group(group_id='forecasting')
    def ml_forecast():
        @task(task_id='fetch_market_data')
        def fetch_data():
            """Fetch market data from BigQuery"""
            df = fetch_market_data()
            return df.to_json(orient='split')

        @task(task_id='arima_forecasting')
        def arima_forecasting(market_data_json):
            """Run ARIMA backtesting and forecasting, then upload to BigQuery"""
            df = pd.read_json(market_data_json, orient='split')
            training_timestamp = datetime.now()

            # Backtest and predict
            backtest_df, best_orders = arima_backtest(df, training_timestamp)
            forecast_df = arima_predict(df, best_orders, training_timestamp)

            # Combine both
            combined_arima_df = pd.concat([backtest_df, forecast_df], ignore_index=True)

            # Upload to BigQuery
            destination_table = "market_data.stock_forecast"
            upload_forecast_to_bigquery(combined_arima_df, destination_table, PROJECT_ID)

            return "ARIMA Forecasting completed"

        @task(task_id='xgboost_forecasting')
        def xgboost_forecasting(market_data_json):
            """Run XGBoost modeling and upload to BigQuery"""
            df = pd.read_json(market_data_json, orient='split')
            training_timestamp = datetime.now()

            # Add features and forecast
            df_with_features = add_features(df)
            forecast_df = xgboost_backtest_forecast(df_with_features, training_timestamp)

            # Upload to BigQuery
            destination_table = "market_data.stock_forecast"
            upload_forecast_to_bigquery(forecast_df, destination_table, PROJECT_ID)

            return "XGBoost Forecasting completed"

        # DAG execution
        market_data = fetch_data()
        arima_forecasting(market_data)
        xgboost_forecasting(market_data)
    
    # Define the DAG structure
    ingest_data() >> ml_forecast()

dag_instance = fin_dag()