from google.cloud import bigquery
from datetime import datetime, timedelta
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.multioutput import MultiOutputRegressor
from xgboost import XGBRegressor
from pandas_gbq import to_gbq
import pandas as pd
import numpy as np
import os
import warnings

warnings.filterwarnings('ignore')

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TABLE_ID = "is3107-project-455413.market_data.yf_daily_json"
BIGQUERY_COLUMNS = ["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
TRAINING_TIMESTAMP = datetime.now()

def fetch_market_data(project_id: str, table_id: str, columns: list) -> pd.DataFrame:
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT {', '.join(columns)} FROM `{table_id}`
    WHERE 
        Ticker in ('AAPL', 'NVDA', 'MSFT')
        AND Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    """
    return client.query(query).to_dataframe()

def arima_backtest(df: pd.DataFrame, training_timestamp: datetime):
    """
    Performs backtesting of ARIMA models for forecasting stock closing prices.

    For each unique ticker in the input DataFrame:
    - Splits the time series into training and test sets (80/20 split).
    - Performs a grid search over ARIMA(p, d, q) parameters to find the best model based on AIC.
    - Uses the best model to forecast future closing prices on the test set.
    - Calculates RMSE and MAE for model performance evaluation.
    - Records the forecasted values, actual values, and associated metrics.
    """
    forecast_records = []
    best_orders = {}

    tickers = df["Ticker"].unique()
    for ticker in tickers:
        ticker_df = df[df["Ticker"] == ticker].sort_values("Date").set_index("Date")
        series = ticker_df["Close"]
        if len(series) < 20:
            continue
        train_size = int(len(series) * 0.8)
        train, test = series[:train_size], series[train_size:]

        best_model, best_order, best_aic = None, None, float("inf")
        for p in range(4):
            for d in range(2):
                for q in range(4):
                    try:
                        model = ARIMA(train, order=(p, d, q)).fit()
                        if model.aic < best_aic:
                            best_aic, best_order, best_model = model.aic, (p, d, q), model
                    except:
                        continue

        if best_model:
            best_orders[ticker] = best_order
            forecast = best_model.forecast(len(test))
            forecast.index = test.index
            rmse = np.sqrt(mean_squared_error(test, forecast))
            mae = mean_absolute_error(test, forecast)

            for date in test.index:
                forecast_records.append({
                    "date": date,
                    "ticker": ticker,
                    "predicted_close": forecast.loc[date],
                    "actual_close": test.loc[date],
                    "type": "backtest",
                    "rmse": rmse,
                    "mae": mae,
                    "model": f"ARIMA{best_order}",
                    "training_timestamp": training_timestamp
                })
    return pd.DataFrame(forecast_records), best_orders

def arima_predict(df: pd.DataFrame, best_orders: dict, training_timestamp: datetime) -> pd.DataFrame:
    """
    Generates 7-day forward forecasts for each ticker using the specified ARIMA orders.

    For each ticker in the `best_orders` dictionary:
    - Fits an ARIMA model using the full historical closing price series.
    - Forecasts the next 7 days of closing prices.
    - Generates a forecast record for each predicted date, tagged as a future prediction.
    """
    forecast_records = []
    for ticker, order in best_orders.items():
        ticker_df = df[df["Ticker"] == ticker].sort_values("Date").set_index("Date")
        series = ticker_df["Close"]
        model = ARIMA(series, order=order).fit()
        forecast = model.forecast(steps=7)
        last_date = series.index[-1]
        forecast_dates = pd.date_range(last_date + timedelta(days=1), periods=7)

        for date, pred in zip(forecast_dates, forecast):
            forecast_records.append({
                "date": date,
                "ticker": ticker,
                "predicted_close": pred,
                "actual_close": None,
                "type": "prediction",
                "rmse": None,
                "mae": None,
                "model": f"ARIMA{order}",
                "training_timestamp": training_timestamp
            })
    return pd.DataFrame(forecast_records)

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(by=["Ticker", "Date"])
    for window in [5, 10, 20]:
        df[f"MA_{window}"] = df.groupby("Ticker")["Close"].transform(lambda x: x.rolling(window).mean())
        df[f"Volume_MA_{window}"] = df.groupby("Ticker")["Volume"].transform(lambda x: x.rolling(window).mean())
    for lag in range(1, 8):
        df[f"lag_{lag}"] = df.groupby("Ticker")["Close"].shift(lag)
    for i in range(1, 8):
        df[f"Close_t+{i}"] = df.groupby("Ticker")["Close"].shift(-i)
    return df


def xgboost_backtest_forecast(df: pd.DataFrame, training_timestamp: datetime) -> pd.DataFrame:
    """
    Trains and evaluates an XGBoost model for stock price forecasting using multi-step prediction.

    This function performs the following steps for each unique stock ticker in the dataset:
    - Splits the dataset into training and test sets.
    - Trains a MultiOutput XGBoost regressor to predict the next 7 closing prices.
    - Backtests the model on the test set and calculates RMSE and MAE for each ticker.
    - Forecasts the next 7 days of closing prices based on the most recent available data.
    - Returns a DataFrame of both backtest and future forecast records.
    """
    tickers = df["Ticker"].unique()
    df = add_features(df)
    forecast_records = []

    for ticker in tickers:
        ticker_df = df[df["Ticker"] == ticker]
        feature_cols = ['Open', 'High', 'Low', 'Volume'] + \
                       [f"MA_{w}" for w in [5,10,20]] + \
                       [f"Volume_MA_{w}" for w in [5,10,20]] + \
                       [f"lag_{i}" for i in range(1, 8)]
        target_cols = [f"Close_t+{i}" for i in range(1, 8)]

        X = ticker_df[feature_cols]
        y = ticker_df[target_cols]
        train_size = int(len(X) * 0.8)
        X_train, X_test = X[:train_size], X[train_size:]
        y_train, y_test = y[:train_size], y[train_size:]

        model = MultiOutputRegressor(XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=5))
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        test_dates = ticker_df["Date"].iloc[train_size:].reset_index(drop=True)

        preds, actuals = [], []
        for i in range(len(X_test)):
            for day in range(7):
                preds.append(y_pred[i][day])
                actuals.append(y_test.iloc[i][day])
                forecast_records.append({
                    "date": test_dates[i] + pd.Timedelta(days=day+1),
                    "ticker": ticker,
                    "predicted_close": y_pred[i][day],
                    "actual_close": y_test.iloc[i][day],
                    "type": "backtest",
                    "rmse": None,
                    "mae": None,
                    "model": "XGBoost",
                    "training_timestamp": training_timestamp
                })

        valid_mask = ~np.isnan(preds) & ~np.isnan(actuals)
        rmse = np.sqrt(mean_squared_error(np.array(actuals)[valid_mask], np.array(preds)[valid_mask]))
        mae = mean_absolute_error(np.array(actuals)[valid_mask], np.array(preds)[valid_mask])

        for record in forecast_records:
            if record["type"] == "backtest" and record["ticker"] == ticker:
                record["rmse"] = rmse
                record["mae"] = mae

        X_latest = X.iloc[[-1]]
        y_future_pred = model.predict(X_latest)[0]
        forecast_dates = pd.date_range(ticker_df["Date"].max() + pd.Timedelta(days=1), periods=7)

        for i, pred in enumerate(y_future_pred):
            forecast_records.append({
                "date": forecast_dates[i],
                "ticker": ticker,
                "predicted_close": pred,
                "actual_close": None,
                "type": "prediction",
                "rmse": None,
                "mae": None,
                "model": "XGBoost",
                "training_timestamp": training_timestamp
            })

    return pd.DataFrame(forecast_records)

# Take the resulting dataframes from xgboost_Backtest_forecast, arima_predict and  arima_backtest and concatenate
def upload_forecast_to_bigquery(df: pd.DataFrame, destination_table: str, project_id: str):
    df["date"] = pd.to_datetime(df["date"], errors='coerce')
    df["training_timestamp"] = pd.to_datetime(df["training_timestamp"], errors='coerce')
    df["ticker"] = df["ticker"].astype(str)
    df["type"] = df["type"].astype(str)
    df["model"] = df["model"].astype(str)
    df["predicted_close"] = pd.to_numeric(df["predicted_close"], errors='coerce')
    df["actual_close"] = pd.to_numeric(df["actual_close"], errors='coerce')

    to_gbq(df, destination_table, project_id=project_id, if_exists='append')
