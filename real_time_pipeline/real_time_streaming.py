import yfinance as yf
import pandas as pd
import json
from google.cloud import pubsub_v1

PROJECT_ID = "is3107-project-455413"
topic_id = "real-time-stock"

# Pub/Sub Publisher Client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, topic_id)

def get_latest_stock_minute(tickers=["MSFT", "AAPL", "NVDA"]):
    # Download 1-minute interval data for today
    df = yf.download(
        tickers=tickers,
        period="1d",
        interval="1m",
        progress=False,
        auto_adjust=False,
        group_by='ticker'  # Ensures separate DataFrames for each ticker
    )

    if df.empty:
        print("No data retrieved.")
        return None

    result = []

    # Process each ticker's latest data
    for ticker in tickers:
        if ticker not in df:
            continue

        ticker_df = df[ticker].dropna()
        if ticker_df.empty:
            continue

        latest_row = ticker_df.iloc[-1]
        utc_timestamp = ticker_df.index[-1]

        data = {
            "Ticker": ticker,
            "DateTime": utc_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            "Open": round(latest_row["Open"], 2),
            "High": round(latest_row["High"], 2),
            "Low": round(latest_row["Low"], 2),
            "Close": round(latest_row["Close"], 2),
            "Volume": round(latest_row["Volume"], 2)
        }
        result.append(data)

    return json.dumps(result, indent=2)

def publish_message(json_data):
    # Convert JSON string back to list of dicts
    stock_data_list = json.loads(json_data)

    for data in stock_data_list:
        # Convert dict to JSON string and then to bytes
        message_bytes = json.dumps(data).encode("utf-8")
        
        # Publish the message
        publish_status = publisher.publish(topic_path, message_bytes)
        print(f"Published message for {data['Ticker']} with ID: {publish_status.result()}")

def run():
    data = get_latest_stock_minute()
    publish_message(data)

run()