import yfinance as yf
import pandas as pd
from datetime import datetime
import time

def get_top50_tickers():
    """
    Scrape the top 50 companies by market cap from companiesmarketcap.com
    """
    url = "https://companiesmarketcap.com/"
    df_list = pd.read_html(url)
    for df in df_list:
        if "Company" in df.columns and "Ticker" in df.columns:
            top50 = df.head(50)
            return top50["Ticker"].dropna().tolist()
    raise Exception("Could not find Ticker column on the page.")

def fetch_historical_prices(tickers, period="1y", interval="1d"):
    """
    Fetch historical closing prices for given tickers.
    """
    all_data = []

    for ticker in tickers:
        try:
            data = yf.download(ticker, period=period, interval=interval)
            data["Ticker"] = ticker
            data.reset_index(inplace=True)
            all_data.append(data[["Date", "Ticker", "Close"]])
            time.sleep(0.2)  # avoid rate limits
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

    return pd.concat(all_data, ignore_index=True)

def run():
    # tickers = get_top50_tickers()
    tickers = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"
    ]
    df = fetch_historical_prices(tickers, period="1y", interval="1d")
    filename = f"top50_prices_1y_{datetime.today().strftime('%Y%m%d')}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved historical prices to {filename}")

if __name__ == "__main__":
    run()