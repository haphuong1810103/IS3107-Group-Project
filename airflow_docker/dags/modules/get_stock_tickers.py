import yfinance as yf
import pandas as pd
import datetime
import json
from google.cloud import secretmanager, storage
from google.oauth2 import service_account
import json
import os
import requests

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

# function to access secret stored in Google Secret Manager
def access_secret_version(project_id: str, secret_id: str, version_id: str) -> str:
    """Helper function to access secret version"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# pull API key for financial model prep
def get_fmp_api_key(project_id: str) -> str:
    # Access the FMP API key secret
    api_key = access_secret_version(project_id, "fmp-api-key", "latest")
    return api_key

# get largest companies by market cap
def get_largest_companies_fmp(limit=500):
    api_key = get_fmp_api_key(PROJECT_ID)
    url = f"https://financialmodelingprep.com/api/v3/market-capitalization/AAPL,MSFT,GOOG,AMZN,META,TSLA,BRK-B,NVDA,UNH,JNJ?apikey={api_key}"
    
    # Better option: Use their stock screening API to get a sorted list directly
    screening_url = f"https://financialmodelingprep.com/api/v3/stock-screener?marketCapMoreThan=1000000000&limit={limit}&apikey={api_key}"
    
    try:
        response = requests.get(screening_url)
        response.raise_for_status()
        
        data = response.json()
        df = pd.DataFrame(data)

        df = df[(df['isEtf'] == False) & (df['isFund'] == False) & (df['isActivelyTrading'] == True)]
        
        # Sort by market cap
        if 'marketCap' in df.columns:
            df = df.sort_values(by='marketCap', ascending=False).reset_index(drop=True)
            return df
        else:
            print("Market cap data not found in response")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

# get 50 largest companies to pull from yfinance
def get_top_50_tickers(project_id: str):
    """
    Gets a list of tickers for the top 50 companies by market cap
    
    Args:
        project_id: GCP project ID for accessing the API key
        
    Returns:
        List of ticker symbols
    """
    # Get FMP API key from Secret Manager
    fmp_api_key = get_fmp_api_key(project_id)
    
    # Get largest companies data
    print("Fetching largest companies by market cap...")
    companies_df = get_largest_companies_fmp(fmp_api_key, limit=500)
    
    if companies_df is not None:
        # Take only the top 50
        top_50_df = companies_df.head(50)
        
        # Extract just the ticker symbols
        if 'symbol' in top_50_df.columns:
            ticker_list = top_50_df['symbol'].tolist()
            return ticker_list
        else:
            print("Symbol column not found in response")
            return []
    else:
        print("Failed to retrieve company data")
        return []