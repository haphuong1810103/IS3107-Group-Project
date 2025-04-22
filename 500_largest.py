import requests
import pandas as pd

def get_largest_companies_fmp(api_key, limit=500):
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
def get_top_50_tickers(api_key: str):
    """
    Gets a list of tickers for the top 50 companies by market cap
    
    Args:
        project_id: GCP project ID for accessing the API key
        
    Returns:
        List of ticker symbols
    """
    # Get largest companies data
    print("Fetching largest companies by market cap...")
    companies_df = get_largest_companies_fmp(api_key, limit=500)
    
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
# Usage
api_key = ""  # Replace with your actual API key
companies = get_top_50_tickers(api_key)
print(companies)