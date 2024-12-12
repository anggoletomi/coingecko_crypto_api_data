import os
import requests
import pandas as pd
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def cg_fetch_coins_ohlc(cg_apikey, id: str, vs_currency: str = "usd", days: str = "90", precision: str = "6"):
    """
    - Get the OHLC chart (Open, High, Low, Close) of a coin based on particular coin id.
    - The timestamp displayed in the payload (response) indicates the end (or close) time of the OHLC data.
    - Data granularity (candle's body) is automatic:
        ~ 1 - 2 days: 30 minutes
        ~ 3 - 30 days: 4 hours
        ~ 31 days and beyond: 4 days
    - Access to historical data via the Public API (Demo plan) is restricted to the past 365 days only

    Parameters:
    - id (str): Required. The ID of the coin (e.g., 'bitcoin', 'ethereum'). Only single value is allowed.
    - vs_currency (str): Required. The target currency for market data. Default is 'usd'. Only single value is allowed.
    - days (str) : Required. data up to number of days ago
    - precision (str): Optional. Decimal places for currency price values

    Returns:
    - pd.DataFrame: DataFrame containing the OHLC data.
    """

    # Process Fetch Data
    url = f"https://api.coingecko.com/api/v3/coins/{id}/ohlc"
    params = {
        "vs_currency": vs_currency,
        "days" : days,
        "precision": precision
    }
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": cg_apikey
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close"])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.insert(0, 'data_ts', datetime.now().replace(microsecond=0))
        df.insert(1, 'currency', vs_currency)
        df.insert(2, 'id', id)

        required_columns = ['data_ts', 'currency', 'id', 'timestamp', 'open', 'high', 'low', 'close']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        df = df[required_columns]
        
        # Clean Data Type
        df['timestamp'] = df['timestamp'].astype('datetime64[us]')
        for f in ['open', 'high', 'low', 'close']:
            df[f] = df[f].astype(float)

        df['id'] = df['id'].str.lower()

        # Rename Column
        rename_columns = {
            "id": "coin_id",
        }
        df = df.rename(columns=rename_columns)
        
        return df
    
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred while processing the data: {e}")
        return pd.DataFrame()
    
if __name__ == "__main__":

    try:
        df = cg_fetch_coins_ohlc(os.getenv("COINGECKO_API_KEY"),id="bitcoin")
        print(f"✅ Successfully fetched OHLC data. Total rows: {len(df)}")
    except Exception as e:
        logging.error("An error occurred while fetching OHLC data", exc_info=True)
        print("❌ Failed to fetch OHLC data. Please check the logs for details.")