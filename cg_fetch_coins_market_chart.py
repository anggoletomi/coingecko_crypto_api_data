import os
import requests
import pandas as pd
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def cg_fetch_coins_market_chart(cg_apikey, id: str,vs_currency: str = "usd",days: str = "30",interval: str = "daily",precision: str = "6"):
    """
    This function fetches historical chart data for a specific cryptocurrency from the CoinGecko API.
    Access to historical data via the Public API (Demo plan) is restricted to the past 365 days only.

    Parameters:
    - id (str): Required. The ID of the coin (e.g., 'bitcoin', 'ethereum'). Only single value is allowed.
    - vs_currency (str): Required. The target currency for market data. Default is 'usd'. Only single value is allowed.
    - days (str): Required. Data up to the number of days ago (e.g., '1', '7', '30').
    - interval (str): Optional. Data interval. Leave empty for automatic granularity. Possible value: 'daily'.
        ~ 1 day from current time = 5-minutely data
        ~ 2 - 90 days from current time = hourly data
        ~ above 90 days from current time = daily data (00:00 UTC)

    - precision (str): Optional. Decimal places for currency price values

    Returns:
    - pd.DataFrame: DataFrame containing the historical market chart data.
    """

    url = f"https://api.coingecko.com/api/v3/coins/{id}/market_chart"
    params = {
        "vs_currency": vs_currency,
        "days": days,
        "interval": interval,
        "precision": precision,
    }
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": cg_apikey
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame({
                            "date": [pd.to_datetime(entry[0], unit="ms") for entry in data["prices"]],
                            "price": [entry[1] for entry in data["prices"]],
                            "market_cap": [entry[1] for entry in data["market_caps"]],
                            "volume": [entry[1] for entry in data["total_volumes"]],
                        })
        df.insert(0, 'data_ts', datetime.now().replace(microsecond=0))
        df.insert(1, 'currency', vs_currency)
        df.insert(2, 'id', id)

        required_columns = ['data_ts', 'currency', 'id', 'date', 'price', 'market_cap', 'volume']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        df = df[required_columns]

        # Drop Current Date
        df = df[df['date'] != df['date'].max()]

        # Clean Data Type
        df['date'] = df['date'].astype('datetime64[us]')
        for f in ['price','market_cap','volume']:
            df[f] = df[f].astype(float)

        df['id'] = df['id'].str.lower()

        # Rename Column
        rename_columns = {
            "id": "coin_id",
            'data_ts': 'mkch_data_ts',
            'currency': 'mkch_currency',
            'price': 'mkch_price',
            'market_cap': 'mkch_market_cap',
            'volume': 'mkch_volume'
        }
        df = df.rename(columns=rename_columns)

        return df
    
    except requests.RequestException as e:
        print(f"\033[1;31mAPI request failed: {e}\033[0m")
        return pd.DataFrame()
    except Exception as e:
        print(f"\033[1;31mAn error occurred while processing the data: {e}\033[0m")
        return pd.DataFrame()
    
if __name__ == "__main__":

    try:
        df = cg_fetch_coins_market_chart(os.getenv("COINGECKO_API_KEY"),id="bitcoin")
        print(f"✅ Successfully fetched coins market chart data. Total rows: {len(df)}")
    except Exception as e:
        logging.error("An error occurred while fetching coins market chart data", exc_info=True)
        print("❌ Failed to fetch coins market chart data. Please check the logs for details.")