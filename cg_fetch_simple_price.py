import os
import requests
import pandas as pd
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def cg_fetch_simple_price(cg_apikey, ids, vs_currencies='usd', include_market_cap=True, include_24hr_vol=True, 
                          include_24hr_change=True, include_last_updated_at=True,precision=None):
    """
    Fetches cryptocurrency price data from CoinGecko API using the simple/price endpoint.

    Args:
        ids (str): Comma-separated string of coin IDs (e.g., "bitcoin,ethereum").
        vs_currencies (str): Comma-separated string of target currencies (e.g., "usd,eur"). Default is "usd"
        include_market_cap (bool): Whether to include market cap information. Default is True.
        include_24hr_vol (bool): Whether to include 24-hour volume. Default is True.
        include_24hr_change (bool): Whether to include 24-hour price change. Default is True.
        include_last_updated_at (bool): Whether to include the last updated time in UNIX format. Default is True.
        precision (str): Decimal place precision for currency price values. Optional.

    Returns:
        dict: A dictionary containing the requested data.

    Raises:
        Exception: If the API request fails.
    """
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ids,
        "vs_currencies": vs_currencies,
        "include_market_cap": str(include_market_cap).lower(),
        "include_24hr_vol": str(include_24hr_vol).lower(),
        "include_24hr_change": str(include_24hr_change).lower(),
        "include_last_updated_at": str(include_last_updated_at).lower()
    }

    # Add precision if specified
    if precision:
        params["precision"] = precision

    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": cg_apikey
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame.from_dict(data, orient='index').reset_index()
        df.rename(columns={'index': 'coin'}, inplace=True)
        df['last_updated_at'] = pd.to_datetime(df['last_updated_at'], unit='s')
        df.insert(0, 'data_ts', datetime.now().replace(microsecond=0)) # Add Timestamp

        # Ensure Columns Exist and Return Cleaned DataFrame
        required_columns = ['data_ts', 'coin', 'usd', 'usd_market_cap', 'usd_24h_vol',
                            'usd_24h_change', 'last_updated_at']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return df[required_columns]
    
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred while processing the data: {e}")
        return pd.DataFrame()
    
if __name__ == "__main__":

    coin_list = ['bitcoin','ethereum']
    
    try:
        df = cg_fetch_simple_price(os.getenv("COINGECKO_API_KEY"),ids=",".join(coin_list))
        print(f"✅ Successfully fetched simple price. Total rows: {len(df)}")
    except Exception as e:
        logging.error("An error occurred while fetching simple price", exc_info=True)
        print("❌ Failed to fetch simple price. Please check the logs for details.")