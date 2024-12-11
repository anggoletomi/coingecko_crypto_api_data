import os
import requests
import pandas as pd
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def cg_fetch_market_data(cg_apikey, vs_currency='usd', ids=None, order='market_cap_desc', 
                         per_page=100, page=1, sparkline=False, price_change_percentage='1h,24h,7d,14d,30d,200d,1y', 
                         locale='en', precision=None):
    """
    Fetch market data for cryptocurrencies from the CoinGecko API.

    Arguments:
    - vs_currency (str): Target currency of coins and market data. Default: 'usd'.
    - ids (list): List of coin IDs. Comma-separated if querying more than 1 coin. Default: None (all coins).
    - order (str): Sort result by field. Options: 'market_cap_asc', 'market_cap_desc', 'volume_asc', 'volume_desc', 'id_asc', 'id_desc'. Default: 'market_cap_desc'.
    - per_page (int): Total results per page. Valid values: 1...250. Default: 100.
    - page (int): Page through results. Default: 1.
    - sparkline (bool): Include sparkline 7-day data. Default: False.
    - price_change_percentage (str): Include price change percentage timeframe. Valid values: '1h', '24h', '7d', '14d', '30d', '200d', '1y'. Comma-separated for multiple timeframes. Default: '1h,24h,7d,14d,30d,200d,1y'.
    - locale (str): Language background. Default: 'en'.
    - precision (str): Decimal place for currency price value. Default: None.

    Returns:
    - pd.DataFrame: DataFrame containing market data.
    """
    if not cg_apikey:
        raise ValueError("cg_apikey is required.")
    if not isinstance(vs_currency, str) or not vs_currency:
        raise ValueError("vs_currency must be a non-empty string.")
    if ids and not all(isinstance(i, str) for i in ids):
        raise ValueError("ids must be a list of strings or None.")
    if per_page < 1 or per_page > 250:
        raise ValueError("per_page must be between 1 and 250.")
    if page < 1:
        raise ValueError("page must be a positive integer.")
    
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "ids": ','.join(ids) if ids else None,
        "order": order,
        "per_page": per_page,
        "page": page,
        "sparkline": str(sparkline).lower(),
        "price_change_percentage": price_change_percentage,
        "locale": locale,
        "precision": precision,
    }
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": cg_apikey,
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        df.insert(0, 'data_ts', datetime.now().replace(microsecond=0)) # Add Timestamp

        # Ensure Columns Exist and Return Cleaned DataFrame
        required_columns = ['data_ts','id', 'symbol', 'name', 'image', 'current_price', 'market_cap',
                            'market_cap_rank', 'fully_diluted_valuation', 'total_volume',
                            'high_24h', 'low_24h', 'price_change_24h',
                            'price_change_percentage_24h', 'market_cap_change_24h',
                            'market_cap_change_percentage_24h', 'circulating_supply',
                            'total_supply', 'max_supply', 'ath', 'ath_change_percentage',
                            'ath_date', 'atl', 'atl_change_percentage', 'atl_date', 'roi',
                            'last_updated', 'price_change_percentage_14d_in_currency',
                            'price_change_percentage_1h_in_currency',
                            'price_change_percentage_24h_in_currency',
                            'price_change_percentage_7d_in_currency',
                            'price_change_percentage_30d_in_currency',
                            'price_change_percentage_200d_in_currency',
                            'price_change_percentage_1y_in_currency']
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
    
    try:
        df = cg_fetch_market_data(os.getenv("COINGECKO_API_KEY"))
        print(f"✅ Successfully fetched market data. Total rows: {len(df)}")
    except Exception as e:
        logging.error("An error occurred while fetching market data", exc_info=True)
        print("❌ Failed to fetch market data. Please check the logs for details.")