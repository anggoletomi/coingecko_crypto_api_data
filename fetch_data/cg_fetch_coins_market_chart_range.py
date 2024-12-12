import os
import requests
import pandas as pd
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def convert_unix(ori_format, value, unix_output_format='milliseconds'):
    """
    Convert between Unix timestamps and human-readable dates.
    
    - ori_format : 'unix' or 'human_date'
    - value: int (for 'unix') or str (for 'human_date')
      e.g. 'unix' (int) = 1692583200000 
      e.g. 'human_date' (str) = '2023-08-21 09:00:00' in UTC
    - unix_output_format: 'seconds' or 'milliseconds' (default: 'milliseconds')
    """
    if ori_format == 'unix':
        value_str = str(value)
        if len(value_str) == 10:  # Unix timestamp in seconds
            unix_time_in_seconds = int(value)
        elif len(value_str) == 13:  # Unix timestamp in milliseconds
            unix_time_in_seconds = int(value) / 1000
        else:
            raise ValueError("Value input does not meet the expected Unix timestamp format.")

        # Convert Unix timestamp to UTC datetime
        dt_utc = datetime.fromtimestamp(unix_time_in_seconds, timezone.utc)
        dt_utc_str = dt_utc.strftime("%Y-%m-%d %H:%M:%S")

        return dt_utc_str

    elif ori_format == 'human_date':
        try:
            dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        except ValueError:
            raise ValueError("Value input does not match the expected format 'YYYY-MM-DD HH:MM:SS'.")

        # Convert UTC datetime to Unix timestamp
        unix_timestamp_seconds = dt.timestamp()
        
        if unix_output_format == 'seconds':
            return int(unix_timestamp_seconds)
        elif unix_output_format == 'milliseconds':
            return int(unix_timestamp_seconds * 1000)
        else:
            raise ValueError("unix_output_format must be either 'seconds' or 'milliseconds'.")

    else:
        raise ValueError("ori_format argument must be either 'unix' or 'human_date'.")

def cg_fetch_coins_market_chart_range(cg_apikey, id: str, from_date: str, to_date: str,
                                      vs_currency: str = "usd", precision: str = "6", interval: str = ""):
    """
    This function fetches historical chart data for a specific cryptocurrency from the CoinGecko API within certain time range in UNIX.
    Access to historical data via the Public API (Demo plan) is restricted to the past 365 days only.

    Parameters:
    - id (str): Required. The ID of the coin (e.g., 'bitcoin', 'ethereum'). Only single value is allowed.
    - from_date (str): Required. UTC starting date in str format (e.g., '2024-10-14').
    - to_date (str): Required. UTC ending date in str format (e.g., '2024-10-15').
    - vs_currency (str): Required. The target currency for market data. Default is 'usd'. Only single value is allowed.
    - precision (str): Optional. Decimal places for currency price values
    - interval (str): Optional. Data interval. Leave empty for automatic granularity. interval='daily' is exclusive to paid plan subscribers only
        ~ 1 day from current time = 5-minutely data
        ~ 1 day from any time (except current time) = hourly data
        ~ 2 - 90 days from any time = hourly data
        ~ above 90 days from any time = daily data (00:00 UTC)

    Returns:
    - pd.DataFrame: DataFrame containing the historical market chart data.
    """

    # Process Unix Timestamp
    start_unix = convert_unix('human_date',from_date + " 00:00:00",unix_output_format='seconds')
    end_unix = convert_unix('human_date',to_date + " 00:00:00",unix_output_format='seconds')

    # Process Fetch Data
    url = f"https://api.coingecko.com/api/v3/coins/{id}/market_chart/range"
    params = {
        "vs_currency": vs_currency,
        "from" : start_unix,
        "to": end_unix,
        "precision": precision,
        "interval": interval
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
        
        # Clean Data Type
        df['date'] = df['date'].astype('datetime64[us]')
        for f in ['price','market_cap','volume']:
            df[f] = df[f].astype(float)

        df['id'] = df['id'].str.lower()

        # Rename Column
        rename_columns = {
            "id": "coin_id",
            'data_ts': 'mrag_data_ts',
            'currency': 'mrag_currency',
            'price': 'mrag_price',
            'market_cap': 'mrag_market_cap',
            'volume': 'mrag_volume'
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

    last_x_days = 30
    to_date = datetime.now().strftime('%Y-%m-%d')
    from_date = (pd.to_datetime(to_date) - pd.Timedelta(days=last_x_days-1)).strftime('%Y-%m-%d')

    try:
        df = cg_fetch_coins_market_chart_range(os.getenv("COINGECKO_API_KEY"),id="bitcoin",from_date=from_date, to_date=to_date)
        print(f"✅ Successfully fetched coins market chart range data. Total rows: {len(df)}")
    except Exception as e:
        logging.error("An error occurred while fetching coins market chart range data", exc_info=True)
        print("❌ Failed to fetch coins market chart range data. Please check the logs for details.")