import os
import requests
import pandas as pd
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def cg_fetch_search_trending(cg_apikey):
    """Fetch trending search data from CoinGecko API and return a cleaned DataFrame.
    Documentation: https://docs.coingecko.com/v3.0.1/reference/trending-search
    According to CoinGecko's documentation, this endpoint allows you to query trending search coins, NFTs, and categories on CoinGecko within the last 24 hours.
    Since we are focusing on coins, we will filter only for coins in the function.
    """
    COINGECKO_TRENDING_URL = "https://api.coingecko.com/api/v3/search/trending"
    HEADERS = {
                "accept": "application/json",
                "x-cg-demo-api-key": cg_apikey
            }
    
    def filter_price_changes(price_changes):
        """Filter price_change_percentage_24h to keep only 'btc' and 'usd'."""
        return {key: value for key, value in price_changes.items() if key in ['btc', 'usd']}

    try:
        response = requests.get(COINGECKO_TRENDING_URL, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        trending_coins = data.get("coins", [])

        # Filter price_change_percentage_24h
        for coin in trending_coins:
            item = coin["item"]
            price_change = item.get("data", {}).get("price_change_percentage_24h", {})
            item["data"]["price_change_percentage_24h"] = filter_price_changes(price_change)

        raw_df = pd.DataFrame(trending_coins)
        df = pd.json_normalize(raw_df['item'])  # Flatten nested 'item'

        # Drop and Rename Columns
        drop_columns = ["price_btc", "data.content", "data.content.title", "data.content.description"]
        df = df.drop(columns=drop_columns, errors="ignore")
        rename_columns = {
            "coin_id": "trdg_id",
            "id": "coin_id",
            "name": "coin_name",
            "symbol": "coin_symbol",
            "market_cap_rank": "trdg_market_cap_rank",
            "thumb": "trdg_img_thumb",
            "small": "trdg_img_small",
            "large": "trdg_img_large",
            "slug": "trdg_slug",
            "score": "trdg_score",
            "data.price": "trdg_price_usd",
            "data.price_btc": "trdg_price_btc",
            "data.price_change_percentage_24h.usd": "trdg_price_change_percentage_24h_usd",
            "data.price_change_percentage_24h.btc": "trdg_price_change_percentage_24h_btc",
            "data.market_cap": "trdg_market_cap_usd",
            "data.market_cap_btc": "trdg_market_cap_btc",
            "data.total_volume": "trdg_total_volume_usd",
            "data.total_volume_btc": "trdg_total_volume_btc",
            "data.sparkline": "trdg_sparkline",
        }
        df = df.rename(columns=rename_columns)
        df.insert(0, 'trdg_data_ts', datetime.now().replace(microsecond=0))

        required_columns = ["trdg_data_ts",
            "trdg_id", "coin_id", "coin_name", "coin_symbol", "trdg_market_cap_rank",
            "trdg_img_thumb", "trdg_img_small", "trdg_img_large", "trdg_slug",
            "trdg_score", "trdg_price_usd", "trdg_price_btc",
            "trdg_price_change_percentage_24h_btc", "trdg_price_change_percentage_24h_usd",
            "trdg_market_cap_usd", "trdg_market_cap_btc", "trdg_total_volume_usd",
            "trdg_total_volume_btc", "trdg_sparkline"
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        df = df[required_columns]
        
        # Clean Data Type
        float_list = ['trdg_price_usd','trdg_price_btc','trdg_price_change_percentage_24h_btc','trdg_price_change_percentage_24h_usd',
                      'trdg_market_cap_usd','trdg_market_cap_btc','trdg_total_volume_usd','trdg_total_volume_btc']

        int_list = ['trdg_id','trdg_market_cap_rank','trdg_score']

        for f in float_list:
            try:
                df[f] = df[f].astype(float)
            except ValueError:
                df[f] = df[f].str.replace('$','').str.replace(',','').astype(float)

        for i in int_list:
            df[i] = df[i].astype('int64')

        df['coin_id'] = df['coin_id'].str.lower()
        df['coin_symbol'] = df['coin_symbol'].str.upper()
        df['coin_name'] = df['coin_name'].str.upper()

        return df

    except requests.RequestException as e:
        print(f"\033[1;31mAPI request failed: {e}\033[0m")
        return pd.DataFrame()
    except Exception as e:
        print(f"\033[1;31mAn error occurred while processing the data: {e}\033[0m")
        return pd.DataFrame()

if __name__ == "__main__":
    
    try:
        df = cg_fetch_search_trending(os.getenv("COINGECKO_API_KEY"))
        print(f"✅ Successfully fetched search trending data. Total rows: {len(df)}")
    except Exception as e:
        logging.error("An error occurred while fetching search trending data", exc_info=True)
        print("❌ Failed to fetch search trending data. Please check the logs for details.")