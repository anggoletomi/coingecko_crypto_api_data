import os
import requests
import pandas as pd
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def cg_fetch_trending_search(cg_apikey):
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
        # API Request
        response = requests.get(COINGECKO_TRENDING_URL, headers=HEADERS)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Parse Data
        data = response.json()
        trending_coins = data.get("coins", [])

        # Filter price_change_percentage_24h
        for coin in trending_coins:
            item = coin["item"]
            price_change = item.get("data", {}).get("price_change_percentage_24h", {})
            item["data"]["price_change_percentage_24h"] = filter_price_changes(price_change)

        # Create DataFrame
        raw_df = pd.DataFrame(trending_coins)
        df = pd.json_normalize(raw_df['item'])  # Flatten nested 'item'

        # Drop and Rename Columns
        drop_columns = ["price_btc", "data.content", "data.content.title", "data.content.description"]
        df = df.drop(columns=drop_columns, errors="ignore")
        rename_columns = {
            "name": "coin_name",
            "symbol": "coin_symbol",
            "market_cap_rank": "coin_market_cap_rank",
            "thumb": "coin_img_thumb",
            "small": "coin_img_small",
            "large": "coin_img_large",
            "slug": "coin_slug",
            "score": "coin_score",
            "data.price": "data_price_usd",
            "data.price_btc": "data_price_btc",
            "data.price_change_percentage_24h.usd": "data_price_change_percentage_24h_usd",
            "data.price_change_percentage_24h.btc": "data_price_change_percentage_24h_btc",
            "data.market_cap": "data_market_cap_usd",
            "data.market_cap_btc": "data_market_cap_btc",
            "data.total_volume": "data_total_volume_usd",
            "data.total_volume_btc": "data_total_volume_btc",
            "data.sparkline": "data_sparkline",
        }
        df = df.rename(columns=rename_columns)

        # Add Time Stamp

        df.insert(0, 'data_ts', datetime.now().replace(microsecond=0))

        # Ensure Columns Exist and Return Cleaned DataFrame
        required_columns = ["data_ts",
            "id", "coin_id", "coin_name", "coin_symbol", "coin_market_cap_rank",
            "coin_img_thumb", "coin_img_small", "coin_img_large", "coin_slug",
            "coin_score", "data_price_usd", "data_price_btc",
            "data_price_change_percentage_24h_btc", "data_price_change_percentage_24h_usd",
            "data_market_cap_usd", "data_market_cap_btc", "data_total_volume_usd",
            "data_total_volume_btc", "data_sparkline"
        ]
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
        df = cg_fetch_trending_search(os.getenv("COINGECKO_API_KEY"))
        print(f"✅ Successfully fetched trending search data. Total rows: {len(df)}")
    except Exception as e:
        logging.error("An error occurred while fetching trending search data", exc_info=True)
        print("❌ Failed to fetch trending search data. Please check the logs for details.")