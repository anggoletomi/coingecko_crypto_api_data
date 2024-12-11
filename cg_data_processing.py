import os
import pandas as pd
import time
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

from cg_fetch_coins_market_chart_range import cg_fetch_coins_market_chart_range
from cg_fetch_coins_market_chart import cg_fetch_coins_market_chart
from cg_fetch_coins_markets import cg_fetch_coins_markets
from cg_fetch_coins_ohlc import cg_fetch_coins_ohlc
from cg_fetch_search_trending import cg_fetch_search_trending
# from cg_fetch_simple_price import cg_fetch_simple_price

import logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# `cg_fetch_coins_markets` provides data similar to `cg_fetch_simple_price`.
# The only difference is that 'usd_24h_vol' is missing in `cg_fetch_coins_markets`.
# However, `cg_fetch_coins_markets` is more comprehensive and contains more detailed data.
# Therefore, for this task, we will use `cg_fetch_coins_markets`.

currency = 'usd'
decimal_precision = '6'
delay_between_request = 3 # in seconds, respect rate limit

to_date = datetime.now().strftime('%Y-%m-%d')
from_date = (pd.to_datetime(to_date) - pd.Timedelta(days=365)).strftime('%Y-%m-%d')

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")

def fetch_loop(ref,coin_list):
    print(f"\033[1;32m🛠️ Process : {ref}\033[0m")
    print(f"Total coin to fetch : {len(coin_list)}")

    all_data = []
    count = 1

    for coin in coin_list:
        try:
            print(f"{count}. Fetching {ref} data for {coin}...")

            if ref == 'ohlc':
                data = cg_fetch_coins_ohlc(COINGECKO_API_KEY,id=coin,vs_currency=currency,days='90',precision=decimal_precision)
            
            elif ref == 'market_chart':
                data = cg_fetch_coins_market_chart(COINGECKO_API_KEY,id=coin,vs_currency=currency,days='90',interval='daily',
                                                   precision=decimal_precision)
            
            elif ref == 'market_chart_range':
                data = cg_fetch_coins_market_chart_range(COINGECKO_API_KEY,id=coin,from_date=from_date,to_date=to_date,
                                                         vs_currency=currency,precision=decimal_precision,interval='')

            all_data.append(data)
            count += 1
            time.sleep(delay_between_request)

        except Exception as e:
            print(f"Error fetching {ref} data for {coin}: {e}")

    df = pd.concat(all_data, ignore_index=True)
    print("------------------------------")

    return df


# 1. COINS MARKET
df_markets = cg_fetch_coins_markets(COINGECKO_API_KEY, vs_currency=currency, ids=None, order='market_cap_desc', 
                           per_page=100, page=1, sparkline=False, price_change_percentage='1h,24h,7d,14d,30d,200d,1y', 
                           locale='en', precision=decimal_precision)
# coin_list = df_markets['id'].unique().tolist()
coin_list = ['bitcoin','ethereum','tether','ripple'] # for testing only, delete later

# 2. SEARCH TRENDING
df_trending = cg_fetch_search_trending(COINGECKO_API_KEY)

# 3. COINS OHLC
df_ohlc = fetch_loop('ohlc',coin_list)

# 4. COINS MARKET CHART
df_market_chart = fetch_loop('market_chart',coin_list)

# 5. COINS MARKET CHART RANGE
df_market_chart_range = fetch_loop('market_chart_range',coin_list)