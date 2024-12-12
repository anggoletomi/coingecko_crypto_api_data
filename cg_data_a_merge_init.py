import pandas as pd
import time
from datetime import datetime
import numpy as np
import os

from dotenv import load_dotenv
load_dotenv()

from cg_fetch_coins_market_chart_range import cg_fetch_coins_market_chart_range
from cg_fetch_coins_market_chart import cg_fetch_coins_market_chart
from cg_fetch_coins_markets import cg_fetch_coins_markets
from cg_fetch_coins_ohlc import cg_fetch_coins_ohlc, ohlc_day_options
from cg_fetch_search_trending import cg_fetch_search_trending
from cg_fetch_simple_price import cg_fetch_simple_price

from bi_function import write_table_by_unique_id, get_local_time, log_function

import logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def cg_data_a_merge_init(cg_apikey,currency = 'usd',decimal_precision = '6',last_x_days = 365, delay_between_request = 3):

    to_date = datetime.now().strftime('%Y-%m-%d')
    from_date = (pd.to_datetime(to_date) - pd.Timedelta(days=last_x_days-1)).strftime('%Y-%m-%d')

    def fetch_loop(ref,coin_list):
        print(f"\033[1;32m🛠️ Process : {ref}\033[0m")
        print(f"Total coin to fetch : {len(coin_list)}")

        all_data = []
        count = 1

        for coin in coin_list:
            try:
                print(f"{count}. Fetching {ref} data for {coin}...")

                if ref == 'ohlc':
                    possible_days_value = max([num for num in ohlc_day_options if num <= last_x_days])
                    data = cg_fetch_coins_ohlc(cg_apikey,id=coin,vs_currency=currency,days=str(possible_days_value),precision=decimal_precision)
                
                elif ref == 'market_chart':
                    data = cg_fetch_coins_market_chart(cg_apikey,id=coin,vs_currency=currency,days=str(last_x_days),interval='daily',
                                                    precision=decimal_precision)
                
                elif ref == 'market_chart_range':
                    data = cg_fetch_coins_market_chart_range(cg_apikey,id=coin,from_date=from_date,to_date=to_date,
                                                            vs_currency=currency,precision=decimal_precision,interval='')

                all_data.append(data)
                count += 1
                time.sleep(delay_between_request)

            except Exception as e:
                print(f"\033[1;31mError fetching {ref} data for {coin}: {e}\033[0m")

        df = pd.concat(all_data, ignore_index=True)
        print("------------------------------")

        return df


    # 1. COINS MARKET
    df_markets = cg_fetch_coins_markets(cg_apikey, vs_currency=currency, ids=None, order='market_cap_desc', 
                            per_page=100, page=1, sparkline=False, price_change_percentage='1h,24h,7d,14d,30d,200d,1y', 
                            locale='en', precision=decimal_precision)
    market_ids = df_markets['coin_id'].unique().tolist() #Get unique coin IDs

    # 2. SEARCH TRENDING
    df_trending = cg_fetch_search_trending(cg_apikey)
    trending_ids = df_trending['coin_id'].unique().tolist() #Get unique coin IDs

    df_trending_drop = df_trending.drop(columns=['coin_symbol', 'coin_name']) #Handle for later merging

    not_exist_trending_ids = [id for id in trending_ids if id not in market_ids] # Find coin IDs that do not exist in df_markets

    df_markets_additional = cg_fetch_coins_markets(cg_apikey, vs_currency=currency, ids=not_exist_trending_ids, order='market_cap_desc', 
                            per_page=100, page=1, sparkline=False, price_change_percentage='1h,24h,7d,14d,30d,200d,1y', 
                            locale='en', precision=decimal_precision) # Fetch non-exist trending coin IDs

    df_market_all = pd.concat([df_markets,df_markets_additional]).reset_index(drop=True)

    # *** Create Coin List ***
    coin_list = df_market_all['coin_id'].unique().tolist()

    # # 3. SIMPLE PRICE
    # df_simple = cg_fetch_simple_price(cg_apikey,ids=",".join(coin_list))
    # # `cg_fetch_coins_markets` provides data similar to `cg_fetch_simple_price`.
    # # The only difference is that 'usd_24h_vol' is missing in `cg_fetch_coins_markets`.
    # # However, `cg_fetch_coins_markets` is more comprehensive and contains more detailed data.
    # # Therefore, for this task, we will use `cg_fetch_coins_markets`.

    # 4. COINS OHLC
    df_ohlc = fetch_loop('ohlc',coin_list)

    # 5. COINS MARKET CHART
    df_market_chart = fetch_loop('market_chart',coin_list)

    # # 6. COINS MARKET CHART RANGE
    # df_market_chart_range = fetch_loop('market_chart_range',coin_list)
    # # `cg_fetch_coins_market_chart` provides same data to `cg_fetch_coins_market_chart_range`.
    # # The only difference is the parameter used to fetch the data, but the result is exactly the same.
    # # Therefore, we will comment it out for now, as we will conduct our analysis using `cg_fetch_coins_market_chart`.

    # *** Merge Market & Trending Data ***
    df_merge = pd.merge(df_market_all,df_trending_drop,on='coin_id',how='left',indicator=True)
    df_merge = df_merge.rename(columns={'_merge': 'merge_status_1'})
    df_merge['trending_flag'] = np.where(df_merge['merge_status_1'] == 'both',1,0)

    # *** Merge Market Chart & OHLC Data ***
    df_market_chart_ohlc = pd.merge(df_market_chart,df_ohlc,on=['coin_id','date'],how='left',indicator=True)
    df_market_chart_ohlc = df_market_chart_ohlc.rename(columns={'_merge': 'merge_status_2'})

    # *** Merge All ***
    df_final = pd.merge(df_market_chart_ohlc,df_merge,on='coin_id',how='left',indicator=True)
    df_final = df_final.rename(columns={'_merge': 'merge_status_3'})

    # Load to BigQuery
    write_table_by_unique_id(df_markets, 'cryptocurrency.cgc_coins_markets', 'replace', ['coin_id'], date_col_ref='date')
    write_table_by_unique_id(df_trending, 'cryptocurrency.cgc_search_trending', 'replace', ['coin_id'], date_col_ref='date')
    write_table_by_unique_id(df_ohlc, 'cryptocurrency.cgc_coins_ohlc', 'replace', ['coin_id'], date_col_ref='date')
    write_table_by_unique_id(df_market_chart, 'cryptocurrency.cgc_coins_market_chart', 'replace', ['coin_id'], date_col_ref='date')

    write_table_by_unique_id(df_final, 'cryptocurrency.cgc_a_market_historical_data', 'replace', ['coin_id'], date_col_ref='date')

    get_local_time()

if __name__ == "__main__":

    tasks = [
        
        (cg_data_a_merge_init,{'cg_apikey' : os.getenv("COINGECKO_API_KEY")}),
        
        ]

    log_function(tasks)

    
