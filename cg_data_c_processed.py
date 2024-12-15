from bi_function import read_from_gbq,BI_CLIENT,BI_PROJECT_ID,write_table_by_unique_id,write_to_gsheet,gs_client,log_function
import numpy as np

def cg_data_c_processed():

    # 1. Data Query

    df = read_from_gbq(BI_CLIENT,f'''SELECT date,cmrk_data_ts as data_ts,cmrk_currency as currency,coin_id,coin_symbol,coin_name,
                                            mkch_price,mkch_market_cap,mkch_volume,
                                            ohlc_open,ohlc_high,ohlc_low,ohlc_close,
                                            cmrk_image,cmrk_current_price,cmrk_market_cap,cmrk_market_cap_rank,
                                            cmrk_fully_diluted_valuation,cmrk_total_volume,cmrk_high_24h,cmrk_low_24h,
                                            cmrk_price_change_24h,cmrk_price_change_percentage_24h,cmrk_market_cap_change_24h,
                                            cmrk_market_cap_change_percentage_24h,cmrk_circulating_supply,cmrk_total_supply,
                                            cmrk_max_supply,cmrk_ath,cmrk_ath_change_percentage,cmrk_ath_date,cmrk_atl,
                                            cmrk_atl_change_percentage,cmrk_atl_date,cmrk_roi,cmrk_last_updated,
                                            cmrk_price_change_percentage_1h_in_currency,cmrk_price_change_percentage_24h_in_currency,
                                            cmrk_price_change_percentage_7d_in_currency,cmrk_price_change_percentage_14d_in_currency,
                                            cmrk_price_change_percentage_30d_in_currency,cmrk_price_change_percentage_200d_in_currency,
                                            cmrk_price_change_percentage_1y_in_currency,
                                            trdg_img_thumb,trdg_img_small,trdg_img_large,trdg_score,trdg_sparkline,
                                            trending_flag
                                    FROM `{BI_PROJECT_ID}.cryptocurrency.cgc_a_market_historical_data`''')
    
    # 2. Handle Missing Value

    for s in ['trdg_img_thumb','trdg_img_small','trdg_img_large','trdg_sparkline']:
        df[s] = df[s].fillna('-')

    # This column below represents the trending score, and 9999 is used as a default value to indicate "not trending."
    df['trdg_score'] = df['trdg_score'].fillna(9999)

    # Drop the `cmrk_roi` column due to high percentage of missing values (>85%) and not essential for analysis at this moment
    df = df.drop(columns=['cmrk_roi'],errors='ignore')

    # Leave critical columns with missing values (e.g., `ohlc_*`, `cmrk_max_supply`, etc.) as it is.
    # These values are essential for analysis, so no imputation (mean, median, or forward filling) is applied.

    # 3. Feature Engineering - Snapshot Table ('cmrk_*')

    df['market_dominance'] = (df['cmrk_market_cap'] / df['cmrk_market_cap'].sum())
    df['circulation_percentage'] = (df['cmrk_circulating_supply'] / df['cmrk_total_supply'])
    df['price_vs_ath'] = ((df['cmrk_current_price'] - df['cmrk_ath']) / df['cmrk_ath'])
    df['volatility_7d'] = abs(df['cmrk_price_change_percentage_7d_in_currency'])
    df['price_change_classification'] = df['cmrk_price_change_percentage_24h_in_currency'].apply(lambda x: 'Bullish' if x > 0 else 'Bearish')
    df['liquidity_score'] = df['cmrk_total_volume'] / df['cmrk_market_cap']
    df['growth_potential'] = abs(df['cmrk_ath_change_percentage'])
    df['risk_reward_ratio'] = abs(df['cmrk_price_change_percentage_7d_in_currency'] / df['cmrk_price_change_percentage_24h_in_currency'])
    df['market_cap_to_supply_ratio'] = df['cmrk_market_cap'] / df['cmrk_circulating_supply']
    df['daily_price_range'] = df['cmrk_high_24h'] - df['cmrk_low_24h']
    df['stability_index'] = df['daily_price_range'] / df['cmrk_current_price']
    df['circulation_health'] = df['circulation_percentage'].apply(lambda x: 'Healthy' if x >= 75 else 'Unhealthy')
    df['performance_trend_1y'] = df['cmrk_price_change_percentage_1y_in_currency'].apply(lambda x: 'High Growth' if x > 100 else 'Moderate' if 0 <= x <= 100 else 'Decline')

    # 4. Handle Infinity Value

    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Write to BigQuery & Google Sheets

    write_table_by_unique_id(df, 'cryptocurrency.cgc_a_market_historical_processed', 'replace', ['coin_id'], date_col_ref='date')

    df['date'] = df['date'].dt.date
    df['data_ts'] = df['data_ts'].dt.tz_localize(None)

    write_to_gsheet(df, spreadsheet_id='1bvZPl_vHrGyoGHw9q8TJ23MHuUdPuVHhAf6rDSS3U9s',
                        worksheet_id=651357280,
                        gs_client=gs_client,
                        clear_old_data=True,
                        new_title='cgc_a_market_historical_processed')


if __name__ == "__main__":

    tasks = [
        
        (cg_data_c_processed,{}),
        
        ]

    log_function(tasks)

    
