from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from openpyxl import load_workbook
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pathlib import Path

from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

import glob
import gspread
import numpy as np
import sys,os
import pandas as pd
import platform
import re
import tzlocal
import shutil
import socket
import subprocess
import time
import warnings

from dotenv import load_dotenv
load_dotenv()

warnings.filterwarnings("ignore")

# GBQ SERVICE ACCOUNT

service_account_bi = {
  "type": "service_account",
  "project_id": os.getenv("GBQ_PROJECT_ID"),
  "private_key_id": os.getenv("GBQ_PRIVATE_KEY_ID"),
  "private_key": os.getenv("GBQ_PRIVATE_KEY"),
  "client_email": os.getenv("GBQ_CLIENT_EMAIL"),
  "client_id": os.getenv("GBQ_CLIENT_ID"),
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": os.getenv("GBQ_CLIENT_X509_CERT_URL"),
  "universe_domain": "googleapis.com"
}

BI_PROJECT_ID = os.getenv("GBQ_PROJECT_ID")
BI_CREDENTIAL = Credentials.from_service_account_info(service_account_bi)
BI_CLIENT = bigquery.Client.from_service_account_info(service_account_bi)

# FUNCTION READ GBQ

def read_from_gbq(client, sql):
    return client.query(sql).to_dataframe()

# FUNCTION WRITE GBQ

def write_to_gbq(df, project_id, credential, target_table, import_method, job_location):
    df.to_gbq(target_table, project_id=project_id, if_exists=import_method, location=job_location, progress_bar=False,
              credentials=credential)

def write_table_by_unique_id(df, target_table, write_method, unique_col_ref, date_col_ref=None):

    """
    Parameters:
    write_method: replace / append
    unique_col_ref : must be a list contains the column name where the data type is string, even if it's only have one value. for example : ['store_id']
    date_col_ref : must be a single variable, 1 column name with date data type. for example : 'order_creation_time'
    """

    if write_method == 'replace':
        print(f'write_method = {write_method}')
        
        write_to_gbq(df, BI_PROJECT_ID, BI_CREDENTIAL, target_table, 'replace', 'asia-southeast2')

    elif write_method == 'append':
        print(f'write_method = {write_method}')

        df_temp = df.copy()

        for i in unique_col_ref:
            df_temp[i] = df_temp[i].str.upper()

        if date_col_ref:

            df_temp[date_col_ref] = pd.to_datetime(df_temp[date_col_ref]).dt.date

            df_temp = df_temp[unique_col_ref + [date_col_ref]].drop_duplicates()

            df_temp[date_col_ref] = pd.to_datetime(df_temp[date_col_ref])
            
            conditions = " AND ".join(
                [f"UPPER(target.{col}) = UPPER(temp.{col})" for col in unique_col_ref] + 
                [f"DATE(target.{date_col_ref}) = DATE(temp.{date_col_ref})"]
            )
        else:
            df_temp = df_temp[unique_col_ref].drop_duplicates()

            conditions = " AND ".join(
                [f"UPPER(target.{col}) = UPPER(temp.{col})" for col in unique_col_ref]
            )

        # Load Temporary Table
        stage_table = f'data_stage.{target_table.replace('.', '_')}'
        write_to_gbq(df_temp, BI_PROJECT_ID, BI_CREDENTIAL, stage_table, 'replace', 'asia-southeast2')
        time.sleep(2)

        # Delete Origin Table
        delete_sql = f'''
            DELETE FROM `{BI_PROJECT_ID}.{target_table}` AS target
            WHERE EXISTS (
                SELECT 1 FROM `{stage_table}` AS temp
                WHERE {conditions}
            );
        '''

        try:
            query_job = BI_CLIENT.query(delete_sql)
            query_job.result()  # Wait for the job to complete

            print(f"Total rows deleted: {query_job.num_dml_affected_rows}")
            print(f'Total rows to upload: {len(df)}')
            
            # Upload data to BigQuery
            write_to_gbq(df, BI_PROJECT_ID, BI_CREDENTIAL, target_table, 'append', 'asia-southeast2')
            print(f"Data uploaded - {target_table} : {datetime.now().strftime('%Y-%m-%d %H:%M')}")

        except Exception as e:
            print(f"\033[1;31mError during delete and load process: {e}\033[0m")
            raise

    else:
        print(f'\033[1;31mThe options for the write_table method are either "replace" or "append". Please choose the correct one.\033[0m')

# # Example Usage:
# write_table_by_unique_id(df,
#                         target_table = 'report_rc.sp_income_released',
#                         write_method='replace',
#                         unique_col_ref = ['store_id'],
#                         date_col_ref = 'order_creation_time'
#                         )

# GOOGLE SHEETS & GOOGLE DRIVE
gs_credentials = ServiceAccountCredentials.from_json_keyfile_dict(service_account_bi, ['https://spreadsheets.google.com/feeds'])
gs_client = gspread.authorize(gs_credentials)

def read_gsheet(gs_title, sheet_name, first_row=0):
    ws = gs_client.open(gs_title)
    sh = ws.worksheet(sheet_name)
    sh = sh.get_all_values()[first_row:]
    sh = pd.DataFrame.from_dict(sh)
    sh.rename(columns=sh.iloc[0], inplace=True)
    sh.drop(sh.index[0], inplace=True)

    df = sh.reset_index(drop=True)

    return df

def write_to_gsheet(dataframe, spreadsheet_id, worksheet_id, gs_client, clear_old_data=True, new_title=None):
    """
    Uploads a DataFrame to a specified Google Sheets worksheet by worksheet ID and optionally renames it.

    Parameters:
    dataframe (pd.DataFrame): The DataFrame to upload.
    spreadsheet_id (str): The ID of the Google Sheets document.
    worksheet_id (int): The ID of the worksheet to upload the data to.
    gs_client (gspread.Client): The authorized gspread client.
    new_title (str, optional): The new title to rename the worksheet. Defaults to None.
    """
    # Open the Google Sheets document by ID
    spreadsheet = gs_client.open_by_key(spreadsheet_id)
    
    # Find the worksheet by ID
    worksheet = next((sheet for sheet in spreadsheet.worksheets() if sheet.id == worksheet_id), None)
    if worksheet is None:
        raise ValueError(f"No worksheet found with ID: {worksheet_id}")
    
    # Rename the worksheet if a new title is provided
    if new_title:
        worksheet.update_title(new_title)
    
    # Clear existing content in the worksheet (optional)
    if clear_old_data:
        worksheet.clear()
    
    # Upload the DataFrame to the worksheet
    set_with_dataframe(worksheet, dataframe)

    print(f"DataFrame uploaded successfully to worksheet ID {worksheet_id} in the Google Sheets document.")

def authenticate_google_sa(api_name, api_version, scopes):
    """
    Authenticate a service account and return the API client service.

    Args:
        api_name (str): Name of the Google API (e.g., 'drive', 'sheets').
        api_version (str): Version of the API (e.g., 'v3', 'v4').
        scopes (list): List of scopes for the API.

    Returns:
        googleapiclient.discovery.Resource: The authenticated API service object.
    """
    credentials = Credentials.from_service_account_info(service_account_bi, scopes=scopes)
    return build(api_name, api_version, credentials=credentials)

# TIMESTAMP

def get_local_time():
    local_timezone = tzlocal.get_localzone()
    current_time = datetime.now(local_timezone)
    timezone_name = local_timezone.key
    gmt_offset = current_time.tzname()

    print(f'\033[36mCurrent Timestamp : {current_time.strftime("%Y-%m-%d %H:%M:%S")} - {timezone_name} (GMT{gmt_offset})\033[0m')

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

# SAFE DIVIDE
    
def safe_divide_optimized(numerator, denominator): # example : df['result'] = safe_divide_optimized(df['num'],df['denom'])
    result = pd.to_numeric(numerator.replace([-np.inf, np.inf], np.nan), errors='coerce') / pd.to_numeric(denominator.replace([-np.inf, np.inf], np.nan), errors='coerce')
    result = result.replace([-np.inf, np.inf], np.nan)
    result = np.where(((numerator == 0) & (denominator == 0)), 0, result)
    return result

# FIX BROKEN EXCEL

def fix_broken_excel(list_of_path, num_files=1):
    libreoffice_command = "soffice" if platform.system() == "Windows" else "libreoffice"
    input_folders = list_of_path

    for folder in input_folders:
        # Exclude files ending with ".backup" from the list and files with "_clean"
        try:
            all_files = [os.path.join(folder, f) for f in os.listdir(folder) if
                         os.path.isfile(os.path.join(folder, f)) and not f.endswith('.backup') and not "_clean." in f]
        except Exception as e:
            print(e)
            continue

        sorted_files = sorted(all_files, key=lambda x: os.path.getmtime(x), reverse=True)
        input_files = sorted_files[:num_files]

        if len(input_files) == 0:
            print(f'There are no files in folder : {folder}')
            continue

        for file_path in input_files:
            # Backup original file only if a backup doesn't already exist
            backup_path = file_path + ".backup"
            if not os.path.exists(backup_path) and not "_clean." in file_path:
                shutil.copy(file_path, backup_path)

            try:
                try:
                    # Attempt to load and save with openpyxl
                    wb = load_workbook(filename=file_path)
                    wb.save(file_path)
                    print(f'* Success to run open and save action for : {file_path}')

                except:
                    print(f'* Failed to run open and save action for : {file_path}')
                    print(f'* Continue the process using convert method for : {file_path}')

                    if file_path.endswith('.xlsx'):

                        subprocess.run(
                            [libreoffice_command, "--headless", "--convert-to", "xls", file_path, "--outdir", folder])
                        print(f'* New file is created')

                        new_file_path = file_path.replace('.xlsx', '.xls')
                        expected_file_path = new_file_path.replace('.xls', '_clean.xls')

                        if os.path.exists(new_file_path):
                            os.rename(new_file_path, expected_file_path)

                            subprocess.run(
                                [libreoffice_command, "--headless", "--convert-to", "xlsx", expected_file_path,
                                 "--outdir", folder])
                            os.remove(expected_file_path)

                            print(f'* Finish to convert and clean the new file')
                            print()

                        else:
                            print(f"* Error: Conversion process was not successful : {file_path}")
                            print()

                    elif file_path.endswith('.xls'):

                        subprocess.run(
                            [libreoffice_command, "--headless", "--convert-to", "xlsx", file_path, "--outdir", folder])
                        print(f'* New file is created')

                        new_file_path = file_path.replace('.xls', '.xlsx')
                        expected_file_path = new_file_path.replace('.xlsx', '_clean.xlsx')

                        if os.path.exists(new_file_path):
                            os.rename(new_file_path, expected_file_path)

                            subprocess.run(
                                [libreoffice_command, "--headless", "--convert-to", "xls", expected_file_path,
                                 "--outdir", folder])
                            os.remove(expected_file_path)

                            print(f'* Finish to convert and clean the new file')
                            print()

                        else:
                            print(f"* Error: Conversion process was not successful : {file_path}")
                            print()

            except Exception as e:
                print(f"Error processing {file_path}. Error: {e}")

    finish_time = datetime.now() + timedelta(hours=(7 if platform.system() != "Windows" else 0))
    print(f'Fixing excel file process end at : {finish_time.strftime("%Y-%m-%d %H:%M")}')

def get_latest_file_multiple_folder(local_path_list, n=1):

    final_file_list = []

    # Helper function to get latest files from a given path
    def get_latest_files(path, n):
        all_files = glob.glob(path + '*.xlsx') + glob.glob(path + '*.csv') + glob.glob(path + '*.xls')
        if all_files:
            sorted_files = sorted(all_files, key=os.path.getmtime, reverse=True)
            return sorted_files[:n]
        return []

    for path in local_path_list:
        if os.path.exists(path):
            final_file_list.extend(get_latest_files(path, n))

    return final_file_list

# LOG FUNCTION

def log_function(script_function_list):

    start_time = time.time()
    print("\033[94m" + "="*50 + "\033[0m")  # Blue color border line
    print("\033[93m" + "        🚀  STARTING SCRIPT EXECUTION  🚀" + "\033[0m")  # Yellow bold header with rocket emoji
    print("\033[94m" + "="*50 + "\033[0m")  # Blue color border line

    def load_report(report_function, kwargs):
        func_start_time = time.time()  # Start time for individual function
        try:
            report_function(**kwargs)  # Unpack the dictionary as keyword arguments
            print(f'\033[92mLoad {report_function.__name__} successful\033[0m')  # Green text for success
            func_time_taken_sec = round(time.time() - func_start_time, 2)
            func_time_taken_min = round((time.time() - func_start_time)/60,2)
            print(f'\033[93mTime taken for {report_function.__name__}: {func_time_taken_sec} seconds ({func_time_taken_min} minutes)\033[0m')  # Yellow text for time taken
            return True
        except Exception as e:
            func_time_taken_sec = round(time.time() - func_start_time, 2)
            func_time_taken_min = round((time.time() - func_start_time)/60,2)
            print(f"\033[91mAn error occurred with {report_function.__name__}: {e}\033[0m")  # Red text for error
            print(f'\033[93mTime taken for {report_function.__name__}: {func_time_taken_sec} seconds ({func_time_taken_min} minutes) - ERROR\033[0m')  # Yellow text for time taken in case of error
            return str(e)

    count = 1
    error_script = []

    for report_function, params in script_function_list: # Loop through the list and call the load_report function with parameters

        script_name = f'SCRIPT {count} : {report_function.__name__}'

        print(f'\033[94m*********************************** {script_name} ***********************************\033[0m')  # Blue text for script names
        result = load_report(report_function, params)
        print()
        print()

        if result is not True:
            error_script.append(f'{script_name}: {result}')
            
        count += 1

    # Print the error scripts
    if error_script:
        print("\n\033[91mError Scripts:\033[0m")  # Red text for error header
        for error in error_script:
            print(f'\033[91m{error}\033[0m')  # Red text for each error
    else:
        print("\n\033[92mAll scripts ran successfully.\033[0m")  # Green text for success message

    print("")
    get_local_time()

    script_time_taken_sec = round(time.time() - start_time, 2)
    script_time_taken_min = round((time.time() - start_time)/60,2)
    print(f'\033[93mTotal Processing Time : {script_time_taken_sec} seconds ({script_time_taken_min} minutes)\033[0m')

    print("\033[92m" + "="*50 + "\033[0m")  # Green color border line
    print("\033[91m" + "        🏁  SCRIPT EXECUTION COMPLETED  🏁" + "\033[0m")  # Red bold text with finish line emoji
    print("\033[92m" + "="*50 + "\033[0m")  # Green color border line
    print("\033[96m" + "        🎉  All tasks have been successfully executed!  🎉" + "\033[0m")  # Cyan text for completion message

# Example Usage

# function_list = [
#     (get_product_master_list, {}),
#     (get_latest_inv_status, {}),
#     
#     (get_daily_inv_brand, {}),
#     (get_monthly_inv, {'report_scope': 'full'}),
#     (get_latest_inv_sku, {}),
#     (fb_b2c_sales_forecast, {}),
#
#     (daily_outbound_data, {'report_scope' : 'full', 'last_x_days' : 7}),
#     (sku_data_summary, {'report_level_chosen': 'sku'}),
#     (sku_data_summary, {'report_level_chosen': 'brand'}),
#     (sku_daily_pivot, {'start_date' : '2024-01-01'}),
#     (sku_weekly_pivot_ytd, {})
# ]

# log_function(function_list)

def flexible_categorize_by_description(description, dict_mapping, type='simple', match_type='strict'):
    """
    Categorizes a given description based on a dictionary of category mappings. This function allows 
    for both strict and flexible matching modes and can return a specified type of label from the 
    category dictionary.

    Parameters:
    description (str): The description text to be categorized.
    
    dict_mapping (dict): A dictionary containing category mappings, where each key is a string 
                         representing a pattern to search for in the description, and each value 
                         is a dictionary containing label types (e.g., 'simple', 'english', 'database').
                         Example format:
                         {
                             'key_pattern': {
                                 'simple': 'Simple Label',
                                 'english': 'English Label',
                                 'database': 'Database Label'
                             }
                         }
                         
    type (str, optional): The label type to retrieve from the category dictionary. For instance, 
                          'simple', 'english', or 'database'. If the specified type is not found 
                          in a category, it defaults to 'simple'. Defaults to 'simple'.

    match_type (str, optional): Specifies the matching mode for finding categories.
                                - 'strict': Matches the description only if it contains the exact 
                                            key pattern as a substring.
                                - 'flexible': Matches the description if it contains all words in 
                                              the key pattern, regardless of word order.
                                Defaults to 'strict'.

    Returns:
    str: The label from the dictionary that corresponds to the specified type if a match is found; 
         otherwise, the original description is returned unchanged.
    
    Example Usage:
    rc_shopee_wallet_category_mappings = {
        'penggantian dana penuh barang hilang': {
            'simple': 'Penggantian Barang Hilang/Rusak',
            'english': 'Full Refund for Lost Item',
            'database': 'refund_lost_full'
        },
        ...
    }
    
    categorized_label = flexible_categorize_by_description(
        description='penggantian dana penuh barang hilang',
        dict_mapping=rc_shopee_wallet_category_mappings,
        type='database',
        match_type='flexible'
    )
    # categorized_label -> 'refund_lost_full' if a match is found
    """
    description_lower = description.lower()

    for key, category_dict in dict_mapping.items():
        if match_type == 'strict':
            # Strict match: exact substring search
            if key in description_lower:
                return category_dict.get(type, category_dict.get('simple'))
        elif match_type == 'flexible':
            # Flexible match: all words must be present, order doesn't matter
            key_words = key.split()
            if all(word in description_lower for word in key_words):
                return category_dict.get(type, category_dict.get('simple'))

    return description

    # example usage : df_wallet['wallet_category'] = df_wallet['w_description'].apply(flexible_categorize_by_description, args=(rc_shopee_wallet_category_mappings, 'database', 'strict'))
    # for manual testing without dataframe:
    # test_var = 'Pemotongan Komisi pemotOngan biaya AMS biaya Komisi'
    # result = flexible_categorize_by_description(test_var, rc_shopee_wallet_category_mappings, type='database', match_type='flexible')
    # print(result)


def get_month_list(month_start,month_end,month_format):
    start_date = datetime.strptime(month_start, month_format)
    end_date = datetime.strptime(month_end, month_format)

    months = [(start_date + relativedelta(months=i)).strftime(month_format)
            for i in range((end_date.year - start_date.year) * 12 + end_date.month - start_date.month + 1)]
    
    return months