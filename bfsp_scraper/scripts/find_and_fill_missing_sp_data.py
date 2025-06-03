import argparse
import datetime as dt
import pandas as pd
import awswrangler as wr
import sys
import io
import requests
import time
import functools
from pathlib import Path
import os

# Adjust sys.path to allow imports from the bfsp_scraper package
# Script location: /scripts/find_and_fill_missing_sp_data.py
# Project root: / (containing bfsp_scraper package)
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from bfsp_scraper.settings import boto3_session, S3_BUCKET, AWS_GLUE_DB, SCHEMA_COLUMNS
# try_again and clean_name are expected to be in utils.general
# Import the refactored utilities
from bfsp_scraper.utils.general import construct_betfair_sp_download_url, download_sp_from_link, try_again, clean_name 

ATHENA_S3_OUTPUT = f's3://{S3_BUCKET}/athena-query-results/find_and_fill/'

def get_source_sp_data(start_date_str, end_date_str, country_code):
    d1 = pd.to_datetime(start_date_str)
    d2 = pd.to_datetime(end_date_str)
    race_dates = pd.date_range(d1, d2, freq='D')
    all_dfs = []

    print(f"Fetching source SP data for {country_code} from {start_date_str} to {end_date_str}")
    for race_date in race_dates:
        file_date = race_date + dt.timedelta(days=1) # Betfair files are for next day's races
        
        file_year_str = str(file_date.year)
        file_month_str = str(file_date.month).zfill(2)
        file_day_str = str(file_date.day).zfill(2)
        
        for type_str in ['win', 'place']:
            # Construct link using the utility function
            link = construct_betfair_sp_download_url(country_code, type_str, file_date)
            
            # Call the refactored download_sp_from_link from utils.general
            daily_df = download_sp_from_link(
                link=link,
                country=country_code,
                type_str=type_str,
                file_year_str=file_year_str, # For potential internal use if S3 naming was ever needed by it
                file_month_str=file_month_str,
                file_day_str=file_day_str,
                return_df=True
            )

            # download_sp_from_link now returns an empty DataFrame on 404 or error if return_df=True
            if daily_df is not None and not daily_df.empty:
                all_dfs.append(daily_df)
            elif daily_df is None: # Should not happen if return_df=True, but as a safeguard
                print(f"No DataFrame returned from download_sp_from_link for {link} (was None)")
            time.sleep(0.5) # Add 0.5 second delay after each API call
    
    if not all_dfs:
        print("No source SP data could be fetched.")
        return pd.DataFrame()
    
    source_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Consolidated source SP data: {len(source_df)} records.")
    return source_df

def get_existing_keys_from_athena(table_name, country_code, start_date_str, end_date_str):
    query = f"""
    SELECT DISTINCT event_id, selection_id
    FROM {table_name}
    WHERE country = '{country_code}'
      AND event_dt BETWEEN cast('{start_date_str} 00:00:00' as timestamp) 
                       AND cast('{end_date_str} 23:59:59' as timestamp)
    """
    print(f"Fetching existing keys from Athena table {table_name}...")
    try:
        df_keys = wr.athena.read_sql_query(
            sql=query,
            database=AWS_GLUE_DB,
            s3_output=ATHENA_S3_OUTPUT + f"{table_name}_keys/",
            boto3_session=boto3_session,
            ctas_approach=False
        )
        print(f"Found {len(df_keys)} existing key combinations in {table_name}.")
        return df_keys
    except Exception as e:
        print(f"Error fetching keys from {table_name}: {e}")
        return pd.DataFrame(columns=['event_id', 'selection_id'])

def align_df_for_athena_write(df_to_align):
    # Ensure DataFrame columns match SCHEMA_COLUMNS order and presence
    # Type casting will be largely handled by wr.s3.to_parquet's dtype argument
    aligned_cols = {}
    for col_name, col_type in SCHEMA_COLUMNS.items():
        if col_name in df_to_align.columns:
            aligned_cols[col_name] = df_to_align[col_name]
        else:
            # This should ideally not happen if fetch_and_process_sp_df_from_link is robust
            print(f"Warning: Column '{col_name}' missing from DataFrame to be written. Adding as None.")
            aligned_cols[col_name] = pd.Series([None] * len(df_to_align), dtype='object') 
            
    return pd.DataFrame(aligned_cols)[list(SCHEMA_COLUMNS.keys())] # Enforce order

def main():
    # Fetch configuration from environment variables
    start_date_str = os.environ.get('BFSP_START_DATE', '2025-05-01')
    end_date_str = os.environ.get('BFSP_END_DATE','2025-06-02')
    country_code = os.environ.get('BFSP_COUNTRY', 'gb')
    force_insert_str = os.environ.get('BFSP_FORCE_INSERT', 'false').lower()

    # Validate required environment variables
    if not all([start_date_str, end_date_str, country_code]):
        print("Error: Missing required environment variables. Please set BFSP_START_DATE, BFSP_END_DATE, and BFSP_COUNTRY.")
        print("Example: export BFSP_START_DATE='2023-01-01'")
        sys.exit(1)

    # Validate country code
    if country_code not in ['gb', 'ire', 'fr', 'usa']:
        print(f"Error: Invalid BFSP_COUNTRY value '{country_code}'. Must be 'gb' or 'ire'.")
        sys.exit(1)

    # Validate date formats (basic check)
    try:
        dt.datetime.strptime(start_date_str, '%Y-%m-%d')
        dt.datetime.strptime(end_date_str, '%Y-%m-%d')
    except ValueError:
        print("Error: Invalid date format. BFSP_START_DATE and BFSP_END_DATE must be YYYY-MM-DD.")
        sys.exit(1)

    force_insert = force_insert_str in ['true', '1', 'yes']

    print(f"Configuration: Start Date={start_date_str}, End Date={end_date_str}, Country={country_code}, Force Insert={force_insert}")

    source_sp_df = get_source_sp_data(start_date_str, end_date_str, country_code)

    if source_sp_df.empty:
        print("No source data retrieved. Exiting.")
        return

    for table_type in ['place', 'win']:
        athena_table_name = f'betfair_{table_type}_prices'
        s3_dataset_path = f's3://{S3_BUCKET}/{table_type}_price_datasets/'

        print(f"\nProcessing for table: {athena_table_name}")

        # Filter source data for current type
        current_type_source_df = source_sp_df[source_sp_df['type'] == table_type].copy()
        if current_type_source_df.empty:
            print(f"No source data for type '{table_type}'. Skipping.")
            continue
        
        # Get existing keys
        athena_keys_df = get_existing_keys_from_athena(athena_table_name, country_code, start_date_str, end_date_str)

        # Identify missing records
        if not athena_keys_df.empty:
            # Ensure key columns are of the same type for merging
            current_type_source_df['event_id'] = current_type_source_df['event_id'].astype(int)
            current_type_source_df['selection_id'] = current_type_source_df['selection_id'].astype(int)
            athena_keys_df['event_id'] = athena_keys_df['event_id'].astype(int)
            athena_keys_df['selection_id'] = athena_keys_df['selection_id'].astype(int)
            
            missing_records_df = pd.merge(
                current_type_source_df,
                athena_keys_df,
                on=['event_id', 'selection_id'],
                how='left',
                indicator=True
            ).query('_merge == "left_only"').drop(columns=['_merge'])
        else:
            # If no existing keys in Athena for the range, all source records are missing
            print(f"No existing data found in {athena_table_name} for the specified range. All source records for type '{table_type}' will be considered missing.")
            missing_records_df = current_type_source_df

        print(f"Found {len(missing_records_df)} missing records for {athena_table_name}.")

        if not missing_records_df.empty:
            if force_insert or input(f"Insert these {len(missing_records_df)} records into {athena_table_name}? (yes/no): ").lower() == 'yes':
                print(f"Preparing to insert {len(missing_records_df)} records...")
                
                # Align DataFrame schema before writing
                df_to_write = align_df_for_athena_write(missing_records_df)
                
                try:
                    wr.s3.to_parquet(
                        df=df_to_write,
                        path=s3_dataset_path,
                        dataset=True,
                        database=AWS_GLUE_DB,
                        table=athena_table_name,
                        mode='append', # Appends new files to the dataset path
                        dtype=SCHEMA_COLUMNS, # Use the DDL-derived schema for writing
                        boto3_session=boto3_session,
                        compression='snappy',
                        use_threads=True
                    )
                    print(f"Successfully appended {len(df_to_write)} records to {s3_dataset_path} for table {athena_table_name}.")
                except Exception as e:
                    print(f"Error inserting data into {athena_table_name}: {e}")
            else:
                print("Insertion skipped by user.")

if __name__ == "__main__":
    main()
