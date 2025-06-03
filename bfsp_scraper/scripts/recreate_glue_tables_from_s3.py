#!/usr/bin/env python
"""
Processes raw Betfair SP Parquet files from a specified S3 prefix, 
writes them to target S3 dataset locations (unpartitioned), 
and then recreates AWS Glue tables pointing to these locations.

This script operates in a batch mode per data type (win/place):
1. Reads all raw files for a specific type (e.g., all 'win' files).
2. Processes and concatenates these files into a single DataFrame.
3. Writes this combined DataFrame to the target S3 dataset prefix using mode='overwrite',
   deleting any pre-existing data under that prefix.
4. Recreates the Glue table for that type.
This is repeated for each data type.

Source S3 Structure (Raw Data):
- s3://{S3_BUCKET}/{BFSP_S3_RAW_DATA_PREFIX}/<type><country><YYYYMMDD>.parquet
  (e.g., s3://your-bucket/data/winire20230115.parquet)

Target S3 Structure (Unpartitioned Datasets):
- Win prices: s3://{S3_BUCKET}/{BFSP_S3_WIN_DATASET_PREFIX}/ (contains Parquet files for win data)
- Place prices: s3://{S3_BUCKET}/{BFSP_S3_PLACE_DATASET_PREFIX}/ (contains Parquet files for place data)

Configuration (Environment Variables):
Optional (with defaults):
- BFSP_GLUE_DATABASE: Name of the AWS Glue database (default: 'finish-time-predict').
- BFSP_GLUE_WIN_TABLE: Name of the Glue table for win prices (default: 'betfair_win_prices').
- BFSP_GLUE_PLACE_TABLE: Name of the Glue table for place prices (default: 'betfair_place_prices').
- BFSP_S3_RAW_DATA_PREFIX: S3 prefix for raw Parquet files (default: 'data/').
- BFSP_S3_WIN_DATASET_PREFIX: S3 prefix for win data files (default: 'win_price_datasets/').
- BFSP_S3_PLACE_DATASET_PREFIX: S3 prefix for place data files (default: 'place_price_datasets/').

Settings from bfsp_scraper.settings:
- S3_BUCKET: The S3 bucket where the data resides.
- boto3_session: The boto3 session to use for AWS interactions.
- SCHEMA_COLUMNS: Schema definition for the Glue tables.
"""

import os
import sys
import logging
import re
import awswrangler as wr
import pandas as pd
import warnings
from tqdm import tqdm

# Suppress specific FutureWarning from awswrangler related to 'promote' parameter
warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message=".*promote has been superseded by promote_options='default'.*",
    module="awswrangler._distributed" # Or more broadly: module="awswrangler"
)

# Add project root to Python path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from bfsp_scraper.settings import (
    S3_BUCKET,
    boto3_session,
    SCHEMA_COLUMNS
)

# Setup logging
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration & Constants ---
FILENAME_PATTERN = re.compile(r"^(win|place)([a-z]{2,3})(\d{4})(\d{2})(\d{2})\.parquet$", re.IGNORECASE)

# --- Helper Functions ---
def process_individual_dataframe(df: pd.DataFrame, file_type_from_filename: str, country_from_filename: str) -> pd.DataFrame:
    """Processes a single DataFrame read from a raw file to align with SCHEMA_COLUMNS."""
    logger.debug(f"Processing DataFrame. Initial columns: {df.columns.tolist()}")

    # Add/overwrite 'type' and 'country' based on filename for consistency
    df['type'] = file_type_from_filename.lower()
    df['country'] = country_from_filename.lower()

    # Handle event_dt conversion and derive 'year'
    if 'event_dt' in df.columns and SCHEMA_COLUMNS.get('event_dt') == 'timestamp':
        df['event_dt'] = pd.to_datetime(df['event_dt'], errors='coerce')
        if 'year' in SCHEMA_COLUMNS and SCHEMA_COLUMNS['year'] == 'int':
            if df['event_dt'].notna().any():
                df['year'] = df['event_dt'].dt.year.astype('Int64')
            else:
                df['year'] = pd.Series([pd.NA] * len(df), dtype='Int64') # All NaT if event_dt is all NaT
    elif 'year' in SCHEMA_COLUMNS and SCHEMA_COLUMNS['year'] == 'int' and 'year' not in df.columns:
        logger.warning("Column 'event_dt' missing or not a timestamp, and 'year' not present. Adding 'year' as NA.")
        df['year'] = pd.Series([pd.NA] * len(df), dtype='Int64')

    # Ensure all columns from SCHEMA_COLUMNS are present
    current_columns = df.columns.tolist()
    for col_name, col_type_str in SCHEMA_COLUMNS.items():
        if col_name not in current_columns:
            logger.warning(f"Column '{col_name}' missing from raw data, adding as null.")
            if col_type_str.startswith('int') or col_type_str.startswith('double'):
                # For concat, it's safer to use a common type like float64 for numbers if pd.NA is used, or object.
                # Pandas Int64 (nullable int) is good. For float, float64.
                if col_type_str.startswith('int'): df[col_name] = pd.Series([pd.NA] * len(df), dtype='Int64')
                else: df[col_name] = pd.Series([pd.NA] * len(df), dtype='Float64')
            elif col_type_str.startswith('timestamp'):
                df[col_name] = pd.Series([pd.NaT] * len(df), dtype='datetime64[ns]')
            elif col_type_str.startswith('boolean'):
                 df[col_name] = pd.Series([pd.NA] * len(df), dtype='boolean') # Nullable boolean
            else:  # string, object
                df[col_name] = pd.Series([None] * len(df), dtype='object')
    
    # Select and reorder columns according to SCHEMA_COLUMNS
    try:
        processed_df = df[list(SCHEMA_COLUMNS.keys())].copy()
    except KeyError as e:
        missing_in_df = [col for col in SCHEMA_COLUMNS.keys() if col not in df.columns]
        logger.error(f"Critical error: After attempting to add missing columns, these are still not in DataFrame: {missing_in_df}. Error: {e}")
        raise
    
    logger.debug(f"Processed DataFrame. Final columns: {processed_df.columns.tolist()}")
    return processed_df

def recreate_glue_table(
        database_name: str,
        table_name: str,
        s3_dataset_path: str, 
        table_schema: dict, 
        session
):
    logger.info(f"Recreating Glue table '{database_name}.{table_name}' from S3 path '{s3_dataset_path}'")
    wr.catalog.delete_table_if_exists(database=database_name, table=table_name, boto3_session=session)
    logger.info(f"Deleted table '{database_name}.{table_name}' if it existed.")

    wr.catalog.create_parquet_table(
        database=database_name,
        table=table_name,
        path=s3_dataset_path, 
        columns_types=table_schema,
        boto3_session=session,
        description=f"Betfair SP {table_name.split('_')[1]} prices, sourced from S3.", # Adjusted description slightly
        table_type="EXTERNAL_TABLE",
        mode="overwrite"
    )
    logger.info(f"Created table '{database_name}.{table_name}'.")
    logger.info(f"Recreation process for table '{database_name}.{table_name}' completed.")

# --- Main Processing Logic ---
def main():
    logger.info("Starting script to process raw S3 data and recreate Glue tables in batch mode.")

    glue_database = os.environ.get("BFSP_GLUE_DATABASE", 'finish-time-predict')
    glue_win_table_name = os.environ.get("BFSP_GLUE_WIN_TABLE", 'betfair_win_prices')
    glue_place_table_name = os.environ.get("BFSP_GLUE_PLACE_TABLE", 'betfair_place_prices')

    s3_raw_data_prefix = os.environ.get('BFSP_S3_RAW_DATA_PREFIX', 'data/')
    s3_win_dataset_prefix = os.environ.get('BFSP_S3_WIN_DATASET_PREFIX', 'win_price_datasets/')
    s3_place_dataset_prefix = os.environ.get('BFSP_S3_PLACE_DATASET_PREFIX', 'place_price_datasets/')

    if not S3_BUCKET:
        logger.error("S3_BUCKET is not configured in settings.py. Exiting.")
        sys.exit(1)

    raw_data_s3_path_base = f"s3://{S3_BUCKET}/{s3_raw_data_prefix.strip('/')}/"
    win_dataset_s3_path = f"s3://{S3_BUCKET}/{s3_win_dataset_prefix.strip('/')}/"
    place_dataset_s3_path = f"s3://{S3_BUCKET}/{s3_place_dataset_prefix.strip('/')}/"

    logger.info(f"Reading raw data from: {raw_data_s3_path_base}")
    all_raw_s3_files = wr.s3.list_objects(raw_data_s3_path_base, boto3_session=boto3_session)

    if not all_raw_s3_files:
        logger.info(f"No raw files found under {raw_data_s3_path_base}. Exiting.")
        sys.exit(0)

    # Data types to process
    process_types = [
        {'name': 'WIN', 'target_s3_path': win_dataset_s3_path, 'glue_table_name': glue_win_table_name},
        {'name': 'PLACE', 'target_s3_path': place_dataset_s3_path, 'glue_table_name': glue_place_table_name}
    ]

    for data_type_info in process_types:
        current_type_name = data_type_info['name']
        current_target_s3_path = data_type_info['target_s3_path']
        current_glue_table_name = data_type_info['glue_table_name']

        logger.info(f"--- Processing {current_type_name} data ---")
        
        type_specific_dfs = []
        processed_files_count = 0
        skipped_files_count = 0

        # Filter files for the current type first
        relevant_s3_files = []
        for s3_file_path in all_raw_s3_files:
            if not s3_file_path.lower().endswith(".parquet"):
                continue
            filename = os.path.basename(s3_file_path)
            match = FILENAME_PATTERN.match(filename)
            if match:
                file_type_from_filename, _, _, _, _ = match.groups()
                if file_type_from_filename.lower() == current_type_name.lower():
                    relevant_s3_files.append(s3_file_path)
            else:
                # Log general skips once, not per type, or it gets noisy.
                # This is handled by the original loop's warning if a file doesn't match pattern at all.
                pass 

        if not relevant_s3_files:
            logger.info(f"No raw files found specifically for {current_type_name} type.")
            # Proceed to ensure S3 path is empty and table is (re)created empty
        else:
            logger.info(f"Found {len(relevant_s3_files)} raw files for {current_type_name} type. Processing...")
            for s3_file_path in tqdm(relevant_s3_files, desc=f"Processing {current_type_name} files", unit="file"):
                filename = os.path.basename(s3_file_path)
                # Match is already confirmed if it's in relevant_s3_files
                match = FILENAME_PATTERN.match(filename)
                file_type_from_filename, country_code_from_filename, _, _, _ = match.groups()

                # This inner check is redundant due to pre-filtering, but harmless
                # if file_type_from_filename.lower() == current_type_name.lower(): 
                logger.debug(f"Reading {current_type_name} file: {filename}")
                try:
                    df = wr.s3.read_parquet(path=s3_file_path, boto3_session=boto3_session)
                    if df.empty:
                        logger.warning(f"Raw file {filename} is empty. Skipping.")
                        skipped_files_count += 1
                        continue
                    
                    processed_df = process_individual_dataframe(df, file_type_from_filename, country_code_from_filename)
                    type_specific_dfs.append(processed_df)
                    processed_files_count += 1
                except Exception as e:
                    logger.error(f"Error processing file {s3_file_path} for {current_type_name} data: {e}", exc_info=True)
                    skipped_files_count += 1
        
        logger.info(f"For {current_type_name} data: Processed {processed_files_count} files, Skipped/Errors {skipped_files_count} files.")

        if type_specific_dfs:
            logger.info(f"Concatenating {len(type_specific_dfs)} DataFrames for {current_type_name} data.")
            combined_df = pd.concat(type_specific_dfs, ignore_index=True)
            logger.info(f"Combined {current_type_name} DataFrame has {len(combined_df)} rows.")

            if not combined_df.empty:
                logger.info(f"Writing combined {current_type_name} data to {current_target_s3_path} (mode: overwrite)")
                wr.s3.to_parquet(
                    df=combined_df,
                    path=current_target_s3_path, # This is the S3 prefix
                    mode="overwrite",
                    boto3_session=boto3_session,
                    dtype=SCHEMA_COLUMNS # Enforce schema on write
                )
                logger.info(f"Successfully wrote combined {current_type_name} data to S3.")
            else:
                logger.info(f"Combined {current_type_name} DataFrame is empty. Deleting existing data in {current_target_s3_path} if any.")
                # Ensure target prefix is empty if combined_df is empty after processing files
                wr.s3.delete_objects(path=current_target_s3_path, boto3_session=boto3_session)
                logger.info(f"Emptied target S3 path {current_target_s3_path} as combined data was empty.")

            logger.info(f"--- Recreating {current_type_name} Glue Table: {glue_database}.{current_glue_table_name} ---")
            recreate_glue_table(
                database_name=glue_database,
                table_name=current_glue_table_name,
                s3_dataset_path=current_target_s3_path,
                table_schema=SCHEMA_COLUMNS,
                session=boto3_session
            )
        else:
            logger.info(f"No {current_type_name} files were successfully processed or found. ")
            logger.info(f"Ensuring target S3 path {current_target_s3_path} is empty as no new data is available.")
            wr.s3.delete_objects(path=current_target_s3_path, boto3_session=boto3_session)
            logger.info(f"Emptied target S3 path {current_target_s3_path}.")
            # Still recreate the table, it will be empty but schema-correct.
            logger.info(f"--- Recreating EMPTY {current_type_name} Glue Table: {glue_database}.{current_glue_table_name} ---")
            recreate_glue_table(
                database_name=glue_database,
                table_name=current_glue_table_name,
                s3_dataset_path=current_target_s3_path,
                table_schema=SCHEMA_COLUMNS,
                session=boto3_session
            )

    logger.info("Script finished processing all data types.")

if __name__ == "__main__":
    main()
