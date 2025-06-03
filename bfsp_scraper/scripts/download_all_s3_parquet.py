#!/usr/bin/env python
"""
Downloads .parquet files from a specified S3 bucket's 'data/' prefix,
converts them to .csv format, and saves them to a local directory.
Filenames are expected to encode type, country, and date (e.g., placeire20250430.parquet).
Optionally filtered by type (win/place), country, and date range using environment variables.

This script connects to AWS S3, lists objects under the 'data/' prefix in the configured S3_BUCKET,
parses filenames to extract metadata, filters based on provided environment variables,
downloads each matching Parquet file, converts it to CSV, and saves it to a local directory.
The S3 path structure ('data/' prefix) is maintained locally, with .csv extensions.

Configuration:
- S3_BUCKET and AWS session are sourced from 'bfsp_scraper.settings'.
- Local download directory: 'BFSP_S3_DOWNLOAD_DIR' (optional, defaults to './s3_downloads_csv').
- Filter by type: 'BFSP_DOWNLOAD_TYPE' (optional, 'win' or 'place').
- Filter by country: 'BFSP_DOWNLOAD_COUNTRY' (optional, e.g., 'gb', 'ire').
- Filter by start date: 'BFSP_DOWNLOAD_START_DATE' (optional, YYYY-MM-DD).
- Filter by end date: 'BFSP_DOWNLOAD_END_DATE' (optional, YYYY-MM-DD).

S3 Structure Expectation:
- Bucket: {S3_BUCKET}
- Prefix: data/
- Filename format: [type][country][YYYYMMDD].parquet (e.g., winfr20230101.parquet, placegb20230102.parquet)

Usage:
  Set environment variables (optional):
  export BFSP_S3_DOWNLOAD_DIR="/path/to/your/local/csv_downloads"
  export BFSP_DOWNLOAD_TYPE="win"
  export BFSP_DOWNLOAD_COUNTRY="gb"
  export BFSP_DOWNLOAD_START_DATE="2023-01-01"
  export BFSP_DOWNLOAD_END_DATE="2023-01-31"
  
  Run the script:
  python bfsp_scraper/scripts/download_all_s3_parquet.py
"""

import os
import sys
import boto3
from pathlib import Path
import logging
import re
import datetime as dt
import pandas as pd
import awswrangler as wr

# Add parent directory to Python path to allow imports from bfsp_scraper package
SCRIPT_DIR = Path(__file__).resolve().parent
PARENT_DIR = SCRIPT_DIR.parent
GRANDPARENT_DIR = PARENT_DIR.parent
if str(GRANDPARENT_DIR) not in sys.path:
    sys.path.append(str(GRANDPARENT_DIR))

from bfsp_scraper.settings import S3_BUCKET, boto3_session

# Setup logging
logging.basicConfig(level='DEBUG', format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_DOWNLOAD_DIR = "./s3_downloads_csv" # Changed default dir name
S3_DATA_PREFIX = "data/" # Expected S3 prefix

# Regex to parse filename: type (win/place), country (2-3 letters), date (YYYYMMDD)
# e.g., placeire20250430.parquet
FILENAME_PATTERN = re.compile(r"^(win|place)([a-z]{2,3})(\d{8})\.parquet$", re.IGNORECASE)

def download_all_parquet_from_s3(local_download_dir: str, 
                               type_filter: str = None, 
                               country_filter: str = None, 
                               start_date_filter: dt.date = None, 
                               end_date_filter: dt.date = None):
    """
    Downloads .parquet files from S3 'data/' prefix, parsing filenames for metadata,
    filters, converts to CSV, and saves them locally.
    """
    paginator = boto3_session.client('s3').get_paginator('list_objects_v2')
    base_download_path = Path(local_download_dir).resolve()
    
    filter_description = []
    if type_filter:
        filter_description.append(f"type='{type_filter}'")
    if country_filter:
        filter_description.append(f"country='{country_filter}'")
    if start_date_filter:
        filter_description.append(f"start_date='{start_date_filter.strftime('%Y-%m-%d')}'")
    if end_date_filter:
        filter_description.append(f"end_date='{end_date_filter.strftime('%Y-%m-%d')}'")
    
    if filter_description:
        logger.info(f"Preparing to download from S3 bucket '{S3_BUCKET}/{S3_DATA_PREFIX}', convert to .csv, and save to '{base_download_path}' with filters: {', '.join(filter_description)}")
    else:
        logger.info(f"Preparing to download all .parquet files from S3 bucket '{S3_BUCKET}/{S3_DATA_PREFIX}', convert to .csv, and save to '{base_download_path}'")

    download_count = 0
    total_objects_processed = 0
    skipped_due_to_error = 0
    skipped_due_to_filter = 0
    skipped_due_to_pattern_mismatch = 0

    try:
        logger.info(f"Scanning S3 prefix: s3://{S3_BUCKET}/{S3_DATA_PREFIX}")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_DATA_PREFIX):
            if 'Contents' not in page:
                continue
            
            for obj in page['Contents']:
                total_objects_processed += 1
                s3_key = obj['Key']
                
                # Skip if it's a 'directory' marker or not directly under S3_DATA_PREFIX
                if s3_key == S3_DATA_PREFIX or '/' in s3_key[len(S3_DATA_PREFIX):]:
                    logger.debug(f"Skipping directory marker or object in sub-folder: {s3_key}")
                    continue

                filename = Path(s3_key).name
                if not filename.endswith('.parquet'):
                    logger.debug(f"Skipping non-parquet file: {s3_key}")
                    continue

                match = FILENAME_PATTERN.match(filename)
                if not match:
                    logger.debug(f"Skipping file '{filename}' as its name does not match pattern (e.g., typeCountryYYYYMMDD.parquet).")
                    skipped_due_to_pattern_mismatch += 1
                    continue

                parsed_type, parsed_country, parsed_date_str = match.groups()
                parsed_type = parsed_type.lower()
                parsed_country = parsed_country.lower()

                try:
                    file_date = dt.datetime.strptime(parsed_date_str, "%Y%m%d").date()
                except ValueError:
                    logger.warning(f"Could not parse date from filename: '{filename}'. Skipping.")
                    skipped_due_to_pattern_mismatch += 1 # Count as pattern mismatch for simplicity
                    continue

                # Apply filters
                if type_filter and parsed_type != type_filter:
                    # logger.debug(f"Skipping {filename} (type mismatch: expected {type_filter}, got {parsed_type})")
                    skipped_due_to_filter +=1
                    continue
                if country_filter and parsed_country != country_filter:
                    # logger.debug(f"Skipping {filename} (country mismatch: expected {country_filter}, got {parsed_country})")
                    skipped_due_to_filter +=1
                    continue
                if start_date_filter and file_date < start_date_filter:
                    # logger.debug(f"Skipping {filename} (before start_date {start_date_filter}, file date {file_date})")
                    skipped_due_to_filter +=1
                    continue
                if end_date_filter and file_date > end_date_filter:
                    # logger.debug(f"Skipping {filename} (after end_date {end_date_filter}, file date {file_date})")
                    skipped_due_to_filter +=1
                    continue

                # Define local CSV file path, maintaining the 'data/' part of the S3 key structure
                local_csv_file_path = (base_download_path / s3_key).with_suffix('.csv')
                
                local_csv_file_path.parent.mkdir(parents=True, exist_ok=True)
                
                s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
                try:
                    logger.debug(f"Processing S3 object: {s3_uri} -> {local_csv_file_path}")
                    df = wr.s3.read_parquet(path=s3_uri, boto3_session=boto3_session)
                    df.to_csv(local_csv_file_path, index=False)
                    logger.info(f"Successfully converted '{s3_key}' to '{local_csv_file_path}'")
                    download_count += 1
                except Exception as e:
                    logger.error(f"Error processing or converting file {s3_uri} to CSV: {e}")
                    skipped_due_to_error += 1
                    if local_csv_file_path.exists():
                        try:
                            local_csv_file_path.unlink()
                        except OSError as ose:
                            logger.error(f"Could not remove partially written file {local_csv_file_path}: {ose}")
        
        logger.info(f"S3 scan complete. Total S3 objects processed: {total_objects_processed}")
        if download_count > 0:
            logger.info(f"Successfully downloaded and converted {download_count} files to .csv format in '{base_download_path}'.")
        else:
            logger.info("No files were downloaded or converted based on the criteria and filename pattern.")
        if skipped_due_to_filter > 0:
            logger.info(f"{skipped_due_to_filter} files were skipped due to not matching active filters.")
        if skipped_due_to_pattern_mismatch > 0:
            logger.info(f"{skipped_due_to_pattern_mismatch} files were skipped due to filename not matching expected pattern or unparsable date.")
        if skipped_due_to_error > 0:
            logger.warning(f"{skipped_due_to_error} files were skipped due to errors during download or conversion.")

    except Exception as e:
        logger.error(f"An unexpected error occurred during S3 processing: {e}", exc_info=True)
        sys.exit(1)


def main():
    # Fetch configuration from environment variables
    local_dir_env = os.environ.get('BFSP_S3_DOWNLOAD_DIR', 'data') 
    download_type_env = os.environ.get('BFSP_DOWNLOAD_TYPE', 'win')
    download_country_env = os.environ.get('BFSP_DOWNLOAD_COUNTRY', 'ire')
    start_date_env = os.environ.get('BFSP_DOWNLOAD_START_DATE', '2025-05-01')
    end_date_env = os.environ.get('BFSP_DOWNLOAD_END_DATE', '2025-06-02')

    if local_dir_env:
        if local_dir_env.lower().startswith("s3://"):
            logger.error(f"Error: BFSP_S3_DOWNLOAD_DIR ('{local_dir_env}') cannot be an S3 path. It must be a local directory. Exiting.")
            sys.exit(1)
        local_dir = local_dir_env
        logger.info(f"Using download directory from BFSP_S3_DOWNLOAD_DIR: '{Path(local_dir).resolve()}'")
    else:
        local_dir = DEFAULT_DOWNLOAD_DIR # Changed default
        logger.info(f"BFSP_S3_DOWNLOAD_DIR not set. Using default download directory: '{Path(local_dir).resolve()}'")

    # Validate type filter if provided
    if download_type_env and download_type_env.lower() not in ['win', 'place']:
        logger.error(f"Invalid value for BFSP_DOWNLOAD_TYPE: '{download_type_env}'. Must be 'win' or 'place'. Exiting.")
        sys.exit(1)
    type_filter = download_type_env.lower() if download_type_env else None
    country_filter = download_country_env.lower() if download_country_env else None

    # Parse and validate date filters
    start_date_obj = None
    if start_date_env:
        try:
            start_date_obj = dt.datetime.strptime(start_date_env, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid BFSP_DOWNLOAD_START_DATE format: '{start_date_env}'. Must be YYYY-MM-DD. Exiting.")
            sys.exit(1)

    end_date_obj = None
    if end_date_env:
        try:
            end_date_obj = dt.datetime.strptime(end_date_env, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid BFSP_DOWNLOAD_END_DATE format: '{end_date_env}'. Must be YYYY-MM-DD. Exiting.")
            sys.exit(1)
    
    if start_date_obj and end_date_obj and start_date_obj > end_date_obj:
        logger.error(f"BFSP_DOWNLOAD_START_DATE ({start_date_obj}) cannot be after BFSP_DOWNLOAD_END_DATE ({end_date_obj}). Exiting.")
        sys.exit(1)
    
    # If only one date is provided, set the other to match for a single-day filter
    if start_date_obj and not end_date_obj:
        end_date_obj = start_date_obj
    elif end_date_obj and not start_date_obj:
        start_date_obj = end_date_obj

    print(f"\nThis script will download .parquet files from the S3 bucket: '{S3_BUCKET}' and save them as .csv files locally.")
    print(f"S3 Bucket: '{S3_BUCKET}', Prefix: '{S3_DATA_PREFIX}'")
    print(f"Local download directory: '{Path(local_dir).resolve()}'")
    print("Files will be converted to .csv format.")
    print("The S3 path structure (under 'data/') will be replicated locally (with .csv extensions).")
    print("Expected S3 filename format: [type][country][YYYYMMDD].parquet (e.g., winfr20230101.parquet)")
    print("Please ensure you have sufficient disk space and the necessary AWS permissions.")
    
    filter_details = []
    if type_filter:
        filter_details.append(f"Type: {type_filter}")
    if country_filter:
        filter_details.append(f"Country: {country_filter}")
    if start_date_obj:
        filter_details.append(f"Start Date: {start_date_obj.strftime('%Y-%m-%d')}")
    if end_date_obj and (start_date_obj != end_date_obj): # Only show end date if it's a range
        filter_details.append(f"End Date: {end_date_obj.strftime('%Y-%m-%d')}")
    
    if filter_details:
        print(f"Filters applied (from environment variables): {'; '.join(filter_details)}")

    try:
        confirm = input("\nProceed with download? (yes/no): ").strip().lower()
    except KeyboardInterrupt:
        logger.info("\nDownload cancelled by user (Ctrl+C).")
        sys.exit(0)
        
    if confirm == 'yes':
        download_all_parquet_from_s3(local_dir, type_filter, country_filter, start_date_obj, end_date_obj)
    else:
        logger.info("Download cancelled by user.")

if __name__ == "__main__":
    main()
