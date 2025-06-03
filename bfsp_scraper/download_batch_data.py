# Download all files from Betfair's website and upload them to an S3 bucket

import pandas as pd
import time
import os
import datetime as dt
from calendar import monthrange
import logging
import json
from collections import defaultdict
import sys
from typing import Tuple, Dict
from tqdm import tqdm

from bfsp_scraper.utils.general import download_sp_from_link
from bfsp_scraper.utils.s3_tools import list_files
from bfsp_scraper.settings import boto3_session, S3_BUCKET

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def worker(params: Tuple[str, str, str, str, str, str, Dict]) -> None:
    """Worker function to download and process a single file."""
    link, country, type_, day, month, year, shared_stats = params
    try:
        logger.info(f"Processing {year}/{month}/{day}/{type_}/{country}")
        download_sp_from_link(
            link=link,
            country=country,
            type_str=type_,
            file_day_str=day,
            file_month_str=month,
            file_year_str=year
        )
        # Update success stats (direct update, no lock needed)
        shared_stats['successful'].append({
            'country': country,
            'type': type_,
            'date': f"{year}-{month}-{day}",
            'link': link
        })
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error processing {link}: {error_msg}")
        # Update error stats (direct update, no lock needed)
        shared_stats['failed'].append({
            'country': country,
            'type': type_,
            'date': f"{year}-{month}-{day}",
            'link': link,
            'error': error_msg
        })
    finally:
        # Add a delay after each task processing, regardless of success or failure
        time.sleep(0.5)

def generate_report(stats: Dict) -> str:
    """Generate a detailed report of the download process."""
    successful = stats['successful']
    failed = stats['failed']
    
    # Group successful downloads by country and type
    success_by_country = defaultdict(lambda: defaultdict(int))
    for s in successful:
        success_by_country[s['country']][s['type']] += 1
    
    # Group failures by error type
    failures_by_error = defaultdict(int)
    for f in failed:
        failures_by_error[f['error']] += 1
    
    report = []
    report.append("Download Report")
    report.append("=" * 50)
    report.append(f"\nTotal files processed: {len(successful) + len(failed)}")
    report.append(f"Successful downloads: {len(successful)}")
    report.append(f"Failed downloads: {len(failed)}")
    
    report.append("\nSuccessful Downloads by Country/Type:")
    report.append("-" * 40)
    for country, types in success_by_country.items():
        report.append(f"\n{country.upper()}:")
        for type_, count in types.items():
            report.append(f"  - {type_}: {count} files")
    
    if failures_by_error:
        report.append("\nFailures by Error Type:")
        report.append("-" * 40)
        for error, count in failures_by_error.items():
            report.append(f"\n{error}: {count} occurrences")
        
        report.append("\nDetailed Failure List:")
        report.append("-" * 40)
        for f in failed:
            report.append(f"\n{f['country']}/{f['type']}/{f['date']}")
            report.append(f"Error: {f['error']}")
    
    return "\n".join(report)

def main():
    logger.info("Starting full refresh process")

    # --- Environment Variable Handling ---
    start_date_str = os.environ.get('START_DATE', '2025-05-01')
    end_date_str = os.environ.get('END_DATE', '2025-06-02')
    types_str = os.environ.get('TYPES', 'win,place')
    countries_str = os.environ.get('COUNTRIES', 'uk,ire,usa,fr')

    missing_vars = []
    if not start_date_str: missing_vars.append('START_DATE')
    if not end_date_str: missing_vars.append('END_DATE')
    if not types_str: missing_vars.append('TYPES')
    if not countries_str: missing_vars.append('COUNTRIES')

    if missing_vars:
        logger.error(f"Missing mandatory environment variables: {', '.join(missing_vars)}")
        logger.error("Please set START_DATE (YYYY-MM-DD), END_DATE (YYYY-MM-DD), TYPES (comma-separated), and COUNTRIES (comma-separated).")
        sys.exit(1)

    try:
        start_date = dt.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = dt.datetime.strptime(end_date_str, "%Y-%m-%d").date()
    except ValueError as e:
        logger.error(f"Invalid date format for START_DATE or END_DATE. Expected YYYY-MM-DD. Error: {e}")
        sys.exit(1)

    if start_date > end_date:
        logger.error(f"START_DATE ({start_date_str}) cannot be after END_DATE ({end_date_str}).")
        sys.exit(1)

    types = [x.lower().strip() for x in types_str.split(',')]
    countries = [x.lower().strip() for x in countries_str.split(',')]

    logger.info(f"Processing data for START_DATE: {start_date_str}, END_DATE: {end_date_str}")
    logger.info(f"Processing TYPES: {types}, COUNTRIES: {countries}")
    
    files = list_files(bucket=S3_BUCKET, prefix='data', session=boto3_session)
    # Remove folder name from the list of returned objects
    if len(files) > 1:
        files = files[1:]
        file_names = [f.get('Key').split('data/')[1] for f in files
                    if len(f.get('Key').split('data/')) > 1]
    else:
        file_names = []

    today_date = dt.date.today()

    # Use a regular dictionary for shared_stats
    shared_stats = {
        'successful': [],
        'failed': []
    }
        
    # Create a list of all download tasks
    tasks = []

    # Generate date range using pandas
    date_range_to_process = pd.date_range(start=start_date, end=end_date, freq='D')

    for country in countries:
        for type_ in types:
            for current_processing_date_dt in date_range_to_process:
                current_processing_date = current_processing_date_dt.date() # Convert pandas Timestamp to datetime.date
                
                year = current_processing_date.year
                month_str = str(current_processing_date.month).zfill(2)
                day_str = str(current_processing_date.day).zfill(2)

                filename_s3_check = f"{type_}{country}{year}{month_str}{day_str}.parquet"

                if filename_s3_check in file_names:
                    logger.debug(f"{type_}{country}{year}{month_str}{day_str} (as {filename_s3_check}) exists in S3, skipping")
                    continue
                                
                if current_processing_date > today_date:
                    logger.debug(f"Date {current_processing_date} is in the future, skipping")
                    continue
                            
                link = (f"https://promo.betfair.com/betfairsp/prices/"
                       f"dwbfprices{country}{type_}{day_str}{month_str}{year}.csv")
                            
                tasks.append((link, country, type_, day_str, month_str, str(year), shared_stats))

    # Run download tasks sequentially
    logger.info(f"Starting downloads sequentially.")
        
    for task_params in tqdm(tasks, desc="Downloading files", unit="file"):
        worker(task_params)

    # Generate and save the report
    report_str = generate_report(shared_stats)
    logger.info("\n" + report_str)
        
    # Save report to S3
    try:
        report_date = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_key = f"reports/full_refresh_{report_date}.txt"
        s3 = boto3_session.client('s3')
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=report_key,
            Body=report_str.encode('utf-8')
        )
        logger.info(f"Report saved to s3://{S3_BUCKET}/{report_key}")
    except Exception as e:
        logger.error(f"Failed to save report to S3: {e}")
        logger.info("Full report:\n" + report_str)

    logger.info("Full refresh process completed.")

if __name__ == "__main__":
    main()
