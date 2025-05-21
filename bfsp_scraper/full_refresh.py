# Download all files from Betfair's website and upload them to an S3 bucket

import pandas as pd
import time
import os
import datetime as dt
from calendar import monthrange
from multiprocessing import Pool, cpu_count, Manager
from typing import Tuple, Dict, List
import logging
import json
from collections import defaultdict

from bfsp_scraper.utils.general import download_sp_from_link
from bfsp_scraper.utils.s3_tools import list_files
from bfsp_scraper.settings import boto3_session, S3_BUCKET

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def worker(params: Tuple[str, str, str, str, str, str, bool, Dict]) -> None:
    """Worker function to download and process a single file."""
    link, country, type_, day, month, year, table_refreshed, shared_stats = params
    try:
        logger.info(f"Processing {year}/{month}/{day}/{type_}/{country}")
        download_sp_from_link(
            link=link,
            country=country,
            type=type_,
            day=day,
            month=month,
            year=year,
            mode='overwrite' if not table_refreshed else 'append'
        )
        # Update success stats
        shared_stats['successful'].append({
            'country': country,
            'type': type_,
            'date': f"{year}-{month}-{day}",
            'link': link
        })
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error processing {link}: {error_msg}")
        # Update error stats
        shared_stats['failed'].append({
            'country': country,
            'type': type_,
            'date': f"{year}-{month}-{day}",
            'link': link,
            'error': error_msg
        })

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
    
    files = list_files(bucket=S3_BUCKET, prefix='data', session=boto3_session)
    # Remove folder name from the list of returned objects
    if len(files) > 1:
        files = files[1:]
        file_names = [f.get('Key').split('data/')[1] for f in files
                    if len(f.get('Key').split('data/')) > 1]
    else:
        file_names = []

    today = dt.datetime.today().date()
    this_year = today.year
    start_year = 2008
    years = list(range(start_year, this_year + 1))

    types = [x.lower() for x in os.environ['TYPES'].split(',')]
    countries = [x.lower() for x in os.environ['COUNTRIES'].split(',')]

    # Create a manager to share statistics between processes
    with Manager() as manager:
        shared_stats = manager.dict({
            'successful': manager.list(),
            'failed': manager.list()
        })
        
        # Create a list of all download tasks
        tasks = []
        table_refreshed = True  # Set to false to refresh

        for country in countries:
            for type_ in types:
                for year in years:
                    for month in range(1, 13):
                        days = monthrange(year, month)[1]
                        for day in range(1, days + 1):
                            filename = f"{type_}{country}{year}{str(month).zfill(2)}{str(day).zfill(2)}.json"
                            if filename in file_names:
                                logger.debug(f"{type_}{country}{year}{month}{day} exists in S3, skipping")
                                continue
                                
                            if dt.datetime(year=int(year), month=int(month), day=int(day)) > dt.datetime.today():
                                continue

                            day_str = str(day).zfill(2)
                            month_str = str(month).zfill(2)
                            
                            link = (f"https://promo.betfair.com/betfairsp/prices/"
                                   f"dwbfprices{country}{type_}{day_str}{month_str}{year}.csv")
                            
                            tasks.append((link, country, type_, day_str, month_str, str(year), table_refreshed, shared_stats))
                            table_refreshed = True

        # Use multiprocessing to execute the downloads
        num_processes = min(cpu_count(), 8)  # Allow up to 8 processes since we have 8 cores allocated
        logger.info(f"Starting downloads using {num_processes} processes")
        
        with Pool(processes=num_processes) as pool:
            pool.map(worker, tasks)
        
        # Generate and save the report
        report = generate_report(dict(shared_stats))
        logger.info("\n" + report)
        
        # Save report to S3
        try:
            report_date = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            report_key = f"reports/full_refresh_{report_date}.txt"
            s3 = boto3_session.client('s3')
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=report_key,
                Body=report.encode('utf-8')
            )
            logger.info(f"Report saved to s3://{S3_BUCKET}/{report_key}")
        except Exception as e:
            logger.error(f"Failed to save report to S3: {e}")
            logger.info("Full report:\n" + report)

if __name__ == '__main__':
    main()
