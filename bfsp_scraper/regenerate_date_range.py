import awswrangler as wr
import datetime as dt
import pandas as pd
import sys
from pathlib import Path
import argparse

# Add the parent directory to the Python path
sys.path.append(str(Path(__file__).parent.parent))

from bfsp_scraper.utils.general import download_sp_from_link
from settings import boto3_session

DATABASE = 'finish-time-predict'


def parse_date(date_str):
    """Parse date string in YYYY-MM-DD format"""
    return dt.datetime.strptime(date_str, '%Y-%m-%d').date()


def delete_data_for_period(start_date, end_date, country, force=False):
    """Delete existing data for the specified period and country"""
    
    # First show what would be deleted
    for table in ['betfair_win_prices', 'betfair_place_prices']:
        preview_query = f"""
        SELECT country, event_dt, COUNT(*) as records
        FROM {table}
        WHERE country = '{country}' 
        AND event_dt BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        GROUP BY country, event_dt
        ORDER BY event_dt
        """
        print(f"\nPreviewing data to be deleted from {table}:")
        preview_df = wr.athena.read_sql_query(
            preview_query,
            database=DATABASE,
            boto3_session=boto3_session
        )
        if len(preview_df) > 0:
            print(preview_df)
        else:
            print("No existing data found for this period")
    
    # Ask for confirmation unless force is True
    if not force:
        try:
            confirm = input(f"\nAre you sure you want to delete data for {country} between {start_date} and {end_date}? (yes/no): ")
            if confirm.lower() != 'yes':
                print("Deletion cancelled")
                return False
        except EOFError:
            print("No input available - use --force to skip confirmation")
            return False
    
    # Proceed with deletion
    for table in ['betfair_win_prices', 'betfair_place_prices']:
        delete_query = f"""
        DELETE FROM {table}
        WHERE country = '{country}' 
        AND event_dt BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        """
        print(f"\nDeleting data from {table}...")
        wr.athena.start_query_execution(
            sql=delete_query,
            database=DATABASE,
            boto3_session=boto3_session
        )
    
    return True


def regenerate_data(race_start_date, race_end_date, country):
    """
    Regenerate data for the specified period and country.
    
    Args:
        race_start_date: The start date of the races to regenerate
        race_end_date: The end date of the races to regenerate
        country: The country code (gb or ire)
        
    Note:
        The Betfair files are named with the next day's date.
        For example, to get Jan 8th's races, we need to download the Jan 9th file.
    """
    d1 = pd.to_datetime(race_start_date)
    d2 = pd.to_datetime(race_end_date)
    race_dates = [(d1 + dt.timedelta(days=x)).date() for x in range((d2-d1).days + 1)]

    for race_date in race_dates:
        # The file date is one day ahead of the race date
        file_date = race_date + dt.timedelta(days=1)
        
        # Use file_date for constructing the URL
        file_year = str(file_date.year)
        file_month = str(file_date.month).zfill(2)
        file_day = str(file_date.day).zfill(2)
        
        # Use race_date for the data we're storing
        race_year = str(race_date.year)
        race_month = str(race_date.month).zfill(2)
        race_day = str(race_date.day).zfill(2)
        
        for type in ['win', 'place']:
            link = f"https://promo.betfair.com/betfairsp/prices/" \
                   f"dwbfprices{'uk' if country == 'gb' else country}{type}{file_day}{file_month}{file_year}.csv"
            download_sp_from_link(
                link=link, country=country, type=type,
                day=race_day, month=race_month, year=race_year,
                mode='append'
            )


def main():
    parser = argparse.ArgumentParser(description='Regenerate Betfair SP data for a specific date range')
    parser.add_argument('--start-date', type=str, default='2025-05-11',
                        help='Start date of races in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, default='2025-05-14',
                        help='End date of races in YYYY-MM-DD format')
    parser.add_argument('--countries', type=str, nargs='+', choices=['gb', 'ire', 'fr'], default=['gb', 'ire', 'fr'],
                        help='Country codes to process. Defaults to both gb and ire')
    parser.add_argument('--force', action='store_true',
                        help='Skip confirmation prompt')
    
    args = parser.parse_args()
    
    # These are the dates of the actual races we want to regenerate
    race_start_date = parse_date(args.start_date)
    race_end_date = parse_date(args.end_date)
    
    for country in args.countries:
        print(f"\nProcessing {country.upper()}...")
        print(f"Checking existing data for {country} between {race_start_date} and {race_end_date}")
        if delete_data_for_period(race_start_date, race_end_date, country, force=args.force):
            print(f"\nRegenerating data for {country} between {race_start_date} and {race_end_date}")
            regenerate_data(race_start_date, race_end_date, country)
            print(f"Data regeneration complete for {country}!")
        else:
            print(f"Operation cancelled for {country}")


if __name__ == "__main__":
    main()
