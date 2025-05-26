#!/usr/bin/env python3
import argparse
import datetime as dt
import pandas as pd
import awswrangler as wr
import os
from bfsp_scraper.settings import S3_BUCKET, AWS_GLUE_DB, boto3_session

def parse_date(date_str):
    """Parse date string in YYYY-MM-DD format"""
    try:
        return dt.datetime.strptime(date_str, '%Y/%m/%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError('Date must be in YYYY/MM/DD format')

def count_rows_by_country(start_date, end_date, table_name='betfair_win_prices'):
    """
    Count rows by country in the specified table between start_date and end_date
    
    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        table_name: Name of the table to query (default: betfair_win_prices)
    """
    query = f"""
    SELECT 
        country,
        CAST(event_dt as date) as event_date,
        COUNT(*) as row_count
    FROM "{AWS_GLUE_DB}".{table_name}
    WHERE event_dt >= CAST('{str(start_date)}' as date)
    GROUP BY country, CAST(event_dt as date)
    ORDER BY event_date, country
    """
    
    try:
        df = wr.athena.read_sql_query(
            sql=query,
            database=AWS_GLUE_DB,
            boto3_session=boto3_session
        )
        
        if df.empty:
            print("\nNo data found for the specified date range.")
            return
            
        # Print summary by country
        print("\nSummary by Country:")
        print("=" * 50)
        summary = df.groupby('country')['row_count'].sum()
        for country, count in summary.items():
            print(f"{country.upper()}: {count:,} rows")
            
        # Print daily breakdown
        print("\nDaily Breakdown:")
        print("=" * 50)
        print("Date       | GB     | IRE    | Total")
        print("-" * 50)
        
        # Pivot the data for easier display
        pivot_df = df.pivot(index='event_date', columns='country', values='row_count').fillna(0)
        pivot_df['total'] = pivot_df.sum(axis=1)
        
        for date, row in pivot_df.iterrows():
            gb_count = int(row.get('gb', 0))
            ire_count = int(row.get('ire', 0))
            total = int(row['total'])
            print(f"{date} | {gb_count:6,d} | {ire_count:6,d} | {total:6,d}")
            
        print("\nTotal rows across all countries:", f"{int(pivot_df['total'].sum()):,}")
        
    except Exception as e:
        print(f"Error querying Athena: {str(e)}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Count rows by country in Betfair SP data')
    parser.add_argument('--start-date', type=str,
                      help='Start date in YYYY-MM-DD format (default: 30 days ago)')
    parser.add_argument('--end-date', type=str,
                      help='End date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--table', type=str, choices=['betfair_win_prices', 'betfair_place_prices'],
                      default='betfair_win_prices',
                      help='Table to query (default: betfair_win_prices)')
    
    args = parser.parse_args()
    
    # Default to last 30 days if dates not specified
    end_date = parse_date(args.end_date) if args.end_date else dt.date.today()
    start_date = parse_date(args.start_date) if args.start_date else end_date - dt.timedelta(days=30)
    
    print(f"Counting rows in {args.table} between {start_date} and {end_date}")
    count_rows_by_country(start_date, end_date, args.table)

if __name__ == "__main__":
    main()
