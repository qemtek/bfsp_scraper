import os
import errno
import time
import io
import pandas as pd
import awswrangler as wr
import requests
import random
import datetime as dt

from bs4 import BeautifulSoup

from bfsp_scraper.settings import SCHEMA_COLUMNS, S3_BUCKET, AWS_GLUE_DB, boto3_session


def clean_name(x, illegal_symbols="'$@#^(%*)._ ", append_with=None):
    x = str(x).lower().strip()
    while x[0].isdigit():
        x = x[1:]
    # Remove any symbols, including spaces
    for s in illegal_symbols:
        x = x.replace(s, "")
    if append_with is not None:
        x = f"{x}_{append_with}"
    return x


def mkdir_p(file_path):
    """Create a file path if one does not exist
    """
    try:
        os.makedirs(file_path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(file_path):
            pass
        else:
            print("OS error: {}".format(exc))
            raise


def safe_open(dir_path, type):
    """ Opens files safely (if the directory does not exist, it is created).
        Taken from https://stackoverflow.com/a/600612/119527
    """
    # Open "path" for writing, creating any parent directories as needed.
    mkdir_p(os.path.dirname(dir_path))
    return open(dir_path, type)


def try_again(initial_wait_seconds=1, max_retries=5, backoff_factor=1):
    """A decorator function that retries a function with backoff
    if it fails. Skips retries for HTTP 404 errors."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    return result
                except requests.exceptions.HTTPError as http_err:
                    if http_err.response.status_code == 404:
                        print(f'{func.__name__} failed with 404 Not Found. Not retrying. Error: {http_err}')
                        raise # Re-raise immediately to skip retry logic
                    else:
                        # For other HTTP errors, treat them as general exceptions for retry
                        last_exception = http_err
                except Exception as e: # Catches other exceptions (like Timeout, ConnectionError)
                    last_exception = e
                
                # If we are here, a retriable error occurred (non-404 HTTPError or other Exception)
                # Check if we've exhausted retries for this error
                if attempt >= max_retries - 1:
                    print(f'{func.__name__} failed after {max_retries} attempts due to: {last_exception}')
                    raise last_exception # Re-raise the last retriable exception

                # Calculate wait time with backoff and jitter
                wait_time = (initial_wait_seconds * (backoff_factor ** attempt)) + \
                            random.uniform(0, initial_wait_seconds * 0.25) # Jitter up to 25% of initial wait
                
                print(f'{func.__name__} failed (Attempt {attempt + 1}/{max_retries}). Retrying in {wait_time:.2f}s. Error: {last_exception}')
                time.sleep(wait_time)
            
            # This part should ideally not be reached if logic is correct (either returns result or raises exception)
            if last_exception:
                raise last_exception
            return None # Fallback
        return wrapper
    return decorator


def construct_betfair_sp_download_url(country_code: str, type_str: str, file_date: dt.date) -> str:
    """
    Constructs the download URL for Betfair SP data files.
    File date is usually the day after the race date.
    Example: file_date = race_date + dt.timedelta(days=1)
    """
    country_for_link = 'uk' if country_code.lower() == 'gb' else country_code.lower()
    file_year_str = str(file_date.year)
    file_month_str = str(file_date.month).zfill(2)
    file_day_str = str(file_date.day).zfill(2)

    link = (f"https://promo.betfair.com/betfairsp/prices/"
            f"dwbfprices{country_for_link}{type_str.lower()}"
            f"{file_day_str}{file_month_str}{file_year_str}.csv")
    return link


@try_again(initial_wait_seconds=1, max_retries=5, backoff_factor=1)
def download_sp_from_link(link: str,
                          country: str, # Country for processing (e.g. 'gb', 'ire')
                          type_str: str, # Type for processing (e.g. 'win', 'place')
                          # Date components for the *source file name* if saving single parquet
                          file_year_str: str,
                          file_month_str: str,
                          file_day_str: str,
                          return_df: bool = False):
    print(f'Trying to download link: {link}')
    try:
        response = requests.get(link, timeout=10) # Using 10s timeout
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        csv_content = io.StringIO(response.text)
        df = pd.read_csv(csv_content)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"HTTP 404 for {link}. Returning {'empty DataFrame' if return_df else 'None (no upload)'}.")
            return pd.DataFrame() if return_df else None
        print(f"HTTP error ({e}) for {link}. Re-raising for @try_again.")
        raise
    except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
        print(f"Request error ({e}) when trying to download link: {link}. Re-raising for @try_again.")
        raise

    if df.empty:
        print(f"Downloaded CSV from {link} is empty or failed to parse.")
        return pd.DataFrame() if return_df else None

    print(f"Successfully downloaded {len(df)} rows from {link}")

    # Standardize column names
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

    # Check for essential columns before proceeding
    essential_cols = ['event_id', 'event_dt', 'selection_name', 'selection_id'] # Added selection_id
    if not all(col in df.columns for col in essential_cols):
        missing_ess_cols = [col for col in essential_cols if col not in df.columns]
        print(f"Essential columns {missing_ess_cols} missing in {link}. Cannot process. Returning {'empty DataFrame' if return_df else 'None'}.")
        return pd.DataFrame() if return_df else None

    # --- Start of common data processing logic ---
    df['event_id'] = df['event_id'].astype(int)
    df['selection_id'] = df['selection_id'].astype(int) # Ensure selection_id is processed
    df['country'] = country.lower()
    df['type'] = type_str.lower()

    try:
        df['event_dt'] = pd.to_datetime(df['event_dt'], format="%d-%m-%Y %H:%M")
    except ValueError:
        try:
            df['event_dt'] = pd.to_datetime(df['event_dt'])
        except Exception as e_dt:
            print(f"Could not parse event_dt for link {link}. Error: {e_dt}. Returning {'empty DataFrame' if return_df else 'None'}.")
            return pd.DataFrame() if return_df else None
    df['event_dt'] = pd.to_datetime(df['event_dt'].dt.strftime('%Y-%m-%d %H:%M'))

    # Derive date components from event_dt for DataFrame columns
    df['year'] = df['event_dt'].dt.year # SCHEMA_COLUMNS type: int
    df['month'] = df['event_dt'].dt.month.astype(str).str.zfill(2) # SCHEMA_COLUMNS type: string
    df['day'] = df['event_dt'].dt.day.astype(str).str.zfill(2) # SCHEMA_COLUMNS type: string

    df['country'] = df['country'].apply(lambda x: 'gb' if x.lower() == 'uk' else x.lower())
    df['selection_name_cleaned'] = df.apply(
        lambda x: clean_name(x['selection_name'], append_with=x['country']), axis=1)
    df['event_date'] = df['event_dt'].dt.strftime('%Y-%m-%d')

    # Ensure all SCHEMA_COLUMNS are present, add if missing, and set order
    processed_cols = {}
    for col_name in SCHEMA_COLUMNS.keys():
        if col_name in df.columns:
            # Attempt to cast to schema type if possible, though to_parquet handles dtype
            # This is more for consistency if the df is returned and used directly
            # For now, just assign. `dtype` in `to_parquet` is the main enforcer for S3.
            processed_cols[col_name] = df[col_name]
        else:
            # print(f"Column '{col_name}' missing, adding as None.") # Optional: for debugging
            processed_cols[col_name] = pd.Series([None] * len(df), name=col_name, dtype=object) # Use object for None compatibility

    df_processed = pd.DataFrame(processed_cols)[list(SCHEMA_COLUMNS.keys())]
    # --- End of common data processing logic ---

    if return_df:
        print(f"Processed {len(df_processed)} rows. Returning DataFrame.")
        return df_processed

    # --- Original S3 upload logic (if not returning df) ---
    if df_processed.empty:
        print('Processed DataFrame is empty. No data to upload.')
        return None
        
    s3_file_name = f"{type_str.lower()}{country.lower()}{file_year_str}{file_month_str}{file_day_str}"
    s3_single_file_path = f"s3://{S3_BUCKET}/data/{s3_file_name}.parquet"
    print(f"Uploading single file to {s3_single_file_path}")
    wr.s3.to_parquet(df_processed, s3_single_file_path, boto3_session=boto3_session, compression='snappy')

    print('Uploading data to S3 dataset')
    athena_table_name = f'betfair_{type_str.lower()}_prices'
    s3_dataset_path = f's3://{S3_BUCKET}/{type_str.lower()}_price_datasets/'

    wr.s3.to_parquet(
        df_processed,
        path=s3_dataset_path,
        dataset=True,
        database=AWS_GLUE_DB,
        table=athena_table_name,
        dtype=SCHEMA_COLUMNS,
        mode='append',
        boto3_session=boto3_session,
        compression='snappy'
    )
    print(f"Uploading complete for {link}")
    return None # Indicate successful completion of upload path


if __name__ == '__main__':
    link = construct_betfair_sp_download_url(country_code='uk', type_str='win', file_date=dt.date(2020, 11, 13))
    download_sp_from_link(link=link, country='uk', type_str='win', file_year_str='2020', file_month_str='11', file_day_str='13')
