import os
import errno
import time
import io
import pandas as pd
import awswrangler as wr
import requests
import random

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


def try_again(initial_wait_seconds=1, max_retries=5, backoff_factor=2):
    """A decorator function that retries a function with exponential backoff
    if it fails."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    last_exception = e
                    # Calculate wait time with exponential backoff and jitter
                    wait_time = (initial_wait_seconds * (backoff_factor ** attempt)) + \
                                random.uniform(0, initial_wait_seconds * 0.25) # Jitter up to 25% of initial wait
                    
                    if attempt < max_retries - 1:
                        print(f'{func.__name__} failed (Attempt {attempt + 1}/{max_retries}). Retrying in {wait_time:.2f}s. Error: {e}')
                        time.sleep(wait_time)
                    else:
                        print(f'{func.__name__} failed after {max_retries} attempts. Error: {e}')
                        # Re-raise the last exception if all retries fail
                        raise last_exception
            # This part should ideally not be reached if an exception is always raised on final failure
            # or a result is returned on success.
            if last_exception: # Should have been raised already
                raise last_exception
            return None # Fallback, though logic implies it won't be hit if func always returns or raises
        return wrapper
    return decorator


def fetch_uk_proxies():
    url = 'https://free-proxy-list.net/uk-proxy.html'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Assuming the IP addresses are contained within a table
    # Note: The website structure might change, so this could need an update
    table = soup.find('table', 'table table-striped table-bordered')
    trs = table.find_all('tr')
    ip_addresses = list()
    for res in trs[1:]:
        ip_address = res.next.next
        port = res.next.next.next.next
        ip_addresses.append(f"{ip_address}:{port}")
    return ip_addresses


@try_again(initial_wait_seconds=5, max_retries=5, backoff_factor=2)
def download_sp_from_link(link, country, type, day, month, year, mode='append', partition_cols=None):
    print(f'Trying to download link: {link}')
    df = pd.read_csv(link)
    print(f"Success: {df}")

    if len(df) > 0:
        # Clean up data columns
        df.columns = [col.lower() for col in list(df.columns)]
        df['event_id'] = df['event_id'].astype(int)
        df['country'] = country
        df['type'] = type
        df['event_dt'] = pd.to_datetime(df['event_dt'], format="%d-%m-%Y %H:%M")
        df['event_dt'] = pd.to_datetime(df['event_dt'].dt.strftime('%Y-%m-%d %H:%M'))
        df['year'] = df['event_dt'].apply(lambda x: x.year)
        # Change country UK to GB
        df['country'] = df['country'].apply(lambda x: 'gb' if x.lower() == 'uk' else x)
        df['selection_name_cleaned'] = df.apply(
            lambda x: clean_name(x['selection_name'], append_with=x['country']), axis=1)
        df['event_date'] = df['event_dt'].apply(lambda x: str(x.date()))
        file_name = f"{type}{country}{year}{month}{day}"
        # Upload the dataframe to S3 in parquet format
        wr.s3.to_parquet(df, f"s3://{S3_BUCKET}/data/{file_name}.parquet", boto3_session=boto3_session)
        # Upload the data to a dataset in S3 as well
        print('Uploading data to parquet dataset')
        table = f'betfair_{str(type).lower()}_prices'
        wr.s3.to_parquet(
            df, path=f's3://{S3_BUCKET}/{str(type).lower()}_price_datasets/',
            dataset=True, database=AWS_GLUE_DB, table=table, dtype=SCHEMA_COLUMNS,
            mode=mode, boto3_session=boto3_session, partition_cols=partition_cols
        )
        print('Uploading complete')
    else:
        print('df returned no rows')


if __name__ == '__main__':
    link = 'https://promo.betfair.com/betfairsp/prices/dwbfpricesukwin13112020.csv'
    download_sp_from_link(link=link, country='uk', type='win', day=13,
                          month=11, year=2020, mode='append', partition_cols=None)
