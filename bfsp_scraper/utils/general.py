import os
import errno
import time
import pandas as pd
import awswrangler as wr
import boto3

from bfsp_scraper.settings import SCHEMA_COLUMNS, S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, \
    AWS_GLUE_TABLE, AWS_GLUE_DB


session = boto3.session.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


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


def try_again(wait_seconds=1, retries=3):
    """A decorator function that retries a function after a
    number of seconds if it fails"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                print('{} failed. retrying, error: ', func.__name__)
                print(e)
                for i in range(retries):
                    try:
                        time.sleep(wait_seconds)
                        result = func(*args, **kwargs)
                        return result
                    except Exception as e:
                        print('{} failed. retrying, error: ', func.__name__)
                        print(e)
                        pass
        return wrapper
    return decorator


@try_again()
def download_sp_from_link(link, country, type, day, month, year, mode='append', partition_cols=None):
    df = pd.read_csv(link)
    if len(df) > 0:
        # Clean up data columns
        df.columns = [col.lower() for col in list(df.columns)]
        df['country'] = country
        df['type'] = type
        df['event_dt'] = pd.to_datetime(df['event_dt'])
        df['year'] = df['event_dt'].apply(lambda x: x.year)
        # Change country UK to GB
        df['country'] = df['country'].apply(lambda x: 'gb' if x.lower() == 'uk' else x)
        df['selection_name_cleaned'] = df.apply(
            lambda x: clean_name(x['selection_name'], append_with=x['country']), axis=1)
        df['event_dt'] = pd.to_datetime(df['event_dt'], format="%d-%m-%Y %H:%M")
        df['event_date'] = df['event_dt'].apply(lambda x: str(x.date()))
        file_name = f"{type}{country}{year}{month}{day}"
        # Upload the dataframe to S3 in parquet format
        wr.s3.to_parquet(df, f"s3://{S3_BUCKET}/data/{file_name}.parquet", boto3_session=session)
        # Upload the data to a dataset in S3 as well
        print('Uploading data to parquet dataset')
        wr.s3.to_parquet(df, path=f's3://{S3_BUCKET}/datasets/', dataset=True, database=AWS_GLUE_DB,
                         table=AWS_GLUE_TABLE, dtype=SCHEMA_COLUMNS, mode=mode, boto3_session=session,
                         partition_cols=partition_cols)
        print('Uploading complete')
