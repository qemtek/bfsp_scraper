# Download all files from the S3 bucket and create a parquet dataset from them.
# If there are no files in S3, or we want to replace them, run the full_refresh.py script,
# which downloads files from Betfair's website, uploads them to S3 and creates the parquet
# dataset.

import awswrangler as wr
import boto3
import pandas as pd
import time
import os
import pyarrow

from apscheduler.schedulers.background import BackgroundScheduler

from bfsp_scraper.settings import PROJECT_DIR, S3_BUCKET, AWS_GLUE_DB, \
    AWS_GLUE_TABLE, SCHEMA_COLUMNS, AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID
from bfsp_scraper.utils.s3_tools import download_from_s3


download = False
upload = True


def append_to_pdataset(local_path, mode='a', index=False, header=False):
    try:
        df = pd.read_parquet(local_path)
        df['event_dt'] = pd.to_datetime(df['event_dt'])
        df['year'] = df['event_dt'].apply(lambda x: x.year)
        df = df[list(SCHEMA_COLUMNS.keys())]
        df.to_csv(df_all_dir, mode=mode, header=header, index=index)
    except pyarrow.lib.ArrowInvalid as e:
        print(f"Loading parquet file failed. \nFile path: {local_path}. \nError: {e}")


session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

df_all_dir = f'{PROJECT_DIR}/tmp/df_all.csv'

if download:
    scheduler = BackgroundScheduler()

    # Get all files currently in S3
    files = wr.s3.list_objects(f's3://{S3_BUCKET}/data/', boto3_session=session)[1:]

    for file in files:
        filename = f"{PROJECT_DIR}/s3_data/{file.split('/')[-1]}"
        print(filename)
        scheduler.add_job(
            func=download_from_s3,
            kwargs={'local_path': filename, 's3_path': file.split(f's3://{S3_BUCKET}/')[1],
                    'bucket': S3_BUCKET, 'session': session},
            id=f"{file.split('/')[-1]}_download", replace_existing=True, misfire_grace_time=9999999999)

    scheduler.start()
    time.sleep(1)
    while len(scheduler._pending_jobs) > 0:
        print(f"Jobs left: {len(scheduler._pending_jobs)}")
    scheduler.shutdown()

if upload:

    scheduler2 = BackgroundScheduler()
    # Get all files currently in S3
    files = os.listdir(f"{PROJECT_DIR}/s3_data/")
    files = [f for f in files if 'DS_Store' not in f]

    # Download / Upload the first file manually with overwrite
    filename = f"{PROJECT_DIR}/s3_data/{files[0]}"
    append_to_pdataset(filename, mode='w', header=True)
    files = files[1:]

    for file in files:
        filename = f"{PROJECT_DIR}/s3_data/{file.split('/')[-1]}"
        print(filename)
        scheduler2.add_job(func=append_to_pdataset, kwargs={"local_path": filename},
                           id=f"{file.split('/')[-1]}_upload", replace_existing=True,
                           misfire_grace_time=999999999)
    scheduler2.start()
    time.sleep(1)
    print(f"Jobs left: {len(scheduler2._pending_jobs)}")
    time.sleep(1)
    while len(scheduler2._pending_jobs) > 0:
        print(f"Jobs left: {len(scheduler2._pending_jobs)}")
    scheduler2.shutdown()

    # Upload the dataframe to the /datasets/ directory in S3
    df = pd.read_csv(df_all_dir)
    wr.s3.to_parquet(df, path=f's3://{S3_BUCKET}/datasets/', dataset=True,
                     dtype=SCHEMA_COLUMNS, mode='overwrite', boto3_session=session,
                     database=AWS_GLUE_DB, table=AWS_GLUE_TABLE, partition_cols=['year'])
    print(f"Uploaded data to parquet dataset")
