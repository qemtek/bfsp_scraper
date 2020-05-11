# Download all files from the S3 bucket and create a parquet dataset from them.
# If there are no files in S3, or we want to replace them, run the full_refresh.py script,
# which downloads files from Betfair's website, uploads them to S3 and creates the parquet
# dataset.

import awswrangler as wr
import boto3
import pandas as pd
import time
import os

from apscheduler.schedulers.background import BackgroundScheduler

from bfsp_scraper.settings import PROJECT_DIR, S3_BUCKET, AWS_GLUE_DB, \
    AWS_GLUE_TABLE, SCHEMA_COLUMNS
from bfsp_scraper.utils.s3_tools import download_from_s3

session = boto3.session.Session()

scheduler = BackgroundScheduler()


def append_to_pdataset(local_path, mode):
    df = pd.read_parquet(local_path)
    wr.s3.to_parquet(df, path=f's3://{S3_BUCKET}/datasets/', dataset=True,
                     dtype=SCHEMA_COLUMNS, mode=mode, boto3_session=session)
    print(f"Uploaded {local_path} to parquet dataset")


# Get all files currently in S3
files = wr.s3.list_objects(f's3://{S3_BUCKET}/data/', boto3_session=session)[1:]

# Download / Upload the first file manually with overwrite
filename = f"{PROJECT_DIR}/s3_data/{files[0].split('/')[-1]}"
download_from_s3(local_path=filename, s3_path=files[0].split(f's3://{S3_BUCKET}/')[1], bucket=S3_BUCKET)
append_to_pdataset(filename, mode='overwrite')
files = files[1:]

for file in files:
    filename = f"{PROJECT_DIR}/s3_data/{file.split('/')[-1]}"
    print(filename)
    scheduler.add_job(
        func=download_from_s3,
        kwargs={'local_path': filename, 's3_path': file.split(f's3://{S3_BUCKET}/')[1], 'bucket': S3_BUCKET},
        id=f"{file.split('/')[-1]}_download", replace_existing=True, misfire_grace_time=9999999999)

scheduler.start()
time.sleep(1)
while len(scheduler._pending_jobs) > 0:
    print(f"Jobs left: {len(scheduler._pending_jobs)}")
scheduler.shutdown()


scheduler2 = BackgroundScheduler()

# Get all files currently in S3
files = os.listdir(f"{PROJECT_DIR}/s3_data/")

for file in files:
    filename = f"{PROJECT_DIR}/s3_data/{file.split('/')[-1]}"
    print(filename)
    scheduler2.add_job(func=append_to_pdataset, kwargs={"local_path": filename, 'mode': 'append'},
                       id=f"{file.split('/')[-1]}_upload", replace_existing=True, misfire_grace_time=999999999)

scheduler2.start()
time.sleep(1)
print(f"Jobs left: {len(scheduler2._pending_jobs)}")
time.sleep(1)
while len(scheduler2._pending_jobs) > 0:
    print(f"Jobs left: {len(scheduler2._pending_jobs)}")
scheduler2.shutdown()


