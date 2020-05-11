import pandas as pd
import time
import os
import datetime as dt
import awswrangler as wr
import boto3

from utils.general import download_sp_from_link
from bfsp_scraper.settings import S3_BUCKET, AWS_GLUE_TABLE, AWS_GLUE_DB, \
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


use_files_in_s3 = True

if use_files_in_s3:
    # Get a list of all files in S3 currently
    session = boto3.session.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    folder_dir = f's3://{S3_BUCKET}/data/'
    files = wr.s3.list_objects(folder_dir, boto3_session=session)
    file_names = [f.split(folder_dir)[1] for f in files]
else:
    file_names = []

today = dt.datetime.today().date()
this_year = str(today.year)
this_month = str(today.month).zfill(2)
this_day = str(today.day).zfill(2)

types = [x.lower() for x in os.environ['TYPES'].split(',')]
countries = [x.lower() for x in os.environ['COUNTRIES'].split(',')]

for country in countries:
    temp_result2 = pd.DataFrame()
    for type in types:
        temp_result = pd.DataFrame()

        if f"{type}{country}{this_year}{this_month}{this_day}.json" in file_names:
            print(f"{type}{country}{this_year}{this_month}{this_day} exists in S3, skipping")
        else:
            print(f"Running scraper for {this_year}/{this_month}/{this_day}/{type}/{country}")
            link = f"http://www.betfairpromo.com/betfairsp/prices/" \
                   f"dwbfprices{country}{type}{this_day}{this_month}{this_year}.csv"
            try:
                try:
                    download_sp_from_link(link=link, country=country, type=type,
                                       day=this_day, month=this_month, year=this_year)
                except Exception as e:
                    print(f"Attempt failed. Retrying.. Error: {e}")
                    time.sleep(1)
                    download_sp_from_link(link=link, country=country, type=type,
                                       day=this_day, month=this_month, year=this_year)
            except Exception as e:
                print(f"Couldnt get data for link: {link}")

# Run crawler
print("Running crawler")
res = wr.s3.store_parquet_metadata(
    path=f"s3://{S3_BUCKET}/datasets/",
    database=AWS_GLUE_DB,
    table=AWS_GLUE_TABLE,
    dataset=True
)
