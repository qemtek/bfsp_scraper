import pandas as pd
import time
import os
import datetime as dt
import awswrangler as wr

from bfsp_scraper.utils.general import download_sp_from_link
from bfsp_scraper.settings import S3_BUCKET, boto3_session

use_files_in_s3 = True

if use_files_in_s3:
    # Get a list of all files in S3 currently
    folder_dir = f's3://{S3_BUCKET}/data/'
    files = wr.s3.list_objects(folder_dir, boto3_session=boto3_session)
    file_names = [f.split(folder_dir)[1] for f in files]
else:
    file_names = []

run_date = dt.datetime.today().date()
# run_date = pd.to_datetime('2020-11-15').date()
this_year = str(run_date.year)
this_month = str(run_date.month).zfill(2)
this_day = str(run_date.day).zfill(2)

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
            link = f"https://promo.betfair.com/betfairsp/prices/" \
                   f"dwbfprices{country}{type}{this_day}{this_month}{this_year}.csv"
            try:
                try:
                    download_sp_from_link(
                        link=link, country=country, type=type,
                        day=this_day, month=this_month, year=this_year,
                        mode='append')  # partition_cols=['year']
                except Exception as e:
                    print(f"Attempt failed. Retrying.. Error: {e}")
                    time.sleep(1)
                    download_sp_from_link(
                        link=link, country=country, type=type,
                        day=this_day, month=this_month, year=this_year,
                        mode='append')  # , partition_cols=['year']
            except Exception as e:
                print(f"Couldn't get data for link: {link}")
