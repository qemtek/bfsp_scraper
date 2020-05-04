import pandas as pd
import time
import os
import datetime as dt

from bfsp_scraper.utils import download_sp_from_link
from bfsp_scraper.s3_tools import list_files

files = list_files(bucket=os.environ['BUCKET_NAME'], prefix='bfex_sp')
# Remove folder name from the list of returned objects
if len(files) > 1:
    files = files[1:]
    file_names = [f.get('Key').split('bfex_sp/')[1] for f in files
                  if len(f.get('Key').split('bfex_sp/')) > 1]
else:
    file_names = []

today = dt.datetime.today().date()
this_year = str(today.year)
this_month = str(today.month).zfill(2)
this_day = str(today.day).zfill(2)
yesterday_day = str((today - dt.timedelta(days=1)).day).zfill(2)

types = os.environ['TYPES'].split(',').lower()
countries = os.environ['COUNTRIES'].lower()

for country in countries:
    temp_result2 = pd.DataFrame()
    for type in types:
        temp_result = pd.DataFrame()

        if f"{type}{country}{this_year}{this_month}{yesterday_day}.json" in file_names:
            print(f"{type}{country}{this_year}{this_month}{yesterday_day} exists in S3, skipping")
        else:
            print(f"Running scraper for {this_year}/{this_month}/{yesterday_day}/{type}/{country}")
            link = f"http://www.betfairpromo.com/betfairsp/prices/" \
                   f"dwbfprices{country}{type}{yesterday_day}{this_month}{this_year}.csv"
            try:
                try:
                    download_sp_from_link(link=link, country=country, type=type,
                                       day=yesterday_day, month=this_month, year=this_year)
                except Exception as e:
                    print(f"Attempt failed. Retrying.. Error: {e}")
                    time.sleep(1)
                    download_sp_from_link(link=link, country=country, type=type,
                                       day=yesterday_day, month=this_month, year=this_year)
            except Exception as e:
                print(f"Couldnt get data for link: {link}")
