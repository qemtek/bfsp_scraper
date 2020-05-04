import pandas as pd
import time
import os
import datetime as dt

from calendar import monthrange

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
this_year = today.year
this_month = today.month
this_day = today.day
start_year = today.year - 10
years = list()
for i in range(start_year, this_year+1):
    years.append(i)

types = os.environ['TYPES'].split(',').lower()
countries = os.environ['COUNTRIES'].lower()

for country in countries:
    temp_result2 = pd.DataFrame()
    for type in types:
        temp_result = pd.DataFrame()
        for year in years:
            for month in range(1, 13):
                days = monthrange(year, month)[1]
                for day in range(1, days+1):
                    if f"{type}{country}{year}{str(month).zfill(2)}{str(day).zfill(2)}.json" in file_names:
                        print(f"{type}{country}{year}{month}{day} exists in S3, skipping")
                    else:
                        if not dt.datetime(year=int(year), month=int(month), day=int(day)) > dt.datetime.today():
                            print(f"{year}/{month}/{day}/{type}/{country}")
                            day = str(day).zfill(2)
                            month = str(month).zfill(2)
                            year = year
                            link = f"http://www.betfairpromo.com/betfairsp/prices/" \
                                   f"dwbfprices{country}{type}{day}{month}{year}.csv"
                            try:
                                try:
                                    download_sp_from_link(link=link, country=country, type=type,
                                                       day=day, month=month, year=year)
                                except Exception as e:
                                    print(f"Attempt failed. Retrying.. Error: {e}")
                                    time.sleep(1)
                                    download_sp_from_link(link=link, country=country, type=type,
                                                       day=day, month=month, year=year)
                            except Exception as e:
                                print(f"Couldnt get data for link: {link}")
