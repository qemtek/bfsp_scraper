# Download all files from Betfair's website and uplpoad them to an S3 bucket

import pandas as pd
import time
import os
import datetime as dt

from apscheduler.schedulers.background import BackgroundScheduler
from calendar import monthrange

from bfsp_scraper.utils.general import download_sp_from_link
from bfsp_scraper.utils.s3_tools import list_files
from bfsp_scraper.settings import boto3_session, S3_BUCKET


job_defaults = {
    'coalesce': False,
    'max_instances': 10
}
scheduler = BackgroundScheduler(job_defaults=job_defaults)

files = list_files(bucket=S3_BUCKET, prefix='data', session=boto3_session)
# Remove folder name from the list of returned objects
if len(files) > 1:
    files = files[1:]
    file_names = [f.get('Key').split('data/')[1] for f in files
                  if len(f.get('Key').split('data/')) > 1]
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

types = [x.lower() for x in os.environ['TYPES'].split(',')]
countries = [x.lower() for x in os.environ['COUNTRIES'].split(',')]

table_refreshed = False
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
                            link = f"https://promo.betfair.com/betfairsp/prices/" \
                                   f"dwbfprices{country}{type}{day}{month}{year}.csv"
                            scheduler.add_job(func=download_sp_from_link, id=str(hash(link)), kwargs={
                                'link': link, 'country': country, 'type': type,
                                'day': day, 'month': month, 'year': year,
                                'mode': 'overwrite' if not table_refreshed else 'append'},
                                              misfire_grace_time=999999999)
                            table_refreshed = True

scheduler.start()
time.sleep(1)
print(f"Jobs left: {len(scheduler.get_jobs())}")
time.sleep(1)
while len(scheduler.get_jobs()) > 0:
    print(f"Jobs left: {len(scheduler.get_jobs())}")
    time.sleep(1)
scheduler.shutdown()

