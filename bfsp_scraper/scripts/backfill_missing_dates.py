import awswrangler as wr
import datetime as dt
import pandas as pd

from bfsp_scraper.utils.general import download_sp_from_link
from settings import boto3_session

DATABASE = 'finish-time-predict'

# Get the date range we are interested in
# d1 = pd.to_datetime(dt.datetime.today().date() - dt.timedelta(days=15))
# d2 = pd.to_datetime(dt.datetime.today().date() - dt.timedelta(days=1))
d1 = pd.to_datetime('2025-01-01')
d2 = pd.to_datetime('2025-04-27')
dd = [(d1 + dt.timedelta(days=x)).date() for x in range((d2-d1).days + 1)]

df = wr.athena.read_sql_query(
    "select distinct country, event_dt from betfair_win_prices",
    database=DATABASE,
    boto3_session=boto3_session
)
df['event_dt'] = df['event_dt'].apply(lambda x: x.date())
df = df.drop_duplicates()

for country in ['fr']:
    dates = list(set(df[df['country'] == country]['event_dt']))
    missing_dates = [d for d in dd if d not in dates]
    for date in missing_dates:
        this_year = str(date.year)
        this_month = str(date.month).zfill(2)
        this_day = str(date.day).zfill(2)
        for type in ['win', 'place']:
            link = f"https://promo.betfair.com/betfairsp/prices/" \
                   f"dwbfprices{'uk' if country == 'gb' else country}{type}{this_day}{this_month}{this_year}.csv"
            download_sp_from_link(
                link=link, country=country, type=type,
                day=this_day, month=this_month, year=this_year,
                mode='append'
            )

