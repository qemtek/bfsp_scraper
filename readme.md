#### Betfair Starting Price (SP) Scraper

This repo contains an automated tool for scraping data from the betfair SP website and storing the results in an S3 bucket. There are two main scripts, one for obtaining the latest data (yesterday) and another to get the last 10 years of data. In the latter script, if the file for a particular day/type (win/place) already exists in S3, the job will skip for that day.

Required environment variables:
- COUNTRIES - Comma separated country codes, for example: gb, ire
- TYPES - Comma separated market types, for example: win, place
- AWS_ACCESS_KEY_ID - The access key to access your S3 bucket.
- AWS_SECRET_ACCESS_KEY - The secret access key to access your S3 bucket.
- BUCKET_NAME - The name of the S3 bucket you want data to be put into.