import awswrangler as wr
import boto3

from settings import SCHEMA_COLUMNS

session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
# Get all files currently in S3
files = wr.s3.list_objects('s3://betfair-sp/data', boto3_session=session)[1:]
i=0
overwrite=True
for file in files[0:100]:
    i+=1
    print(f"{i/len(files)}")
    df = wr.s3.read_parquet(file, boto3_session=session, use_threads=True)
    wr.s3.to_parquet(df, path='s3://betfair-sp/datasets/', dataset=True, dtype = SCHEMA_COLUMNS,
                     mode='overwrite' if i==1 and overwrite else 'append', boto3_session=session)

df = wr.s3.read_parquet(path='s3://betfair-sp/datasets/', dataset=True, boto3_session=session)

# Retrieving the data from Amazon Athena
df = wr.athena.read_sql_query("SELECT * FROM betfair_sp", database="finish-time-predict",
                              boto3_session=session, s3_output='s3://qemtek-athena-queries')

