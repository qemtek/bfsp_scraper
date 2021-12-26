import boto3
import pathlib
import bfsp_scraper

from bfsp_scraper.utils.config import get_attribute

SCHEMA_COLUMNS = {
    'event_id': 'int',
    'menu_hint': 'string',
    'event_name': 'string',
    'event_dt': 'timestamp',
    'selection_id': 'int',
    'selection_name': 'string',
    'win_lose': 'int',
    'bsp': 'double',
    'ppwap': 'double',
    'morningwap': 'double',
    'ppmax': 'double',
    'ppmin': 'double',
    'ipmax': 'double',
    'ipmin': 'double',
    'morningtradedvol': 'double',
    'pptradedvol': 'double',
    'iptradedvol': 'double',
    'country': 'string',
    'type': 'string',
    'selection_name_cleaned': 'string',
    'event_date': 'string',
    'year': 'int'
}

PROJECT_DIR = str(pathlib.Path(bfsp_scraper.__file__).resolve().parent).replace('\\', '/')
S3_BUCKET = get_attribute('S3_BUCKET')

AWS_GLUE_DB = get_attribute('AWS_GLUE_DB')
AWS_GLUE_TABLE = get_attribute('AWS_GLUE_TABLE')

AWS_ACCESS_KEY_ID = get_attribute('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = get_attribute('AWS_SECRET_ACCESS_KEY')

TYPES = get_attribute('TYPES')
COUNTRIES = get_attribute('COUNTRIES')

boto3_session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='eu-west-1')
