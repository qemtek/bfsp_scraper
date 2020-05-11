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

PROJECT_DIR = get_attribute('PROJECT_DIR')
S3_BUCKET = get_attribute('S3_BUCKET')

AWS_GLUE_DB = get_attribute('AWS_GLUE_DB')
AWS_GLUE_TABLE = get_attribute('AWS_GLUE_TABLE')

AWS_ACCESS_KEY_ID = get_attribute('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = get_attribute('AWS_SECRET_ACCESS_KEY')
